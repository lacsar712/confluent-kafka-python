# Copyright 2026 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Runtime execution-policy stack for Producer/Consumer instances.

This stack is deliberately *not* configuration. It never mutates ``acks``,
``linger.ms``, batch limits, ``isolation.level`` or ``auto.offset.reset``;
it never touches librdkafka connections, authentication, rebalancing or
serialization registries. A frame is pushed for a specific client (or a set
of clients participating in one cooperative scope, e.g. consumer + EOS
producer) and every gate decision re-reads the *current* top of that
client's stack at call time -- delivery reports, rebalance callbacks and
nested business callbacks therefore observe the frame that is active when
they run, never a snapshot captured when the enclosing section began.

Three independent axes live on every frame:

* ``emit_admit``  -- ``honor_cfg`` / ``deny_new`` / ``one_more``
* ``fetch_admit`` -- ``honor_cfg`` / ``deny_new`` / ``force_drain``
* ``offset_gate`` -- ``honor_cfg`` / ``deny_new`` / ``force_rewind``

An empty stack (or a frame whose axis is ``honor_cfg``) behaves exactly like
the plain cimpl API. Inner frames delegate untouched axes to outer frames
and popping a frame restores the outer view.
"""

from __future__ import annotations

import contextlib
import itertools
import threading
import weakref
from typing import Any, Dict, List, Optional, Sequence, Tuple

from ..cimpl import Consumer as _ConsumerImpl
from ..cimpl import Message, TopicPartition
from ..cimpl import Producer as _ProducerImpl
from ..error import KafkaException

# ---------------------------------------------------------------------------
# Gate values
# ---------------------------------------------------------------------------

EMIT_HONOR_CFG = "honor_cfg"
EMIT_DENY_NEW = "deny_new"
EMIT_ONE_MORE = "one_more"

FETCH_HONOR_CFG = "honor_cfg"
FETCH_DENY_NEW = "deny_new"
FETCH_FORCE_DRAIN = "force_drain"

OFFSET_HONOR_CFG = "honor_cfg"
OFFSET_DENY_NEW = "deny_new"
OFFSET_FORCE_REWIND = "force_rewind"

_EMIT_GATES = (EMIT_HONOR_CFG, EMIT_DENY_NEW, EMIT_ONE_MORE)
_FETCH_GATES = (FETCH_HONOR_CFG, FETCH_DENY_NEW, FETCH_FORCE_DRAIN)
_OFFSET_GATES = (OFFSET_HONOR_CFG, OFFSET_DENY_NEW, OFFSET_FORCE_REWIND)

# Offset ledger states (per partition, per frame).
_ST_STORED = "stored"  # store_offsets() seen, no commit afterwards
_ST_COMMITTED = "committed"  # commit() seen -- never rewound
_ST_SOUGHT = "sought"  # explicit user seek -- left alone
_ST_CLOSED = "closed"  # partition revoked/lost inside the frame
# an offset op finished after a newer deny_new/force_rewind frame was pushed
_ST_TAINTED = "tainted"

_frame_ids = itertools.count(1)

# ---------------------------------------------------------------------------
# Per-client frame stacks
# ---------------------------------------------------------------------------

_registry_lock = threading.RLock()
# cid -> ordered list of frames; outermost first, top last.
_stacks: Dict[int, List["_PolicyFrame"]] = {}
# cid -> weak reference, used to reject id() reuse races.
_live: Dict[int, "weakref.ref[object]"] = {}
# cid -> read-only snapshot of the pinned construction configuration. Kept
# here, not on the client: the clients carry no policy private bag.
_conf_views: Dict[int, Dict[str, Any]] = {}


def _cleanup_client(cid: int) -> None:
    with _registry_lock:
        _stacks.pop(cid, None)
        _live.pop(cid, None)
        _conf_views.pop(cid, None)


def register_conf_view(client: Any, view: Dict[str, Any]) -> None:
    """Record the read-only construction-config snapshot for a client."""
    cid = _register_client(client)
    with _registry_lock:
        _conf_views[cid] = dict(view)


def _register_client(client: Any) -> int:
    cid = id(client)
    with _registry_lock:
        existing = _live.get(cid)
        if existing is not None and existing() is client and cid in _stacks:
            return cid
        _stacks[cid] = []
        _live[cid] = weakref.ref(client)
        weakref.finalize(client, _cleanup_client, cid)
    return cid


def _stack_snapshot(client: Any) -> List["_PolicyFrame"]:
    """Live stack for a client at call time. Never cache across calls."""
    with _registry_lock:
        return list(_stacks.get(id(client), ()))


def _top_frame(client: Any) -> Optional["_PolicyFrame"]:
    with _registry_lock:
        stack = _stacks.get(id(client))
        return stack[-1] if stack else None


class _OffsetEntry:
    __slots__ = ("topic", "partition", "state", "was_store", "prev_offsets")

    def __init__(self, topic: str, partition: int) -> None:
        self.topic = topic
        self.partition = partition
        self.state = _ST_STORED
        self.was_store = False
        self.prev_offsets: List[int] = []


class _TxnEntry:
    __slots__ = ("producer_cid", "positions", "state", "tainted")

    def __init__(self, producer_cid: int, positions: List[TopicPartition]) -> None:
        self.producer_cid = producer_cid
        self.positions = positions
        # 'sent' -> send_offsets_to_transaction observed
        # 'committed' / 'aborted' -> resolved by commit/abort_transaction
        self.state = "sent"
        self.tainted = False


class _PolicyFrame:
    """One frame of the runtime policy stack."""

    def __init__(
        self,
        clients: Tuple[Any, ...],
        emit_admit: str,
        fetch_admit: str,
        offset_gate: str,
        options: Dict[str, Any],
    ) -> None:
        self.fid = next(_frame_ids)
        self.clients = clients
        self.client_ids = frozenset(id(c) for c in clients)
        self.emit_admit = emit_admit
        self.fetch_admit = fetch_admit
        self.offset_gate = offset_gate
        self.options = options
        self.lock = threading.RLock()

        # emit axis
        self.emit_quota = 1 if emit_admit == EMIT_ONE_MORE else 0
        # cid -> number of successfully admitted produces
        self.emit_admitted: Dict[int, int] = {}

        # offset axis: cid(consumer) -> {(topic, partition): entry}
        self.ledger: Dict[int, Dict[Tuple[str, int], _OffsetEntry]] = {
            id(c): {} for c in clients if isinstance(c, _ConsumerImpl)
        }
        # EOS axis: cid(producer) -> transaction offset entries
        self.txn_entries: Dict[int, List[_TxnEntry]] = {id(c): [] for c in clients if isinstance(c, _ProducerImpl)}
        # cid(producer) -> last observed transaction lifecycle signal
        self.txn_state: Dict[int, str] = {}

        # fetch axis: messages pulled by pop-time force_drain settlement
        self.drained: Dict[int, List[Message]] = {}

        # settlement runs at most once, even on exception-driven unwind
        self.settled = False
        # observability only: which cooperative scope pushed this frame
        self.scope_ident = threading.get_ident()

    # -- ledger helpers -----------------------------------------------------

    def ledger_dict(self, cid: int) -> Dict[Tuple[str, int], _OffsetEntry]:
        with self.lock:
            return self.ledger.setdefault(cid, {})

    def mark(
        self, cid: int, key: Tuple[str, int], state: str, *, prev_offset: Optional[int] = None, was_store: bool = False
    ) -> None:
        with self.lock:
            led = self.ledger.setdefault(cid, {})
            entry = led.get(key)
            if entry is None:
                entry = _OffsetEntry(*key)
                led[key] = entry
            if state == _ST_COMMITTED:
                entry.state = _ST_COMMITTED
            elif state == _ST_CLOSED:
                # Partition is gone; nothing can safely be done to it.
                entry.state = _ST_CLOSED
            elif state == _ST_SOUGHT:
                if entry.state != _ST_COMMITTED:
                    entry.state = _ST_SOUGHT
            elif state == _ST_TAINTED:
                if entry.state not in (_ST_COMMITTED, _ST_CLOSED):
                    entry.state = _ST_TAINTED
            elif state == _ST_STORED:
                if entry.state not in (_ST_COMMITTED, _ST_SOUGHT, _ST_CLOSED):
                    entry.state = _ST_STORED
                if was_store:
                    entry.was_store = True
                    if prev_offset is not None:
                        entry.prev_offsets.append(prev_offset)

    def add_txn_entry(self, cid: int, positions: List[TopicPartition]) -> _TxnEntry:
        with self.lock:
            entry = _TxnEntry(cid, list(positions))
            self.txn_entries.setdefault(cid, []).append(entry)
            return entry

    def txn_signal(self, cid: int, state: str) -> None:
        with self.lock:
            self.txn_state[cid] = state
            if state in ("committed", "aborted"):
                for entry in self.txn_entries.get(cid, ()):
                    entry.state = state


# ---------------------------------------------------------------------------
# Observable settlement report
# ---------------------------------------------------------------------------


class FrameSettlement:
    """Observable result of popping a frame.

    Every piece of half-finished state the pop touched is reported here;
    nothing is silently dropped or faked.
    """

    def __init__(self, frame_id: int) -> None:
        self.frame_id = frame_id
        #: cid -> {"admitted": int, "outstanding": int, "remaining": int}
        self.emit: Dict[int, Dict[str, int]] = {}
        #: cid -> messages pulled to the visible boundary by force_drain
        self.drained: Dict[int, List[Message]] = {}
        #: partitions rewound to their committed offset by force_rewind
        self.rewound: List[TopicPartition] = []
        #: stored/tainted partitions that could not be rewound (tp, reason)
        self.rewind_pending: List[Tuple[TopicPartition, str]] = []
        #: partitions left untouched because they were committed/sought/closed
        self.left_committed: List[Tuple[str, int]] = []
        self.left_sought: List[Tuple[str, int]] = []
        self.left_closed: List[Tuple[str, int]] = []
        #: EOS offsets sent to a transaction that was not committed in frame
        self.transaction_pending: List[Dict[str, Any]] = []
        #: non-fatal errors observed while settling (still observable)
        self.errors: List[str] = []

    def as_dict(self) -> Dict[str, Any]:
        return {
            "frame_id": self.frame_id,
            "emit": self.emit,
            "drained": {cid: len(msgs) for cid, msgs in self.drained.items()},
            "rewound": [(tp.topic, tp.partition, tp.offset) for tp in self.rewound],
            "rewind_pending": [(tp.topic, tp.partition, reason) for tp, reason in self.rewind_pending],
            "left_committed": self.left_committed,
            "left_sought": self.left_sought,
            "left_closed": self.left_closed,
            "transaction_pending": self.transaction_pending,
            "errors": self.errors,
        }

    def __repr__(self) -> str:
        return "FrameSettlement({!r})".format(self.as_dict())


# ---------------------------------------------------------------------------
# Observable rejection
# ---------------------------------------------------------------------------


class PolicyAdmissionError(KafkaException):
    """Raised when the live policy stack observably rejects a call.

    It is always raised *before* the corresponding librdkafka entry point is
    reached (or after an offset operation whose governing frame changed
    mid-flight), so a rejected produce/poll is never cancelled in flight and
    never rewritten into an empty success, and a rejected commit/store/seek
    is never silently marked as done.

    Attributes:
        axis:  ``"emit"``, ``"fetch"`` or ``"offset"``.
        gate:  the gate value that caused the rejection.
        client: the client the call was made on.
        action: the API action (``produce``, ``poll``, ``commit`` ...).
        frame_id: id of the frame whose gate fired (when known).
    """

    def __init__(
        self, axis: str, gate: str, client: Any, action: str, message: str, *, frame_id: Optional[int] = None
    ) -> None:
        self.axis = axis
        self.gate = gate
        self.client = client
        self.action = action
        self.frame_id = frame_id
        super().__init__(message)


# ---------------------------------------------------------------------------
# Gate resolution (live on every call -- never snapshot across calls)
# ---------------------------------------------------------------------------


def _resolved_gate(client: Any, attr: str) -> Tuple[Optional[_PolicyFrame], str]:
    for frame in reversed(_stack_snapshot(client)):
        gate = getattr(frame, attr)
        if gate != "honor_cfg":
            return frame, gate
    return None, "honor_cfg"


def check_emit(client: Any, action: str = "produce") -> Optional[_PolicyFrame]:
    frame, gate = _resolved_gate(client, "emit_admit")
    if gate == EMIT_DENY_NEW:
        raise PolicyAdmissionError(
            "emit",
            gate,
            client,
            action,
            "produce is observably denied for this scope (emit_admit=deny_new): "
            "in-flight records are not cancelled, but no new record is admitted",
            frame_id=frame.fid if frame else None,
        )
    return frame if gate == EMIT_ONE_MORE else None


def check_fetch(client: Any, action: str) -> None:
    frame, gate = _resolved_gate(client, "fetch_admit")
    if gate == FETCH_DENY_NEW:
        raise PolicyAdmissionError(
            "fetch",
            gate,
            client,
            action,
            "{} is observably denied for this scope (fetch_admit=deny_new): "
            "no empty success is fabricated and the in-flight fetch is not "
            "cancelled".format(action),
            frame_id=frame.fid if frame else None,
        )


def check_offset(client: Any, action: str) -> None:
    frame, gate = _resolved_gate(client, "offset_gate")
    if gate == OFFSET_DENY_NEW:
        raise PolicyAdmissionError(
            "offset",
            gate,
            client,
            action,
            "{} is observably denied for this scope (offset_gate=deny_new): "
            "the offset is not silently marked as committed/stored".format(action),
            frame_id=frame.fid if frame else None,
        )


class _EmitTicket:
    """one_more admission ticket; rollback() if librdkafka refuses the record."""

    __slots__ = ("frame", "cid", "active")

    def __init__(self, frame: _PolicyFrame, cid: int) -> None:
        self.frame = frame
        self.cid = cid
        self.active = True

    def admitted(self) -> None:
        with self.frame.lock:
            self.frame.emit_admitted[self.cid] = self.frame.emit_admitted.get(self.cid, 0) + 1
            self.active = False

    def rollback(self) -> None:
        # The record was not successfully accepted (e.g. queue full), so the
        # one_more budget must not be consumed by a failed attempt.
        with self.frame.lock:
            if self.active:
                self.frame.emit_quota += 1
                self.active = False


def admit_emit(client: Any, action: str = "produce") -> Optional[_EmitTicket]:
    """Resolve the emit axis for one new produce attempt.

    Raises on deny_new / exhausted one_more; returns a ticket that must be
    confirmed (``admitted()``) once librdkafka accepted the record or
    released (``rollback()``) if acceptance failed.
    """
    frame = check_emit(client, action)
    cid = id(client)
    if frame is None:
        # Count admitted produces on all frames bound to this client so pop
        # settlement knows whether in-flight records belong to the frame.
        for fr in _stack_snapshot(client):
            with fr.lock:
                fr.emit_admitted[cid] = fr.emit_admitted.get(cid, 0) + 1
        return None
    with frame.lock:
        if frame.emit_quota <= 0:
            raise PolicyAdmissionError(
                "emit",
                EMIT_ONE_MORE,
                client,
                action,
                "one_more budget exhausted: exactly one more produce was "
                "allowed in this frame; this call is observably rejected",
                frame_id=frame.fid,
            )
        frame.emit_quota -= 1
    return _EmitTicket(frame, cid)


# ---------------------------------------------------------------------------
# Offset ledger helpers used by the client wrappers
# ---------------------------------------------------------------------------


def _partition_keys(
    consumer: Any, message: Optional[Message], offsets: Optional[Sequence[TopicPartition]]
) -> List[Tuple[str, int]]:
    if message is not None and offsets is not None:
        raise ValueError("message and offsets are mutually exclusive")
    if offsets is not None:
        return [(tp.topic, tp.partition) for tp in offsets]
    if message is not None:
        # A positional list is historical sugar for offsets= on some
        # call paths; a real Message exposes topic()/partition().
        if isinstance(message, (list, tuple)):
            return [(tp.topic, tp.partition) for tp in message]
        return [(message.topic(), message.partition())]
    # commit()/store_offsets() without arguments act on the current assignment
    assignment = _consumer_assignment(consumer)
    return [(tp.topic, tp.partition) for tp in assignment]


def _partition_keys_safe(
    consumer: Any, message: Optional[Message], offsets: Optional[Sequence[TopicPartition]]
) -> List[Tuple[str, int]]:
    """Like _partition_keys but never raises: bookkeeping must not mask the
    raw call's own validation/closed-client errors. Runs after the raw call."""
    try:
        return _partition_keys(consumer, message, offsets)
    except Exception:
        return []


def _mark_all_frames(client: Any, cid: int, keys: Sequence[Tuple[str, int]], state: str, **kwargs: Any) -> None:
    for frame in _stack_snapshot(client):
        for key in keys:
            frame.mark(cid, key, state, **kwargs)


def finish_offset_op(
    client: Any,
    keys: Sequence[Tuple[str, int]],
    state: str,
    entry_frame: Optional[_PolicyFrame],
    *,
    was_store: bool = False,
    prev_offsets: Optional[Dict[Tuple[str, int], int]] = None,
) -> None:
    """Bookkeep one completed commit/store/seek and enforce the
    started-before-push rule.

    If a different frame is on top when the operation returns, and that newer
    frame denies offsets or forces a rewind, the operation is *explicitly
    rejected* (never silently attributed to the entry frame) and its
    partitions are tainted on the newer frame so that frame's pop rewinds
    them.
    """
    cid = id(client)
    top = _top_frame(client)
    changed = top is not None and top is not entry_frame and top.offset_gate in (OFFSET_DENY_NEW, OFFSET_FORCE_REWIND)

    for frame in _stack_snapshot(client):
        if changed and frame is top:
            # The operation was already in flight when this frame entered;
            # never silently call it committed.
            for key in keys:
                frame.mark(cid, key, _ST_TAINTED)
            continue
        for key in keys:
            prev = prev_offsets.get(key) if (was_store and prev_offsets) else None
            frame.mark(cid, key, state, was_store=was_store, prev_offset=prev)

    if changed:
        raise PolicyAdmissionError(
            "offset",
            top.offset_gate,
            client,
            state,
            "the offset operation started under a different frame and a newer "
            "deny_new/force_rewind frame is now on top; the operation is "
            "explicitly rejected and the partitions are tainted for rewind",
            frame_id=top.fid,
        )


def mark_closed(consumer: Any, partitions: Sequence[TopicPartition]) -> None:
    """Rebalance revoke/lost: mark the partitions closed in every live frame."""
    cid = id(consumer)
    keys = [(tp.topic, tp.partition) for tp in partitions]
    _mark_all_frames(consumer, cid, keys, _ST_CLOSED)


def record_txn_offsets(producer: Any, positions: Sequence[TopicPartition]) -> _TxnEntry:
    top = _top_frame(producer)
    if top is None:
        # Should not happen: the wrapper only calls this with a live frame.
        raise PolicyAdmissionError(
            "offset",
            OFFSET_HONOR_CFG,
            producer,
            "send_offsets_to_transaction",
            "internal error: no policy frame bound to producer",
        )
    entry = top.add_txn_entry(id(producer), list(positions))
    return entry


def txn_signal(producer: Any, state: str) -> None:
    for frame in _stack_snapshot(producer):
        frame.txn_signal(id(producer), state)


# ---------------------------------------------------------------------------
# push / pop
# ---------------------------------------------------------------------------


def push(
    *clients: Any,
    emit_admit: str = EMIT_HONOR_CFG,
    fetch_admit: str = FETCH_HONOR_CFG,
    offset_gate: str = OFFSET_HONOR_CFG,
    emit_settle_timeout: Optional[float] = None,
    drain_poll_timeout: float = 0.0,
    drain_max_polls: int = 10000,
    rewind_committed_timeout: float = 5.0,
) -> _PolicyFrame:
    """Push a runtime policy frame bound to the given client(s).

    Returns an opaque token for :func:`pop`. Frame gates only apply to the
    clients listed here; other producers/consumers in the process keep
    following the plain API.
    """
    from ._clients import Consumer as _WrappedConsumer
    from ._clients import Producer as _WrappedProducer

    if not clients:
        raise ValueError("push() requires at least one Producer or Consumer")
    for client in clients:
        if not isinstance(client, (_WrappedProducer, _WrappedConsumer)):
            raise TypeError(
                "policy frames only bind confluent_kafka.Producer/Consumer " "instances, got {!r}".format(type(client))
            )
    if emit_admit not in _EMIT_GATES:
        raise ValueError("invalid emit_admit: {!r}".format(emit_admit))
    if fetch_admit not in _FETCH_GATES:
        raise ValueError("invalid fetch_admit: {!r}".format(fetch_admit))
    if offset_gate not in _OFFSET_GATES:
        raise ValueError("invalid offset_gate: {!r}".format(offset_gate))

    options = {
        "emit_settle_timeout": emit_settle_timeout,
        "drain_poll_timeout": drain_poll_timeout,
        "drain_max_polls": drain_max_polls,
        "rewind_committed_timeout": rewind_committed_timeout,
    }
    frame = _PolicyFrame(tuple(clients), emit_admit, fetch_admit, offset_gate, options)
    with _registry_lock:
        for client in clients:
            cid = _register_client(client)
            _stacks[cid].append(frame)
    return frame


def pop(frame: _PolicyFrame) -> FrameSettlement:
    """Remove a frame from every client stack and settle it.

    Always removes the frame (even when the body raised) and runs at most
    once. Settlement never cancels in-flight records, never touches clients
    not bound to the frame and never touches partitions not opened inside
    the frame.
    """
    with _registry_lock:
        for cid in frame.client_ids:
            stack = _stacks.get(cid)
            if stack is not None and frame in stack:
                stack.remove(frame)
    if frame.settled:
        report = FrameSettlement(frame.fid)
        report.errors.append("frame already settled")
        return report
    frame.settled = True
    return _settle(frame)


@contextlib.contextmanager
def scope(
    *clients: Any,
    emit_admit: str = EMIT_HONOR_CFG,
    fetch_admit: str = FETCH_HONOR_CFG,
    offset_gate: str = OFFSET_HONOR_CFG,
    **kwargs: Any,
):
    """Context manager around :func:`push`/:func:`pop` (exception-safe pop)."""
    frame = push(*clients, emit_admit=emit_admit, fetch_admit=fetch_admit, offset_gate=offset_gate, **kwargs)
    try:
        yield frame
    finally:
        pop(frame)


# ---------------------------------------------------------------------------
# Settlement
# ---------------------------------------------------------------------------


def _settle(frame: _PolicyFrame) -> FrameSettlement:
    report = FrameSettlement(frame.fid)

    # 1. emit axis: drive ready delivery reports to observable end. This
    #    never purges/cancels in-flight records; deny_new explicitly forbids
    #    that. A bounded flush runs only when the caller asked for one.
    for client in frame.clients:
        if not isinstance(client, _ProducerImpl):
            continue
        cid = id(client)
        with frame.lock:
            admitted = frame.emit_admitted.get(cid, 0)
        if admitted <= 0:
            continue
        try:
            outstanding = len(client)
            client.poll(0)
            timeout = frame.options.get("emit_settle_timeout")
            remaining = client.flush(timeout) if timeout is not None else outstanding
            report.emit[cid] = {
                "admitted": admitted,
                "outstanding": outstanding,
                "remaining": int(remaining) if remaining is not None else outstanding,
            }
        except Exception as exc:  # observable, never silent
            report.errors.append("emit settlement failed: {!r}".format(exc))

    # 2. fetch axis: pull the bound consumer's visible queue to the boundary.
    #    Only this exact client is polled -- neighbor consumers cannot be hit.
    for client in frame.clients:
        if not isinstance(client, _ConsumerImpl):
            continue
        if frame.fetch_admit != FETCH_FORCE_DRAIN:
            continue
        cid = id(client)
        pulled: List[Message] = []
        attempts = 0
        max_polls = int(frame.options.get("drain_max_polls", 10000))
        poll_timeout = float(frame.options.get("drain_poll_timeout", 0.0))
        try:
            while attempts < max_polls:
                msg = _consumer_poll(client, poll_timeout)
                if msg is None:
                    break
                pulled.append(msg)
                attempts += 1
        except Exception as exc:
            report.errors.append("force_drain failed: {!r}".format(exc))
        frame.drained[cid] = pulled
        report.drained[cid] = pulled

    # 3. offset axis: rewind stored/tainted partitions to their committed
    #    positions -- only partitions opened inside THIS frame, only on
    #    consumers bound to THIS frame.
    for client in frame.clients:
        if not isinstance(client, _ConsumerImpl):
            continue
        if frame.offset_gate != OFFSET_FORCE_REWIND:
            continue
        _settle_rewind(frame, client, report)

    # 4. EOS axis: half-transaction consumer offsets are observable. We never
    #    abort the producer transaction (it may carry unrelated records).
    _settle_transactions(frame, report)

    return report


def _settle_rewind(frame: _PolicyFrame, consumer: Any, report: FrameSettlement) -> None:
    cid = id(consumer)
    timeout = float(frame.options.get("rewind_committed_timeout", 5.0))
    with frame.lock:
        entries = list(frame.ledger.get(cid, {}).values())

    for entry in entries:
        key = (entry.topic, entry.partition)
        if entry.state == _ST_COMMITTED:
            report.left_committed.append(key)
            continue
        if entry.state == _ST_SOUGHT:
            report.left_sought.append(key)
            continue
        if entry.state == _ST_CLOSED:
            report.left_closed.append(key)
            continue
        if entry.state not in (_ST_STORED, _ST_TAINTED):
            continue

        probe = TopicPartition(entry.topic, entry.partition)
        try:
            committed = _consumer_committed(consumer, [probe], timeout)
        except Exception as exc:
            report.rewind_pending.append((probe, "committed() failed: {!r}".format(exc)))
            continue
        if not committed:
            report.rewind_pending.append((probe, "no committed offset available"))
            continue
        ctp = committed[0]
        if ctp.offset is None or ctp.offset < 0:
            report.rewind_pending.append((ctp, "no committed offset (offset={!r})".format(ctp.offset)))
            continue

        target = TopicPartition(ctp.topic, ctp.partition, ctp.offset)
        try:
            _consumer_seek(consumer, target)
        except Exception as exc:
            report.rewind_pending.append((target, "seek() failed: {!r}".format(exc)))
            continue

        # Neutralize a staged offset only if store_offsets actually worked in
        # this frame (it is unavailable with enable.auto.commit=True).
        if entry.state == _ST_STORED and entry.was_store:
            try:
                _consumer_store_offsets(consumer, offsets=[target])
            except Exception as exc:
                report.errors.append(
                    "overwrite staged offset for {}-{} failed: {!r}".format(entry.topic, entry.partition, exc)
                )

        report.rewound.append(target)


def _settle_transactions(frame: _PolicyFrame, report: FrameSettlement) -> None:
    consumers = [c for c in frame.clients if isinstance(c, _ConsumerImpl)]

    with frame.lock:
        txn_entries = [(cid, list(entries)) for cid, entries in frame.txn_entries.items()]

    for producer_cid, entries in txn_entries:
        for entry in entries:
            if entry.state in ("committed", "aborted"):
                continue
            item: Dict[str, Any] = {
                "producer_cid": producer_cid,
                "status": "pending_transaction",
                "positions": [(tp.topic, tp.partition, tp.offset) for tp in entry.positions],
                "rewound": [],
            }
            # Best effort: rewind the consumption side on a consumer bound to
            # this same frame; never touch a neighbor consumer.
            for tp in entry.positions:
                for consumer in consumers:
                    if not _consumer_owns(consumer, tp):
                        continue
                    probe = TopicPartition(tp.topic, tp.partition)
                    try:
                        committed = _consumer_committed(
                            consumer, [probe], float(frame.options.get("rewind_committed_timeout", 5.0))
                        )
                        ctp = committed[0]
                        if ctp.offset is not None and ctp.offset >= 0:
                            _consumer_seek(consumer, TopicPartition(ctp.topic, ctp.partition, ctp.offset))
                            item["rewound"].append((ctp.topic, ctp.partition, ctp.offset))
                    except Exception as exc:
                        report.errors.append(
                            "transaction offset rewind {}-{} failed: {!r}".format(tp.topic, tp.partition, exc)
                        )
                    break
            report.transaction_pending.append(item)


def _consumer_owns(consumer: Any, tp: TopicPartition) -> bool:
    try:
        assignment = _consumer_assignment(consumer)
    except Exception:
        return False
    return any(a.topic == tp.topic and a.partition == tp.partition for a in assignment)


# ---------------------------------------------------------------------------
# Merged view (construction config + live stack)
# ---------------------------------------------------------------------------

_PRODUCER_PINNED = {
    "acks": ("acks", "request.required.acks"),
    "linger.ms": ("linger.ms", "linger_ms"),
    "batch.num.messages": ("batch.num.messages", "batch_num_messages"),
}
_CONSUMER_PINNED = {
    "isolation.level": ("isolation.level", "isolation_level"),
    "auto.offset.reset": ("auto.offset.reset", "auto_offset_reset"),
}


def _capture_conf_view(args: tuple, kwargs: Dict[str, Any], pinned: Dict[str, Tuple[str, ...]]) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    if args and isinstance(args[0], dict):
        merged.update(args[0])
    merged.update(kwargs)
    view: Dict[str, Any] = {}
    for canonical, aliases in pinned.items():
        value = None
        for alias in aliases:
            if alias in merged:
                value = merged[alias]
                break
        if value is None:
            dotted = canonical.replace(".", "_")
            value = merged.get(dotted, value)
        view[canonical] = value
    return view


def effective_view(client: Any, **call_overrides: Any) -> Dict[str, Any]:
    """Merged observable view: pinned construction config plus the gates the
    *next* call on this client would resolve from the live stack.

    The config section is read-only evidence; this API never mutates it.
    """
    is_producer = isinstance(client, _ProducerImpl)
    is_consumer = isinstance(client, _ConsumerImpl)
    if not (is_producer or is_consumer):
        raise TypeError("expected a Producer or Consumer")

    with _registry_lock:
        conf_view = dict(_conf_views.get(id(client), {}))

    _, emit = _resolved_gate(client, "emit_admit")
    _, fetch = _resolved_gate(client, "fetch_admit")
    _, offset = _resolved_gate(client, "offset_gate")
    top = _top_frame(client)

    one_more_remaining: Optional[int] = None
    if emit == EMIT_ONE_MORE and top is not None:
        with top.lock:
            one_more_remaining = top.emit_quota

    view = {
        "client": "producer" if is_producer else "consumer",
        "config": dict(conf_view),
        "emit_admit": emit,
        "fetch_admit": fetch,
        "offset_gate": offset,
        "frame_depth": len(_stack_snapshot(client)),
        "frame_id": top.fid if top is not None else None,
        "one_more_remaining": one_more_remaining,
    }
    if call_overrides:
        view["call"] = dict(call_overrides)
    return view


# ---------------------------------------------------------------------------
# Raw librdkafka seams.
#
# All client wrappers go through these module-level functions instead of
# calling the cimpl methods directly, so the policy layer stays the single
# wiring point and the behavior can be exercised without a broker.
# ---------------------------------------------------------------------------


def _producer_produce(client: Any, *args: Any, **kwargs: Any) -> None:
    _ProducerImpl.produce(client, *args, **kwargs)


def _producer_produce_batch(client: Any, *args: Any, **kwargs: Any) -> int:
    return _ProducerImpl.produce_batch(client, *args, **kwargs)


def _producer_poll(client: Any, timeout: float = -1) -> int:
    return _ProducerImpl.poll(client, timeout)


def _producer_flush(client: Any, timeout: float = -1) -> int:
    return _ProducerImpl.flush(client, timeout)


def _producer_send_offsets_to_transaction(
    client: Any, positions: Any, group_metadata: Any, timeout: float = -1
) -> None:
    _ProducerImpl.send_offsets_to_transaction(client, positions, group_metadata, timeout)


def _producer_begin_transaction(client: Any) -> None:
    _ProducerImpl.begin_transaction(client)


def _producer_commit_transaction(client: Any, timeout: float = -1) -> None:
    _ProducerImpl.commit_transaction(client, timeout)


def _producer_abort_transaction(client: Any, timeout: float = -1) -> None:
    _ProducerImpl.abort_transaction(client, timeout)


def _consumer_poll(client: Any, timeout: float = -1):
    return _ConsumerImpl.poll(client, timeout)


def _consumer_consume(client: Any, num_messages: int = 1, timeout: float = -1):
    return _ConsumerImpl.consume(client, num_messages, timeout)


def _consumer_subscribe(
    client: Any, topics: Any, on_assign: Any = None, on_revoke: Any = None, on_lost: Any = None
) -> None:
    # cimpl rejects explicit None callbacks ("expects a callable"); only
    # forward callbacks that were actually supplied so it keeps its defaults.
    kwargs = {}
    if on_assign is not None:
        kwargs["on_assign"] = on_assign
    if on_revoke is not None:
        kwargs["on_revoke"] = on_revoke
    if on_lost is not None:
        kwargs["on_lost"] = on_lost
    _ConsumerImpl.subscribe(client, topics, **kwargs)


def _consumer_assignment(client: Any):
    return _ConsumerImpl.assignment(client)


def _consumer_position(client: Any, partitions: Any):
    return _ConsumerImpl.position(client, partitions)


def _consumer_committed(client: Any, partitions: Any, timeout: float = -1):
    return _ConsumerImpl.committed(client, partitions, timeout)


def _consumer_commit(client: Any, message: Any = None, offsets: Any = None, asynchronous: bool = True):
    return _ConsumerImpl.commit(client, message=message, offsets=offsets, asynchronous=asynchronous)


def _consumer_store_offsets(client: Any, message: Any = None, offsets: Any = None) -> None:
    _ConsumerImpl.store_offsets(client, message=message, offsets=offsets)


def _consumer_seek(client: Any, partition: Any) -> None:
    _ConsumerImpl.seek(client, partition)


# Importing _clients registers the wrapped Producer/Consumer classes; keep at
# the bottom to avoid an import cycle.
from ._clients import Consumer, Producer  # noqa: E402,F401

__all__ = [
    "EMIT_HONOR_CFG",
    "EMIT_DENY_NEW",
    "EMIT_ONE_MORE",
    "FETCH_HONOR_CFG",
    "FETCH_DENY_NEW",
    "FETCH_FORCE_DRAIN",
    "OFFSET_HONOR_CFG",
    "OFFSET_DENY_NEW",
    "OFFSET_FORCE_REWIND",
    "PolicyAdmissionError",
    "FrameSettlement",
    "push",
    "pop",
    "scope",
    "effective_view",
    "Producer",
    "Consumer",
]

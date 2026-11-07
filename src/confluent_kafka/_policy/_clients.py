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
Producer/Consumer wrappers that wire every produce / fetch / offset exit
point through the runtime policy stack in :mod:`confluent_kafka._policy`.

The wrappers add no required constructor parameter and perform no behavior
change while no frame is bound: every method forwards one-to-one to the
cimpl implementation. They do not hold any per-Message/per-callback private
bag -- all runtime state lives in the policy stack, keyed by client.
"""

from __future__ import annotations

from typing import Any, List, Optional

from ..cimpl import Consumer as _ConsumerImpl
from ..cimpl import Message, TopicPartition
from ..cimpl import Producer as _ProducerImpl
from . import (
    OFFSET_DENY_NEW,
    OFFSET_FORCE_REWIND,
    _CONSUMER_PINNED,
    _PRODUCER_PINNED,
    _ST_COMMITTED,
    _ST_SOUGHT,
    _ST_STORED,
)

# Raw librdkafka seams are kept as module attributes (P._consumer_poll etc.)
# so they stay patchable and the policy module remains the single wiring
# point between these wrappers and cimpl. Absolute self-import: at import
# time the package module is partially initialized but present.
import confluent_kafka._policy as P
from . import (
    _capture_conf_view,
    _partition_keys_safe,
    _top_frame,
    admit_emit,
    check_fetch,
    check_offset,
    finish_offset_op,
    mark_closed,
    record_txn_offsets,
    txn_signal,
)


class Producer(_ProducerImpl):
    """Producer with the runtime policy stack wired into every emit exit.

    ``poll``/``flush`` are deliberately *not* gated by ``emit_admit``: they
    are how an already accepted record (and its delivery report) reaches an
    observable end, and cancelling them would violate the in-flight
    guarantees of ``deny_new``.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # Read-only evidence for effective_view(), held by the policy stack
        # registry (never on the client itself, never mutated back into the
        # client: no acks/linger/batch/isolation changes).
        P.register_conf_view(self, _capture_conf_view(args, kwargs, _PRODUCER_PINNED))

    def produce(
        self,
        topic: str,
        value: Any = None,
        key: Any = None,
        partition: int = -1,
        callback: Any = None,
        on_delivery: Any = None,
        timestamp: int = 0,
        headers: Any = None,
    ) -> None:
        ticket = admit_emit(self, "produce")
        try:
            P._producer_produce(
                self,
                topic,
                value,
                key,
                partition,
                callback if callback is not None else on_delivery,
                None,
                timestamp,
                headers,
            )
        except BaseException:
            if ticket is not None:
                ticket.rollback()
            raise
        if ticket is not None:
            ticket.admitted()

    def produce_batch(
        self, topic: str, messages: List[dict], partition: int = -1, callback: Any = None, on_delivery: Any = None
    ) -> int:
        ticket = admit_emit(self, "produce_batch")
        try:
            accepted = P._producer_produce_batch(self, topic, messages, partition, callback, on_delivery)
        except BaseException:
            if ticket is not None:
                ticket.rollback()
            raise
        if ticket is not None:
            # The one_more budget pays for one admission; a fully failed batch
            # consumes nothing.
            if accepted and accepted > 0:
                ticket.admitted()
            else:
                ticket.rollback()
        return accepted

    # poll/flush are ungated on purpose -- they deliver in-flight records and
    # pump delivery callbacks, they do not admit new records.
    def poll(self, timeout: float = -1) -> int:
        return P._producer_poll(self, timeout)

    def flush(self, timeout: float = -1) -> int:
        return P._producer_flush(self, timeout)

    # -- EOS transaction surface -------------------------------------------

    def send_offsets_to_transaction(
        self, positions: List[TopicPartition], group_metadata: Any, timeout: float = -1
    ) -> None:
        check_offset(self, "send_offsets_to_transaction")
        entry_frame = _top_frame(self)
        P._producer_send_offsets_to_transaction(self, positions, group_metadata, timeout)
        if entry_frame is not None:
            record_txn_offsets(self, positions)
        top = _top_frame(self)
        if top is not None and top is not entry_frame and top.offset_gate in (OFFSET_DENY_NEW, OFFSET_FORCE_REWIND):
            # Frame changed mid-flight: make it observable instead of letting
            # the entry frame silently own these offsets.
            entry = record_txn_offsets(self, positions)
            entry.tainted = True
            raise P.PolicyAdmissionError(
                "offset",
                top.offset_gate,
                self,
                "send_offsets_to_transaction",
                "send_offsets_to_transaction started under a different frame "
                "and a newer deny_new/force_rewind frame is now on top",
                frame_id=top.fid,
            )

    def begin_transaction(self) -> None:
        P._producer_begin_transaction(self)
        txn_signal(self, "begun")

    def commit_transaction(self, timeout: float = -1) -> None:
        P._producer_commit_transaction(self, timeout)
        txn_signal(self, "committed")

    def abort_transaction(self, timeout: float = -1) -> None:
        P._producer_abort_transaction(self, timeout)
        txn_signal(self, "aborted")


class _ConsumerIterator:
    """Iterator-style fetch; every step re-enters the gated ``poll``."""

    def __init__(self, consumer: "Consumer", timeout: float) -> None:
        self._consumer = consumer
        self._timeout = timeout

    def __iter__(self) -> "_ConsumerIterator":
        return self

    def __next__(self) -> Message:
        while True:
            # The gate is re-resolved on every poll. deny_new raises
            # PolicyAdmissionError here, never StopIteration and never an
            # empty/fabricated message.
            msg = self._consumer.poll(self._timeout)
            if msg is not None:
                return msg


class Consumer(_ConsumerImpl):
    """Consumer with the runtime policy stack wired into every fetch and
    offset exit."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        P.register_conf_view(self, _capture_conf_view(args, kwargs, _CONSUMER_PINNED))

    # -- fetch exits --------------------------------------------------------

    def poll(self, timeout: float = -1) -> Optional[Message]:
        # Gate checked at entry: a deny_new pushed after this call entered
        # (from another thread or from a nested callback) neither cancels the
        # in-flight poll nor turns it into an empty success; the next poll is
        # what gets denied.
        check_fetch(self, "poll")
        return P._consumer_poll(self, timeout)

    def consume(self, num_messages: int = 1, timeout: float = -1) -> List[Message]:
        check_fetch(self, "consume")
        return P._consumer_consume(self, num_messages, timeout)

    def __iter__(self) -> _ConsumerIterator:
        return _ConsumerIterator(self, timeout=1.0)

    # -- subscription / rebalance callbacks ---------------------------------

    def subscribe(self, topics: List[str], on_assign: Any = None, on_revoke: Any = None, on_lost: Any = None) -> None:
        # The shims resolve the stack when the callback *fires* (live), never
        # a snapshot taken at subscribe() entry time.
        wrapped_revoke = self._wrap_rebalance(on_revoke, "revoke") if on_revoke else None
        wrapped_lost = self._wrap_rebalance(on_lost, "lost") if on_lost else None
        wrapped_assign = on_assign
        P._consumer_subscribe(self, topics, wrapped_assign, wrapped_revoke, wrapped_lost)

    def _wrap_rebalance(self, callback: Any, kind: str) -> Any:
        consumer = self

        def shim(ctrl: Any, partitions: List[TopicPartition]) -> None:
            # Partitions revoked/lost while a frame is active can no longer be
            # seeked/stored; close them in every live frame (live lookup).
            if partitions:
                mark_closed(consumer, partitions)
            return callback(ctrl, partitions)

        shim.__wrapped__ = callback
        return shim

    # -- offset exits -------------------------------------------------------

    def commit(
        self,
        message: Optional[Message] = None,
        offsets: Optional[List[TopicPartition]] = None,
        asynchronous: bool = True,
        **kwargs: Any,
    ):
        # C accepts the legacy "async" alias as well.
        if "async" in kwargs:
            asynchronous = kwargs["async"]
        check_offset(self, "commit")
        entry_frame = _top_frame(self)
        result = P._consumer_commit(self, message, offsets, asynchronous)
        # Bookkeeping only after the raw call succeeded, so it can never mask
        # cimpl's own validation / closed-handle errors. Keys are computed
        # live (never carried over from the entry frame snapshot).
        keys = _partition_keys_safe(self, message, offsets)
        if keys:
            finish_offset_op(self, keys, _ST_COMMITTED, entry_frame)
        return result

    def store_offsets(self, message: Optional[Message] = None, offsets: Optional[List[TopicPartition]] = None) -> None:
        check_offset(self, "store_offsets")
        entry_frame = _top_frame(self)
        prev_offsets = None
        if entry_frame is not None:
            prev_offsets = self._capture_positions(message, offsets)
        P._consumer_store_offsets(self, message, offsets)
        keys = _partition_keys_safe(self, message, offsets)
        if keys:
            finish_offset_op(self, keys, _ST_STORED, entry_frame, was_store=True, prev_offsets=prev_offsets)

    def seek(self, partition: TopicPartition) -> None:
        check_offset(self, "seek")
        entry_frame = _top_frame(self)
        P._consumer_seek(self, partition)
        finish_offset_op(self, [(partition.topic, partition.partition)], _ST_SOUGHT, entry_frame)

    def _capture_positions(self, message: Optional[Message], offsets: Optional[List[TopicPartition]]) -> Optional[dict]:
        """Best-effort fetch positions before a store, for the frame ledger."""
        try:
            if offsets is not None:
                probes = [TopicPartition(tp.topic, tp.partition) for tp in offsets]
            elif isinstance(message, (list, tuple)):
                probes = [TopicPartition(tp.topic, tp.partition) for tp in message]
            elif message is not None:
                probes = [TopicPartition(message.topic(), message.partition())]
            else:
                probes = list(_ConsumerImpl.assignment(self))
            if not probes:
                return None
            positions = P._consumer_position(self, probes)
            return {(tp.topic, tp.partition): tp.offset for tp in positions}
        except Exception:
            return None

#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
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
Unit tests for the runtime execution-policy stack
(``confluent_kafka.policy`` / ``confluent_kafka._policy``).

These tests run without a broker: producers accept records into their local
queue, consumers poll/assignment/position work locally, and every librdkafka
exit that would talk to a broker is replaced through the documented raw
seams on the ``_policy`` module.
"""

import threading

import pytest

from confluent_kafka import Consumer, PolicyAdmissionError, Producer, SerializingProducer, TopicPartition, policy
from confluent_kafka import cimpl
from confluent_kafka import _policy as P

PRODUCER_CONF = {
    "bootstrap.servers": "nonexistent-broker:9092",
    "socket.timeout.ms": 10,
    "message.timeout.ms": 100,
}
CONSUMER_CONF = {
    "bootstrap.servers": "nonexistent-broker:9092",
    "group.id": "policy-stack-tests",
    "socket.timeout.ms": 10,
    "session.timeout.ms": 100,
    "enable.auto.commit": False,
}


@pytest.fixture
def producer():
    p = Producer(dict(PRODUCER_CONF))
    yield p
    try:
        p.poll(0)
        p.flush(0.5)
    except Exception:
        pass


@pytest.fixture
def producer2():
    p = Producer(dict(PRODUCER_CONF))
    yield p
    try:
        p.poll(0)
        p.flush(0.5)
    except Exception:
        pass


@pytest.fixture
def consumer():
    c = Consumer(dict(CONSUMER_CONF))
    yield c
    try:
        c.close()
    except Exception:
        pass


@pytest.fixture
def consumer2():
    c = Consumer({**CONSUMER_CONF, "group.id": "policy-stack-tests-2"})
    yield c
    try:
        c.close()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Empty stack == pinned behavior
# ---------------------------------------------------------------------------


def test_empty_stack_matches_plain_api(producer, consumer):
    pv = policy.effective_view(producer)
    cv = policy.effective_view(consumer)
    assert pv["emit_admit"] == policy.EMIT_HONOR_CFG
    assert cv["fetch_admit"] == policy.FETCH_HONOR_CFG
    assert cv["offset_gate"] == policy.OFFSET_HONOR_CFG
    assert pv["frame_depth"] == 0 and cv["frame_depth"] == 0
    assert pv["frame_id"] is None and cv["frame_id"] is None

    producer.produce("topic", value=b"a")
    assert len(producer) == 1
    assert consumer.poll(0) is None
    assert consumer.consume(1, 0) == []
    consumer.pause([])
    consumer.resume([])


def test_no_new_required_constructor_arguments():
    # Same single-dict construction as always; no mandatory new args.
    p = Producer({"bootstrap.servers": "nonexistent-broker:9092"})
    c = Consumer({"bootstrap.servers": "nonexistent-broker:9092", "group.id": "g"})
    p.poll(0)
    c.close()


def test_push_validates_clients():
    with pytest.raises(ValueError):
        policy.push()
    raw = cimpl.Producer({"bootstrap.servers": "nonexistent-broker:9092"})
    try:
        with pytest.raises(TypeError):
            policy.push(raw)
    finally:
        raw.poll(0)


def test_effective_view_is_merged_with_construction_config():
    p = Producer(
        {
            "bootstrap.servers": "nonexistent-broker:9092",
            "acks": "all",
            "linger.ms": 20,
            "batch.num.messages": 100,
        }
    )
    c = Consumer(
        {
            "bootstrap.servers": "nonexistent-broker:9092",
            "group.id": "g",
            "isolation.level": "read_committed",
            "auto.offset.reset": "earliest",
        }
    )
    pv = policy.effective_view(p)
    cv = policy.effective_view(c, action="poll")
    assert pv["config"]["acks"] == "all"
    assert pv["config"]["linger.ms"] == 20
    assert pv["config"]["batch.num.messages"] == 100
    assert cv["config"]["isolation.level"] == "read_committed"
    assert cv["config"]["auto.offset.reset"] == "earliest"
    assert cv["call"] == {"action": "poll"}
    c.close()


# ---------------------------------------------------------------------------
# emit_admit
# ---------------------------------------------------------------------------


def test_deny_new_emit_observably_rejects_and_isolates_neighbor(producer, producer2):
    producer2.produce("other", value=b"before")
    with policy.scope(producer, emit_admit=policy.EMIT_DENY_NEW):
        with pytest.raises(PolicyAdmissionError) as ei:
            producer.produce("topic", value=b"x")
        assert ei.value.axis == "emit"
        assert ei.value.gate == policy.EMIT_DENY_NEW
        # producer2 never pushed a frame: plain API all the way.
        producer2.produce("other", value=b"during")
    # pop restores the outer (empty-stack) view.
    producer.produce("topic", value=b"after")
    assert len(producer) == 1


def test_deny_new_does_not_cancel_inflight_records(producer):
    producer.produce("topic", value=b"inflight")
    assert len(producer) == 1
    with policy.scope(producer, emit_admit=policy.EMIT_DENY_NEW):
        # Already accepted record stays queued, never cancelled / purged,
        # and flush/poll (the delivery paths) are not denied.
        assert len(producer) == 1
        producer.poll(0)
        producer.flush(0.01)
        assert len(producer) >= 1
        with pytest.raises(PolicyAdmissionError):
            producer.produce("topic", value=b"new")


def test_one_more_allows_exactly_one_and_counts_successful_admission(producer):
    with policy.scope(producer, emit_admit=policy.EMIT_ONE_MORE):
        view = policy.effective_view(producer)
        assert view["one_more_remaining"] == 1
        producer.produce("topic", value=b"one")
        assert policy.effective_view(producer)["one_more_remaining"] == 0
        with pytest.raises(PolicyAdmissionError):
            producer.produce("topic", value=b"two")
        with pytest.raises(PolicyAdmissionError):
            producer.produce("topic", value=b"three")
    producer.produce("topic", value=b"after")


def test_one_more_failed_admission_returns_budget(producer, monkeypatch):
    def boom(*args, **kwargs):
        raise BufferError("queue full")

    with policy.scope(producer, emit_admit=policy.EMIT_ONE_MORE):
        monkeypatch.setattr(P, "_producer_produce", boom)
        with pytest.raises(BufferError):
            producer.produce("topic", value=b"fail")
        monkeypatch.undo()
        # Failed acceptance did not consume the one_more budget.
        producer.produce("topic", value=b"real-one")
        with pytest.raises(PolicyAdmissionError):
            producer.produce("topic", value=b"next")


def test_one_more_second_call_rereads_live_top(producer):
    # Outer honor frame + inner one_more frame.
    outer = policy.push(producer)
    inner = policy.push(producer, emit_admit=policy.EMIT_ONE_MORE)
    producer.produce("topic", value=b"inner-one")
    with pytest.raises(PolicyAdmissionError):
        producer.produce("topic", value=b"inner-two")
    policy.pop(inner)
    # Second produce after pop must re-read the new top (outer honor_cfg).
    producer.produce("topic", value=b"outer-allowed")

    # Now an outer deny_new frame proves inner pop restores outer gates.
    policy.pop(outer)
    deny = policy.push(producer, emit_admit=policy.EMIT_DENY_NEW)
    extra = policy.push(producer, emit_admit=policy.EMIT_ONE_MORE)
    producer.produce("topic", value=b"extra-one")
    policy.pop(extra)
    with pytest.raises(PolicyAdmissionError):
        producer.produce("topic", value=b"outer-denies")
    policy.pop(deny)


def test_produce_batch_goes_through_same_emit_gate(producer):
    with policy.scope(producer, emit_admit=policy.EMIT_DENY_NEW):
        with pytest.raises(PolicyAdmissionError):
            producer.produce_batch("topic", [{"value": b"a"}])
    with policy.scope(producer, emit_admit=policy.EMIT_ONE_MORE):
        accepted = producer.produce_batch("topic", [{"value": b"a"}, {"value": b"b"}])
        assert accepted >= 1
        with pytest.raises(PolicyAdmissionError):
            producer.produce_batch("topic", [{"value": b"c"}])


def test_serializing_producer_denied_before_serializers_run():
    side_effects = []

    def spying_serializer(obj, ctx):
        side_effects.append(obj)
        return b"x"

    sp = SerializingProducer(
        {
            "bootstrap.servers": "nonexistent-broker:9092",
            "message.timeout.ms": 100,
            "value.serializer": spying_serializer,
        }
    )
    try:
        with policy.scope(sp, emit_admit=policy.EMIT_DENY_NEW):
            with pytest.raises(PolicyAdmissionError):
                sp.produce("topic", value={"k": 1})
        assert side_effects == []
    finally:
        sp.poll(0)
        sp.flush(0.5)


# ---------------------------------------------------------------------------
# fetch_admit
# ---------------------------------------------------------------------------


def test_deny_new_fetch_covers_poll_consume_and_iterator(consumer, consumer2):
    with policy.scope(consumer, fetch_admit=policy.FETCH_DENY_NEW):
        with pytest.raises(PolicyAdmissionError):
            consumer.poll(0)
        with pytest.raises(PolicyAdmissionError):
            consumer.consume(1, 0)
        it = iter(consumer)
        with pytest.raises(PolicyAdmissionError):
            # Observable rejection -- never StopIteration, never a fake msg.
            next(it)
        # Neighbor consumer with no frame follows the plain API.
        assert consumer2.poll(0) is None
        assert consumer2.consume(1, 0) == []


def test_deny_pushed_after_poll_started_neither_cancels_nor_fakes_success(consumer, monkeypatch):
    msg = cimpl.Message(topic="topic", partition=0, offset=5, value=b"m")
    inner_frames = []

    def raw_poll(client, timeout=-1):
        # Equivalent to another scope pushing deny while this poll is in
        # flight: the raw fetch has started, then the frame changes.
        inner_frames.append(policy.push(client, fetch_admit=policy.FETCH_DENY_NEW))
        return msg

    monkeypatch.setattr(P, "_consumer_poll", raw_poll)
    # This poll was admitted under the empty stack and completes normally;
    # it is not cancelled and not rewritten into None.
    assert consumer.poll(1.0) is msg
    # The next fetch is observably denied.
    with pytest.raises(PolicyAdmissionError):
        consumer.poll(0)
    policy.pop(inner_frames[0])


def test_force_drain_pulls_to_boundary_without_hitting_neighbor_or_committing(consumer, consumer2, monkeypatch):
    m1 = cimpl.Message(topic="topic", partition=0, offset=0, value=b"a")
    m2 = cimpl.Message(topic="topic", partition=0, offset=1, value=b"b")
    sequence = iter([m1, m2, None])

    def raw_poll(client, timeout=-1):
        assert client is consumer
        return next(sequence)

    commits = []
    monkeypatch.setattr(P, "_consumer_poll", raw_poll)
    monkeypatch.setattr(P, "_consumer_commit", lambda *a, **k: commits.append((a, k)))

    frame = policy.push(consumer, fetch_admit=policy.FETCH_FORCE_DRAIN)
    settlement = policy.pop(frame)

    assert [m.offset() for m in settlement.drained[id(consumer)]] == [0, 1]
    assert commits == []  # draining never auto-commits
    # Neighbor consumer was never polled by the frame settlement; with the
    # frame popped and the patch removed it follows the plain API again.
    monkeypatch.undo()
    assert consumer2.poll(0) is None


# ---------------------------------------------------------------------------
# offset_gate
# ---------------------------------------------------------------------------


def test_deny_new_offset_blocks_commit_store_seek_and_eos(consumer, producer, monkeypatch):
    calls = []

    def spy(name):
        def fn(*args, **kwargs):
            calls.append(name)
            raise AssertionError("raw {} must not be reached".format(name))

        return fn

    monkeypatch.setattr(P, "_consumer_commit", spy("commit"))
    monkeypatch.setattr(P, "_consumer_store_offsets", spy("store"))
    monkeypatch.setattr(P, "_consumer_seek", spy("seek"))
    monkeypatch.setattr(P, "_producer_send_offsets_to_transaction", spy("send_offsets"))

    with policy.scope(consumer, producer, offset_gate=policy.OFFSET_DENY_NEW):
        with pytest.raises(PolicyAdmissionError):
            consumer.commit(offsets=[TopicPartition("t", 0, 1)], asynchronous=False)
        with pytest.raises(PolicyAdmissionError):
            consumer.store_offsets(offsets=[TopicPartition("t", 0, 1)])
        with pytest.raises(PolicyAdmissionError):
            consumer.seek(TopicPartition("t", 0, 0))
        with pytest.raises(PolicyAdmissionError):
            producer.send_offsets_to_transaction([TopicPartition("t", 0, 1)], object())
    assert calls == []
    # pause/resume are not commit/seek/store exits: they stay on the plain API.
    consumer.pause([])
    consumer.resume([])


def test_offset_op_pushing_deny_mid_flight_is_explicitly_rejected(consumer, monkeypatch):
    pushed = []

    def raw_commit(client, message=None, offsets=None, asynchronous=True):
        pushed.append(policy.push(client, offset_gate=policy.OFFSET_DENY_NEW))
        return [TopicPartition("t", 0, 5)]

    monkeypatch.setattr(P, "_consumer_commit", raw_commit)
    with pytest.raises(PolicyAdmissionError) as ei:
        consumer.commit(offsets=[TopicPartition("t", 0, 5)], asynchronous=False)
    assert ei.value.gate == policy.OFFSET_DENY_NEW
    # Nothing was silently marked committed; popping the late frame is a
    # no-op settlement (deny_new does not rewrite offsets by itself).
    settlement = policy.pop(pushed[0])
    assert settlement.rewound == []


def test_offset_op_pushing_force_rewind_mid_flight_gets_then_rewound(consumer, monkeypatch):
    pushed = []

    def raw_commit(client, message=None, offsets=None, asynchronous=True):
        pushed.append(policy.push(client, offset_gate=policy.OFFSET_FORCE_REWIND, rewind_committed_timeout=1.0))
        return [TopicPartition("t", 0, 5)]

    seeks = []
    monkeypatch.setattr(P, "_consumer_commit", raw_commit)
    monkeypatch.setattr(
        P,
        "_consumer_committed",
        lambda client, parts, timeout=-1: [TopicPartition(tp.topic, tp.partition, 4) for tp in parts],
    )
    monkeypatch.setattr(P, "_consumer_seek", lambda client, tp: seeks.append(tp))

    with pytest.raises(PolicyAdmissionError):
        consumer.commit(offsets=[TopicPartition("t", 0, 5)], asynchronous=False)

    settlement = policy.pop(pushed[0])
    assert [(tp.topic, tp.partition, tp.offset) for tp in settlement.rewound] == [("t", 0, 4)]
    assert [(tp.topic, tp.partition, tp.offset) for tp in seeks] == [("t", 0, 4)]


def test_force_rewind_scopes_to_frame_partitions_and_bound_consumer(consumer, consumer2, monkeypatch):
    seeks = []
    neighbor_committed_calls = []

    monkeypatch.setattr(P, "_consumer_store_offsets", lambda *a, **k: None)
    monkeypatch.setattr(
        P, "_consumer_commit", lambda client, message=None, offsets=None, asynchronous=True: list(offsets or [])
    )
    monkeypatch.setattr(
        P, "_consumer_position", lambda client, parts: [TopicPartition(tp.topic, tp.partition, 3) for tp in parts]
    )

    def fake_committed(client, parts, timeout=-1):
        if client is consumer2:
            neighbor_committed_calls.append(parts)
        offset = {("t", 0): 7, ("t", 1): 8}.get((parts[0].topic, parts[0].partition), 0)
        return [TopicPartition(tp.topic, tp.partition, offset) for tp in parts]

    monkeypatch.setattr(P, "_consumer_committed", fake_committed)
    monkeypatch.setattr(P, "_consumer_seek", lambda client, tp: seeks.append(tp))

    frame = policy.push(consumer, offset_gate=policy.OFFSET_FORCE_REWIND)
    consumer.store_offsets(offsets=[TopicPartition("t", 0, 4)])
    consumer.store_offsets(offsets=[TopicPartition("t", 1, 5)])
    # A later commit clears that partition.
    consumer.commit(offsets=[TopicPartition("t", 1, 9)], asynchronous=False)
    # An explicit user seek opts that partition out of automatic rewind.
    consumer.seek(TopicPartition("t", 2, 3))
    seeks.clear()  # isolate the seeks performed by pop-time settlement
    settlement = policy.pop(frame)

    assert [(tp.topic, tp.partition, tp.offset) for tp in settlement.rewound] == [("t", 0, 7)]
    assert [(tp.topic, tp.partition, tp.offset) for tp in seeks] == [("t", 0, 7)]
    assert ("t", 1) in settlement.left_committed
    assert ("t", 2) in settlement.left_sought
    assert neighbor_committed_calls == []


def test_eos_offsets_half_transaction_are_observable_and_rewound_on_bound_consumer(producer, consumer, monkeypatch):
    monkeypatch.setattr(P, "_producer_send_offsets_to_transaction", lambda *a, **k: None)
    monkeypatch.setattr(P, "_producer_begin_transaction", lambda *a, **k: None)
    monkeypatch.setattr(P, "_consumer_assignment", lambda client: [TopicPartition("t", 0)])
    monkeypatch.setattr(
        P,
        "_consumer_committed",
        lambda client, parts, timeout=-1: [TopicPartition(tp.topic, tp.partition, 3) for tp in parts],
    )
    seeks = []
    monkeypatch.setattr(P, "_consumer_seek", lambda client, tp: seeks.append(tp))

    frame = policy.push(producer, consumer, offset_gate=policy.OFFSET_FORCE_REWIND)
    # begin observed, but no commit_transaction before pop -> half transaction.
    producer.begin_transaction()
    producer.send_offsets_to_transaction([TopicPartition("t", 0, 4)], object())
    settlement = policy.pop(frame)

    pending = settlement.transaction_pending
    assert len(pending) == 1
    assert pending[0]["status"] == "pending_transaction"
    assert pending[0]["rewound"] == [("t", 0, 3)]
    assert [(tp.topic, tp.partition, tp.offset) for tp in seeks] == [("t", 0, 3)]


def test_eos_committed_transaction_is_not_pending(producer, monkeypatch):
    monkeypatch.setattr(P, "_producer_send_offsets_to_transaction", lambda *a, **k: None)
    monkeypatch.setattr(P, "_producer_begin_transaction", lambda *a, **k: None)
    monkeypatch.setattr(P, "_producer_commit_transaction", lambda *a, **k: None)
    frame = policy.push(producer, offset_gate=policy.OFFSET_FORCE_REWIND)
    producer.begin_transaction()
    producer.send_offsets_to_transaction([TopicPartition("t", 0, 4)], object())
    producer.commit_transaction()
    settlement = policy.pop(frame)
    assert settlement.transaction_pending == []


# ---------------------------------------------------------------------------
# Rebalance callbacks read the live stack
# ---------------------------------------------------------------------------


def test_rebalance_callback_reads_live_stack_not_subscribe_snapshot(consumer, monkeypatch):
    captured = {}

    def fake_subscribe(client, topics, on_assign, on_revoke, on_lost):
        captured["on_revoke"] = on_revoke
        captured["topics"] = topics

    monkeypatch.setattr(P, "_consumer_subscribe", fake_subscribe)
    seen = {}

    def on_revoke(ctrl, partitions):
        # Fires later; must see the frame that is live *now*, not whatever
        # (nothing) was live when subscribe() was called.
        seen["gate"] = policy.effective_view(ctrl)["offset_gate"]

    consumer.subscribe(["topic"], on_revoke=on_revoke)
    assert "gate" not in seen

    frame = policy.push(consumer, offset_gate=policy.OFFSET_FORCE_REWIND)
    captured["on_revoke"](consumer, [TopicPartition("topic", 0)])
    assert seen["gate"] == policy.OFFSET_FORCE_REWIND

    settlement = policy.pop(frame)
    # Revoked partition is observable as closed; no seek attempted on it.
    assert ("topic", 0) in settlement.left_closed
    assert settlement.rewound == []


# ---------------------------------------------------------------------------
# Delivery callbacks read the live stack
# ---------------------------------------------------------------------------


def test_delivery_callback_reads_live_stack(producer):
    seen = []

    def on_delivery(err, msg):
        seen.append(policy.effective_view(producer)["emit_admit"])

    # Record is accepted before the deny frame exists. Its delivery report
    # still fires (with a timeout error, no broker here) while the frame is
    # live -- and must read the then-current top.
    producer.produce("topic", value=b"x", on_delivery=on_delivery)
    with policy.scope(producer, emit_admit=policy.EMIT_DENY_NEW, emit_settle_timeout=3.0):
        producer.flush(3.0)
    assert seen == [policy.EMIT_DENY_NEW]


# ---------------------------------------------------------------------------
# Exception safety / nesting / settlement
# ---------------------------------------------------------------------------


def test_exception_pops_frame_cleanly(producer):
    with pytest.raises(RuntimeError):
        with policy.scope(producer, emit_admit=policy.EMIT_DENY_NEW):
            raise RuntimeError("boom")
    assert policy.effective_view(producer)["frame_depth"] == 0
    producer.produce("topic", value=b"ok")


def test_double_pop_is_idempotent(producer):
    frame = policy.push(producer, emit_admit=policy.EMIT_DENY_NEW)
    policy.pop(frame)
    report = policy.pop(frame)
    assert report.errors  # observable note, not a crash
    assert policy.effective_view(producer)["frame_depth"] == 0


def test_emit_settlement_reports_outstanding_without_cancel(producer):
    frame = policy.push(producer, emit_settle_timeout=0.0)
    producer.produce("topic", value=b"a")
    producer.produce("topic", value=b"b")
    settlement = policy.pop(frame)
    info = settlement.emit[id(producer)]
    assert info["admitted"] == 2
    # No broker in unit tests: the records remain observable, never purged.
    assert info["remaining"] >= 1


# ---------------------------------------------------------------------------
# Concurrent scopes do not cross stacks
# ---------------------------------------------------------------------------


def test_concurrent_scopes_per_client_do_not_cross(producer, producer2):
    results = []
    barrier = threading.Barrier(2, timeout=10)

    def scope_a():
        try:
            with policy.scope(producer, emit_admit=policy.EMIT_DENY_NEW):
                barrier.wait()
                try:
                    producer.produce("topic", value=b"a")
                    results.append(("a-p1", "unexpectedly admitted"))
                except PolicyAdmissionError:
                    results.append(("a-p1", "denied"))
                # An unbound third client follows the plain API even here.
                unbound = Producer(dict(PRODUCER_CONF))
                try:
                    unbound.produce("topic", value=b"u")
                    results.append(("a-unbound", "admitted"))
                finally:
                    unbound.poll(0)
                    unbound.flush(0.01)
                barrier.wait()
        except Exception as exc:  # noqa: BLE001
            results.append(("a-error", repr(exc)))

    def scope_b():
        try:
            with policy.scope(producer2, emit_admit=policy.EMIT_ONE_MORE):
                barrier.wait()
                producer2.produce("topic", value=b"b1")
                results.append(("b-p2-first", "admitted"))
                try:
                    producer2.produce("topic", value=b"b2")
                    results.append(("b-p2-second", "unexpectedly admitted"))
                except PolicyAdmissionError:
                    results.append(("b-p2-second", "denied"))
                barrier.wait()
        except Exception as exc:  # noqa: BLE001
            results.append(("b-error", repr(exc)))

    t1 = threading.Thread(target=scope_a)
    t2 = threading.Thread(target=scope_b)
    t1.start()
    t2.start()
    t1.join(15)
    t2.join(15)
    assert not t1.is_alive() and not t2.is_alive()

    got = dict(results)
    assert got.get("a-p1") == "denied"
    assert got.get("a-unbound") == "admitted"
    assert got.get("b-p2-first") == "admitted"
    assert got.get("b-p2-second") == "denied"
    assert not any(k.endswith("error") and not k.startswith("a-p1") for k in got)

    # Both stacks cleanly unwound: plain behavior again.
    producer.produce("topic", value=b"after")
    producer2.produce("topic", value=b"after")


def test_concurrent_fetch_scopes_per_consumer_do_not_cross(consumer, consumer2):
    results = []
    barrier = threading.Barrier(2, timeout=10)

    def scope_a():
        try:
            with policy.scope(consumer, fetch_admit=policy.FETCH_DENY_NEW):
                barrier.wait()
                try:
                    consumer.poll(0)
                    results.append(("a", "admitted"))
                except PolicyAdmissionError:
                    results.append(("a", "denied"))
                barrier.wait()
        except Exception as exc:  # noqa: BLE001
            results.append(("a-error", repr(exc)))

    def scope_b():
        try:
            with policy.scope(consumer2, fetch_admit=policy.FETCH_FORCE_DRAIN):
                barrier.wait()
                # force_drain is not deny: the plain poll still runs.
                results.append(("b", "polled-{}".format(consumer2.poll(0))))
                barrier.wait()
        except Exception as exc:  # noqa: BLE001
            results.append(("b-error", repr(exc)))

    t1 = threading.Thread(target=scope_a)
    t2 = threading.Thread(target=scope_b)
    t1.start()
    t2.start()
    t1.join(15)
    t2.join(15)
    got = dict(results)
    assert got.get("a") == "denied"
    assert got.get("b") == "polled-None"

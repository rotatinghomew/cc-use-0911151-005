from __future__ import annotations

import unittest

from corridor_twin.backtest.event_stream import (
    OUTCOME,
    StreamEvent,
    ReplayView,
    candidates_for,
    derive_border,
    load_event_stream,
    parse_event_time,
    record_to_event,
    route_parts,
)
from corridor_twin.events.logistics_event import ContractError, DomainRecord


def observation(shipment_id, occurred, route="nanning-pingxiang-hanoi", cargo="general",
                revision=1, correction=False, sequence=1, queue=30.0, temperature=20.0,
                eta=105.0, customs="clear"):
    return StreamEvent(
        shipment_id=shipment_id, kind="observation", occurred_at=occurred, revision=revision,
        is_correction=correction, event_sequence=sequence,
        payload={"route_code": route, "cargo_class": cargo, "eta_min": eta,
                 "border_queue": queue, "warehouse_load": 0.5, "temperature": temperature,
                 "customs_status": customs, "capacity_units": 12, "priority": 1,
                 "event_sequence": sequence,
                 "candidate_routes": ["nanning-pingxiang-hanoi", "kunming-hanoi-vientiane"]})


def outcome(shipment_id, occurred, actual=None, state="delivered", sequence=100):
    payload = {"actual_eta_min": actual} if actual is not None else {"note": state}
    return StreamEvent(shipment_id=shipment_id, kind=OUTCOME, occurred_at=occurred,
                       revision=1, event_sequence=sequence, state=state, payload=payload)


class EventStreamContractTests(unittest.TestCase):
    def test_route_and_border_derivation(self):
        self.assertEqual(route_parts("nanning_pingxiang-hanoi"), ("nanning", "pingxiang", "hanoi"))
        self.assertEqual(derive_border("nanning-pingxiang-hanoi"), "pingxiang")
        self.assertEqual(derive_border("a-b"), "b")
        with self.assertRaises(ContractError):
            route_parts("single")

    def test_time_requires_timezone_aware_iso(self):
        parsed = parse_event_time("2026-11-01T00:00:00Z")
        self.assertEqual(parsed.utcoffset().total_seconds(), 0.0)
        with self.assertRaises(ContractError):
            parse_event_time("not-a-time")

    def test_outcome_state_must_be_known(self):
        with self.assertRaises(ContractError):
            StreamEvent("s", OUTCOME, "2026-11-01T00:00:00+00:00", 1, {}, state="vanished")

    def test_correction_revision_must_advance(self):
        events = [
            observation("s", "2026-11-01T00:00:00+00:00", sequence=1),
            observation("s", "2026-11-01T05:00:00+00:00", revision=1, correction=True, sequence=2),
        ]
        with self.assertRaisesRegex(ContractError, "更正事件版本必须高于"):
            load_event_stream({"events": [event.as_dict() for event in events]})

    def test_load_sorts_by_occurred_at(self):
        events = [
            observation("b", "2026-11-01T03:00:00+00:00", sequence=2),
            observation("a", "2026-11-01T01:00:00+00:00", sequence=1),
        ]
        loaded = load_event_stream({"events": [event.as_dict() for event in events]})
        self.assertEqual([event.shipment_id for event in loaded], ["a", "b"])

    def test_record_adapter_preserves_payload(self):
        record = DomainRecord("ship-1", {"route_code": "a-b-c", "eta_min": 90})
        event = record_to_event(record)
        self.assertEqual(event.shipment_id, "ship-1")
        self.assertEqual(event.payload["route_code"], "a-b-c")

    def test_candidates_fallback_to_route_code(self):
        event = StreamEvent("s", "observation", "2026-11-01T00:00:00+00:00", 1,
                            {"route_code": "a-b-c"})
        self.assertEqual(candidates_for(event), ("a-b-c",))


class ReplayViewTests(unittest.TestCase):
    def setUp(self):
        self.events = [
            observation("s1", "2026-11-01T00:00:00+00:00", sequence=1),
            outcome("s1", "2026-11-01T02:00:00+00:00", actual=120.0, sequence=2),
            observation("s2", "2026-11-01T03:00:00+00:00", sequence=3),
            observation("s2", "2026-11-01T09:00:00+00:00", revision=2,
                        correction=True, sequence=4, queue=220.0),
        ]

    def view_at(self, index):
        return ReplayView.at(self.events, self.events[index].time)

    def test_cutoff_hides_future_events_and_corrections(self):
        view = self.view_at(2)
        self.assertIsNone(view.outcome("s2"))
        self.assertEqual(view.latest_observation("s2").revision, 1)
        self.assertNotIn("s2", view.completed_shipments)
        self.assertIn("s2", view.open_shipments)

    def test_correction_becomes_visible_only_after_its_occurred_at(self):
        view = self.view_at(3)
        self.assertEqual(view.latest_observation("s2").revision, 2)
        self.assertTrue(view.latest_observation("s2").is_correction)

    def test_history_keeps_exception_journeys(self):
        events = self.events + [
            outcome("s2", "2026-11-01T10:00:00+00:00", state="unreachable", sequence=5)]
        view = ReplayView.at(events, events[-1].time)
        history = view.history()
        self.assertEqual({row.shipment_id for row in history}, {"s1", "s2"})
        s2 = next(row for row in history if row.shipment_id == "s2")
        self.assertTrue(s2.is_exception)
        self.assertIsNone(s2.actual_minutes)

    def test_knowledge_fingerprint_changes_after_correction(self):
        before = self.view_at(2).knowledge_fingerprint()
        after = self.view_at(3).knowledge_fingerprint()
        self.assertNotEqual(before, after)


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
"""生成滚动回测事件流夹具（确定性、无外部依赖）。

覆盖：
- 两个口岸（pingxiang、hanoi）× 两类货（general、cold）的分段样本；
- 正常送达、不可达、改道、冷链异常四类结局（异常旅程不被剔除）；
- 两条未结束旅程，其中一条的路况事件事后被更正，用于验证失效与重算。
"""
from __future__ import annotations

import json
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "fixtures" / "backtest_events.json"

ROUTES = {
    "nanning-pingxiang-hanoi": 420.0,
    "nanning-pingxiang-haiphong": 470.0,
    "kunming-hanoi-vientiane": 560.0,
    "nanning-hanoi-vientiane": 640.0,
}
CARGO = ("general", "cold")
BORDER_BY_ROUTE = {
    "nanning-pingxiang-hanoi": "pingxiang",
    "nanning-pingxiang-haiphong": "pingxiang",
    "kunming-hanoi-vientiane": "hanoi",
    "nanning-hanoi-vientiane": "hanoi",
}
# 各口岸的系统性延误倾向，使分段误差存在差异。
BORDER_BIAS = {"pingxiang": 1.06, "hanoi": 1.14}


def iso(hour: int) -> str:
    return f"2026-11-01T{hour % 24:02d}:00:00+00:00" if hour < 24 else f"2026-11-{1 + hour // 24:02d}T{hour % 24:02d}:00:00+00:00"


def make_observation(shipment, route, cargo, hour, seq, revision=1, correction=False,
                     border_queue=None, temperature=None):
    base = ROUTES[route] / 4.0
    border_queue = 40 + (seq * 7) % 60 if border_queue is None else border_queue
    temperature = 6.0 + (seq % 5) if cargo == "cold" else 22.0 + (seq % 6) if temperature is None else temperature
    return {
        "shipment_id": shipment, "kind": "observation",
        "occurred_at": iso(hour), "observed_at": iso(hour),
        "revision": revision, "is_correction": correction, "event_sequence": seq,
        "state": "booked", "source": "synthetic-backtest",
        "payload": {
            "route_code": route, "cargo_class": cargo, "vehicle_class": "reefer-truck" if cargo == "cold" else "box-truck",
            "eta_min": round(base, 1), "border_queue": border_queue,
            "warehouse_load": round(0.3 + (seq % 5) * 0.12, 2),
            "temperature": temperature,
            "customs_status": "clear" if seq % 4 else "pending",
            "capacity_units": 12, "priority": 1 + seq % 3, "event_sequence": seq,
            "candidate_routes": list(ROUTES),
        },
    }


def main() -> None:
    events = []
    seq = 0
    hour = 0
    # 16 条已结束旅程：每个口岸×货类 4 条，含 3 条异常。
    shipments = []
    for border in ("pingxiang", "hanoi"):
        routes = [r for r, b in BORDER_BY_ROUTE.items() if b == border]
        for cargo in CARGO:
            for k in range(4):
                route = routes[k % len(routes)]
                shipments.append((f"J-{border[:2]}-{cargo[:2]}-{k:02d}", route, cargo, border))
    exception_plan = {2: "unreachable", 7: "rerouted", 12: "cold_chain_exception"}
    for index, (shipment, route, cargo, border) in enumerate(shipments):
        seq += 1
        hour += 1
        events.append(make_observation(shipment, route, cargo, hour, seq))
        hour += 2
        if index in exception_plan:
            state = exception_plan[index]
            events.append({
                "shipment_id": shipment, "kind": "outcome", "occurred_at": iso(hour),
                "observed_at": iso(hour), "revision": 1, "event_sequence": seq + 100,
                "state": state, "source": "synthetic-backtest",
                "payload": {"route_code": route, "cargo_class": cargo,
                            "actual_eta_min": None, "note": f"journey ended with {state}"},
            })
            continue
        planned = ROUTES[route] / 4.0
        noise = ((seq * 13) % 17 - 8) * 1.5
        actual = round(planned * BORDER_BIAS[border] + noise, 1)
        events.append({
            "shipment_id": shipment, "kind": "outcome", "occurred_at": iso(hour),
            "observed_at": iso(hour), "revision": 1, "event_sequence": seq + 100,
            "state": "delivered", "source": "synthetic-backtest",
            "payload": {"route_code": route, "cargo_class": cargo, "actual_eta_min": actual},
        })

    # 两条未结束旅程。
    open_shipments = [("J-open-01", "nanning-pingxiang-hanoi", "general"),
                      ("J-open-02", "kunming-hanoi-vientiane", "cold")]
    for shipment, route, cargo in open_shipments:
        seq += 1
        hour += 1
        events.append(make_observation(shipment, route, cargo, hour, seq))

    # J-open-02 的路况在更晚时刻被更正：边境排队大幅上修、冷链温度异常。
    correction_hour = hour + 3
    seq += 1
    events.append(make_observation("J-open-02", "kunming-hanoi-vientiane", "cold",
                                   correction_hour, seq, revision=2, correction=True,
                                   border_queue=210, temperature=17.5))

    fixture = {"scenario": "eta-rolling-backtest", "synthetic": True, "events": events}
    OUT.write_text(json.dumps(fixture, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {len(events)} events to {OUT}")


if __name__ == "__main__":
    main()

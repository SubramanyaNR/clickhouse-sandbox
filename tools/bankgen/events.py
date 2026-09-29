"""Phase 2: event emitters, one per stream.

Every emitter takes a customer dict (which carries the persona) and an event
timestamp, and returns (topic_key, key, value). Event time is always set
explicitly by the generator and never inferred from wall clock, so replays are
reproducible and Flink watermarks behave the same every run.
"""

from __future__ import annotations

import random
import uuid
from datetime import timedelta

import config as C


def iso(dt) -> str:
    return dt.isoformat()


def _profile(customer: dict) -> dict:
    return C.PERSONA_PROFILE[customer["persona"]]


# ------------------------------------------------------------------ anomalies
def _in_crash_window(ts) -> bool:
    a = C.ANOMALY_CRASH
    start = C.NOW - timedelta(days=a["start_days_ago"])
    end = C.NOW - timedelta(days=a["end_days_ago"])
    return start <= ts <= end


def _post_policy(ts) -> bool:
    start = C.NOW - timedelta(days=C.ANOMALY_POLICY["start_days_ago"])
    return ts >= start


def _pick_version(rng: random.Random, ts) -> str:
    """Version adoption drifts over time so the crash spike is attributable."""
    days_ago = (C.NOW - ts).days
    if days_ago > 120:
        return rng.choices(C.APP_VERSIONS, weights=[0.70, 0.28, 0.02, 0.0])[0]
    if days_ago > 60:
        return rng.choices(C.APP_VERSIONS, weights=[0.25, 0.55, 0.20, 0.0])[0]
    if days_ago > 25:
        return rng.choices(C.APP_VERSIONS, weights=[0.08, 0.32, 0.60, 0.0])[0]
    return rng.choices(C.APP_VERSIONS, weights=[0.03, 0.15, 0.42, 0.40])[0]


# ------------------------------------------------------------------ emitters
def transaction(rng, customer, account, ts, loans=None):
    ttype = rng.choices(
        C.TXN_TYPES,
        weights=[0.42, 0.18, 0.08, 0.10, 0.12, 0.10]
        if customer["persona"] != "high_volume_upi"
        else [0.72, 0.08, 0.02, 0.06, 0.06, 0.06],
    )[0]

    if ttype == "emi_payment" and loans:
        loan = rng.choice(loans)
        amount = loan["emi_amount"]
        category = "loan_repayment"
        # persona decides whether this payment actually lands
        success = rng.random() < _profile(customer)["emi_on_time_prob"]
    else:
        loan = None
        amount = round(rng.lognormvariate(6.4, 1.25), 2)
        category = rng.choice(C.MERCHANT_CATEGORIES)
        success = rng.random() < 0.97

    return C.TOPICS["transactions"], account["account_id"], {
        "txn_id": f"T-{uuid.uuid4().hex[:14]}",
        "account_id": account["account_id"],
        "customer_id": customer["customer_id"],
        "loan_id": loan["loan_id"] if loan else None,
        "amount": amount,
        "currency": "INR",
        "txn_type": ttype,
        "merchant_category": category,
        "channel": rng.choice(C.CHANNELS),
        "status": "success" if success else rng.choice(["failed", "reversed"]),
        "failure_reason": None if success else rng.choice(
            ["insufficient_funds", "timeout", "bank_declined"]),
        "event_time": iso(ts),
        "event_date": ts.date().isoformat(),
    }


def clickstream_session(rng, customer, ts):
    """Emit a whole session as a list of events, so funnels are coherent."""
    prof = _profile(customer)
    session_id = f"S-{uuid.uuid4().hex[:16]}"
    device = rng.choices(C.DEVICES, weights=[0.68, 0.32])[0]
    version = _pick_version(rng, ts)

    abandon_prob = prof["abandon_prob"]
    # anomaly 2: funnel cliff rides on the same conditions as the crash spike
    if (_in_crash_window(ts) and device == C.ANOMALY_CRASH["device"]
            and version == C.ANOMALY_CRASH["version"]):
        abandon_prob = min(0.95, abandon_prob + C.ANOMALY_FUNNEL_ABANDON_BOOST)

    events = []
    t = ts
    for step, screen in enumerate(C.PAYMENT_FUNNEL):
        events.append((C.TOPICS["clickstream"], session_id, {
            "event_id": f"E-{uuid.uuid4().hex[:14]}",
            "session_id": session_id,
            "customer_id": customer["customer_id"],
            "screen": screen,
            "action": "view" if step < len(C.PAYMENT_FUNNEL) - 1 else "submit",
            "funnel_step": step,
            "device": device,
            "app_version": version,
            "event_time": iso(t),
            "event_date": t.date().isoformat(),
        }))
        t = t + timedelta(seconds=rng.randint(4, 90))
        if rng.random() < abandon_prob:
            break

    return events


def app_log(rng, ts, customer=None):
    a = C.ANOMALY_CRASH
    device = rng.choices(C.DEVICES, weights=[0.68, 0.32])[0]
    version = _pick_version(rng, ts)
    module = rng.choice(C.MODULES)

    base_error_rate = 0.04
    rate = base_error_rate
    # anomaly 1: crash spike, narrowly scoped so it is attributable
    if (_in_crash_window(ts) and device == a["device"]
            and version == a["version"] and module == a["module"]):
        rate = min(0.85, base_error_rate * a["error_rate_multiplier"])

    is_error = rng.random() < rate
    level = rng.choices(["ERROR", "FATAL"], weights=[0.8, 0.2])[0] if is_error \
        else rng.choices(["INFO", "WARN", "DEBUG"], weights=[0.7, 0.2, 0.1])[0]

    error_code = None
    if is_error:
        error_code = ("PAY_NULL_REF_402" if module == "payments"
                      else rng.choice(["NET_TIMEOUT_504", "AUTH_EXPIRED_401",
                                       "RENDER_FAIL_500", "DB_LOCK_409"]))

    return C.TOPICS["app_logs"], f"{version}:{module}", {
        "log_id": f"LG-{uuid.uuid4().hex[:14]}",
        "customer_id": customer["customer_id"] if customer else None,
        "service": "mobile-api",
        "module": module,
        "level": level,
        "error_code": error_code,
        "message": f"{level} in {module}" + (f" ({error_code})" if error_code else ""),
        "app_version": version,
        "device": device,
        "is_crash": bool(is_error and level == "FATAL"),
        "event_time": iso(ts),
        "event_date": ts.date().isoformat(),
    }


def call_center(rng, customer, ts, loans=None):
    persona = customer["persona"]
    if persona == "chronic_dpd":
        intent = rng.choices(C.TICKET_INTENTS,
                             weights=[0.30, 0.22, 0.06, 0.06, 0.14, 0.08, 0.04, 0.10])[0]
    elif persona == "app_abandoner":
        intent = rng.choices(C.TICKET_INTENTS,
                             weights=[0.10, 0.30, 0.32, 0.06, 0.02, 0.04, 0.06, 0.10])[0]
    else:
        intent = rng.choice(C.TICKET_INTENTS)

    opened = ts
    handle_min = rng.randint(3, 55)
    resolved = rng.random() < 0.82

    return C.TOPICS["call_center"], f"TK-{uuid.uuid4().hex[:12]}", {
        "ticket_id": f"TK-{uuid.uuid4().hex[:12]}",
        "customer_id": customer["customer_id"],
        "loan_id": rng.choice(loans)["loan_id"] if loans and rng.random() < 0.6 else None,
        "intent": intent,
        "category": "billing" if "emi" in intent or "payment" in intent else "technical",
        "channel": rng.choices(["phone", "chat", "email"], weights=[0.5, 0.4, 0.1])[0],
        "status": "resolved" if resolved else "open",
        "resolution": rng.choice(["explained", "escalated", "refunded", "pending_docs"])
        if resolved else None,
        "csat": rng.randint(1, 5) if resolved else None,
        "handle_time_min": handle_min,
        "opened_at": iso(opened),
        "closed_at": iso(opened + timedelta(minutes=handle_min)) if resolved else None,
        "event_time": iso(opened),
        "event_date": opened.date().isoformat(),
    }


def loan_application(rng, customer, ts):
    """Emit the full stage progression for one application as a list.

    Stage-by-stage rows are what let the same stream answer both
    'where is my application stuck' and 'where do applicants drop off'.
    """
    product = rng.choice(list(C.LOAN_PRODUCTS))
    app_id = f"AP-{uuid.uuid4().hex[:12]}"

    # approval odds track the customer's risk score
    approve_base = 0.75 if customer["risk_score"] > 700 else \
                   0.45 if customer["risk_score"] > 620 else 0.18

    # anomaly 3: policy change tightens income verification for some products
    forced_reason = None
    if (_post_policy(ts) and product in C.ANOMALY_POLICY["products"]
            and rng.random() < C.ANOMALY_POLICY["share"]):
        approve_base *= 0.25
        forced_reason = C.ANOMALY_POLICY["reason"]

    approved = rng.random() < approve_base
    if approved:
        reached = len(C.APPLICATION_STAGES)
        reason = None
    else:
        reached = rng.randint(2, len(C.APPLICATION_STAGES) - 1)
        reason = forced_reason or rng.choice(list(C.REJECTION_REASONS))

    out = []
    t = ts
    for i, stage in enumerate(C.APPLICATION_STAGES[:reached]):
        terminal = (i == reached - 1)
        decision = ("approved" if approved else "rejected") if terminal else "in_progress"
        out.append((C.TOPICS["applications"], app_id, {
            "application_id": app_id,
            "customer_id": customer["customer_id"],
            "product": product,
            "requested_amount": round(rng.uniform(*C.LOAN_PRODUCTS[product]["principal"]), -3),
            "stage": stage,
            "stage_seq": i,
            "decision": decision,
            "rejection_reason_code": reason if (terminal and not approved) else None,
            "rejection_reason_text": C.REJECTION_REASONS.get(reason)
            if (terminal and not approved) else None,
            "risk_score_at_apply": customer["risk_score"],
            "stage_entered_at": iso(t),
            "event_time": iso(t),
            "event_date": t.date().isoformat(),
        }))
        t = t + timedelta(hours=rng.randint(2, 96))

    return out


# --------------------------------------------------- CDC-ish snapshot emitters
def customer_record(customer):
    return C.TOPICS["customers"], customer["customer_id"], {
        **{k: v for k, v in customer.items() if k != "persona"},
        "event_time": customer["onboarded_at"],
    }


def account_record(account):
    return C.TOPICS["accounts"], account["account_id"], {
        **account, "event_time": account["opened_at"]}


def loan_record(loan):
    return C.TOPICS["loans"], loan["loan_id"], {
        **loan, "event_time": loan["disbursed_at"]}

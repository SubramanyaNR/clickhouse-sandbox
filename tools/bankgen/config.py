"""Tunables for the synthetic banking data generator.

Everything that controls scale, behaviour mix, and planted anomalies lives
here so the generator itself stays readable.
"""

import os
from datetime import datetime, timezone

# ---------------------------------------------------------------- determinism
SEED = 20260908

# ---------------------------------------------------------------------- scale
N_CUSTOMERS = 5_000
EVENTS_PER_SEC = 200          # live mode throughput target
BACKFILL_DAYS = 180           # history depth for trend questions

# Anchor for all event time. Backfill runs BACKFILL_DAYS before this.
NOW = datetime(2026, 9, 8, 0, 0, 0, tzinfo=timezone.utc)

# ------------------------------------------------------------------- personas
# Weights must sum to 1.0. Persona is assigned once, at universe build time,
# and drives payment behaviour, app usage AND support contact together.
PERSONAS = {
    "on_time_payer":   0.60,
    "occasional_late": 0.20,
    "chronic_dpd":     0.10,
    "app_abandoner":   0.07,
    "high_volume_upi": 0.03,
}

# Per-persona knobs consumed by events.py
PERSONA_PROFILE = {
    "on_time_payer": {
        "emi_on_time_prob": 0.98,
        "days_early_max": 3,
        "txn_per_day": (1, 4),
        "sessions_per_week": (2, 5),
        "ticket_per_month_prob": 0.02,
        "abandon_prob": 0.05,
    },
    "occasional_late": {
        "emi_on_time_prob": 0.80,
        "days_early_max": 1,
        "txn_per_day": (1, 5),
        "sessions_per_week": (3, 7),
        "ticket_per_month_prob": 0.10,
        "abandon_prob": 0.15,
    },
    "chronic_dpd": {
        "emi_on_time_prob": 0.35,
        "days_early_max": 0,
        "txn_per_day": (0, 3),
        "sessions_per_week": (4, 12),   # anxious checking
        "ticket_per_month_prob": 0.45,
        "abandon_prob": 0.35,
    },
    "app_abandoner": {
        "emi_on_time_prob": 0.85,
        "days_early_max": 2,
        "txn_per_day": (0, 2),
        "sessions_per_week": (5, 14),
        "ticket_per_month_prob": 0.25,
        "abandon_prob": 0.70,
    },
    "high_volume_upi": {
        "emi_on_time_prob": 0.95,
        "days_early_max": 5,
        "txn_per_day": (8, 30),
        "sessions_per_week": (6, 15),
        "ticket_per_month_prob": 0.05,
        "abandon_prob": 0.08,
    },
}

# ------------------------------------------------------------------- products
LOAN_PRODUCTS = {
    "vehicle":  {"principal": (300_000, 1_500_000), "rate": (8.5, 12.0), "tenure": (36, 84)},
    "personal": {"principal": (50_000, 800_000),    "rate": (11.0, 18.0), "tenure": (12, 60)},
    "gold":     {"principal": (25_000, 500_000),    "rate": (9.0, 14.0),  "tenure": (6, 36)},
    "home":     {"principal": (1_500_000, 9_000_000), "rate": (7.8, 9.5), "tenure": (120, 300)},
}

SEGMENTS = ["retail", "priority", "wealth", "nri"]
CITIES = ["Bengaluru", "Mumbai", "Chennai", "Hyderabad", "Pune",
          "Delhi", "Kolkata", "Ahmedabad", "Kochi", "Jaipur"]

TXN_TYPES = ["upi", "pos", "atm", "emi_payment", "neft", "bill_pay"]
MERCHANT_CATEGORIES = ["fuel", "groceries", "dining", "travel", "utilities",
                       "healthcare", "education", "entertainment", "retail"]

CHANNELS = ["mobile_app", "net_banking", "branch", "atm", "upi_app"]

# --------------------------------------------------------------- applications
APPLICATION_STAGES = [
    "submitted", "document_upload", "kyc_verification",
    "credit_check", "underwriting", "offer_issued", "disbursed",
]
REJECTION_REASONS = {
    "LOW_CREDIT_SCORE":         "Credit score below the minimum threshold for this product",
    "INCOME_VERIFICATION_FAIL": "Declared income could not be verified from submitted documents",
    "HIGH_EXISTING_DEBT":       "Existing obligations exceed the permitted debt-to-income ratio",
    "INCOMPLETE_DOCUMENTS":     "Required documents were not submitted within the window",
    "ADDRESS_MISMATCH":         "Address on documents did not match the application",
}

# -------------------------------------------------------------------- app/ops
APP_VERSIONS = ["4.1.8", "4.2.0", "4.2.1", "4.3.0"]
DEVICES = ["android", "ios"]
MODULES = ["payments", "accounts", "loans", "onboarding", "support", "cards"]

SCREENS = ["home", "accounts", "loan_detail", "emi_payment", "payment_confirm",
           "statements", "profile", "support_chat", "offers"]

# Funnel used for drop-off analysis
PAYMENT_FUNNEL = ["home", "loan_detail", "emi_payment", "payment_confirm"]

TICKET_INTENTS = ["emi_due_query", "payment_failed", "app_crash", "statement_request",
                  "foreclosure_query", "rate_query", "kyc_update", "complaint"]

# ------------------------------------------------------------------ anomalies
# 1. Crash spike: version 4.2.1, android only, payments module
ANOMALY_CRASH = {
    "version": "4.2.1",
    "device": "android",
    "module": "payments",
    "start_days_ago": 21,
    "end_days_ago": 7,
    "error_rate_multiplier": 14.0,
}

# 2. Funnel cliff rides on the same sessions as the crash spike
ANOMALY_FUNNEL_ABANDON_BOOST = 0.55

# 3. Policy change: applications after this date see a spike in one reason
ANOMALY_POLICY = {
    "start_days_ago": 45,
    "reason": "INCOME_VERIFICATION_FAIL",
    "share": 0.55,
    "products": ["personal", "gold"],
}

# ---------------------------------------------------------------------- kafka
TOPICS = {
    "customers":         "bank.dbo.customers",
    "accounts":          "bank.dbo.accounts",
    "loans":             "bank.dbo.loans",
    "transactions":      "bank.dbo.transactions",
    "applications":      "bank.dbo.loan_applications",
    "clickstream":       "bank.app.clickstream",
    "app_logs":          "bank.app.application_logs",
    "call_center":       "bank.call_center_events",
}

BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092")

# Fraction of events emitted deliberately out of order, to exercise watermarks
LATE_EVENT_RATE = 0.03
LATE_EVENT_MAX_LAG_SEC = 240

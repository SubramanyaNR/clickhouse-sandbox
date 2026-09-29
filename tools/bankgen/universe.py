"""Phase 1: build a fixed, persisted universe of customers, accounts and loans.

Nothing in the event stream may reference an entity that does not exist here.
This is the single most important property of the generator: if events point at
IDs that were never created, every downstream Flink join silently produces
nothing and you will debug SQL that was never the problem.

Run this once. It writes universe.json, which the event generator loads.
"""

from __future__ import annotations

import json
import random
from dataclasses import dataclass, asdict, field
from datetime import timedelta

import config as C


# ------------------------------------------------------------------ dataclasses
@dataclass
class Customer:
    customer_id: str
    name: str
    segment: str
    city: str
    kyc_status: str
    risk_score: int
    persona: str
    onboarded_at: str
    email: str
    phone: str


@dataclass
class Account:
    account_id: str
    customer_id: str
    account_type: str
    balance: float
    opened_at: str
    status: str


@dataclass
class Loan:
    loan_id: str
    customer_id: str
    account_id: str
    product: str
    principal: float
    rate: float
    tenure_months: int
    emi_amount: float
    outstanding: float
    emi_day: int
    disbursed_at: str
    status: str
    dpd_bucket: str


@dataclass
class Universe:
    customers: list = field(default_factory=list)
    accounts: list = field(default_factory=list)
    loans: list = field(default_factory=list)


# ----------------------------------------------------------------- name pieces
FIRST = ["Aarav", "Vivaan", "Aditya", "Ananya", "Diya", "Ishaan", "Kavya",
         "Rohan", "Meera", "Arjun", "Sneha", "Karthik", "Priya", "Rahul",
         "Divya", "Nikhil", "Shreya", "Varun", "Anjali", "Suresh", "Lakshmi",
         "Manoj", "Pooja", "Rajesh", "Sunita", "Vikram", "Neha", "Girish"]
LAST = ["Sharma", "Verma", "Reddy", "Nair", "Iyer", "Rao", "Kulkarni", "Shetty",
        "Bhat", "Menon", "Gupta", "Joshi", "Desai", "Pillai", "Hegde", "Kamath"]


def _pick_persona(rng: random.Random) -> str:
    r = rng.random()
    cum = 0.0
    for persona, weight in C.PERSONAS.items():
        cum += weight
        if r <= cum:
            return persona
    return "on_time_payer"


def _emi(principal: float, annual_rate: float, months: int) -> float:
    """Standard reducing-balance EMI."""
    r = annual_rate / 12 / 100
    if r == 0:
        return round(principal / months, 2)
    factor = (1 + r) ** months
    return round(principal * r * factor / (factor - 1), 2)


def build(seed: int = C.SEED, n_customers: int = C.N_CUSTOMERS) -> Universe:
    rng = random.Random(seed)
    uni = Universe()

    for i in range(n_customers):
        cid = f"C-{100000 + i}"
        persona = _pick_persona(rng)

        # risk score correlates with persona so the data is internally coherent
        if persona == "chronic_dpd":
            risk = rng.randint(280, 620)
        elif persona == "occasional_late":
            risk = rng.randint(600, 730)
        else:
            risk = rng.randint(690, 850)

        onboarded = C.NOW - timedelta(days=rng.randint(120, 2400))
        first, last = rng.choice(FIRST), rng.choice(LAST)

        uni.customers.append(Customer(
            customer_id=cid,
            name=f"{first} {last}",
            segment=rng.choices(C.SEGMENTS, weights=[0.70, 0.18, 0.07, 0.05])[0],
            city=rng.choice(C.CITIES),
            kyc_status=rng.choices(["verified", "pending", "expired"],
                                   weights=[0.92, 0.05, 0.03])[0],
            risk_score=risk,
            persona=persona,
            onboarded_at=onboarded.isoformat(),
            email=f"{first.lower()}.{last.lower()}{i}@example.com",
            phone=f"+91{rng.randint(7000000000, 9999999999)}",
        ))

        # ---- accounts: every customer has a savings account, some have more
        n_acc = rng.choices([1, 2, 3], weights=[0.65, 0.28, 0.07])[0]
        cust_accounts = []
        for a in range(n_acc):
            aid = f"A-{cid[2:]}-{a}"
            acc = Account(
                account_id=aid,
                customer_id=cid,
                account_type="savings" if a == 0 else rng.choice(["current", "salary"]),
                balance=round(rng.lognormvariate(10.5, 1.1), 2),
                opened_at=onboarded.isoformat(),
                status="active",
            )
            cust_accounts.append(acc)
            uni.accounts.append(acc)

        # ---- loans: not everyone has one
        n_loans = rng.choices([0, 1, 2], weights=[0.35, 0.52, 0.13])[0]
        for l in range(n_loans):
            product = rng.choices(list(C.LOAN_PRODUCTS),
                                  weights=[0.30, 0.38, 0.22, 0.10])[0]
            spec = C.LOAN_PRODUCTS[product]
            principal = round(rng.uniform(*spec["principal"]), -3)
            rate = round(rng.uniform(*spec["rate"]), 2)
            tenure = rng.randint(*spec["tenure"])
            disbursed = C.NOW - timedelta(days=rng.randint(60, min(1800, tenure * 30)))
            months_elapsed = max(1, (C.NOW - disbursed).days // 30)
            emi = _emi(principal, rate, tenure)

            paid_ratio = min(0.95, months_elapsed / tenure)
            outstanding = round(principal * (1 - paid_ratio * rng.uniform(0.75, 0.95)), 2)

            if persona == "chronic_dpd":
                dpd = rng.choices(["0", "1-30", "31-60", "61-90", "90+"],
                                  weights=[0.15, 0.25, 0.25, 0.20, 0.15])[0]
            elif persona == "occasional_late":
                dpd = rng.choices(["0", "1-30", "31-60"], weights=[0.65, 0.30, 0.05])[0]
            else:
                dpd = rng.choices(["0", "1-30"], weights=[0.96, 0.04])[0]

            uni.loans.append(Loan(
                loan_id=f"L-{cid[2:]}-{l}",
                customer_id=cid,
                account_id=cust_accounts[0].account_id,
                product=product,
                principal=principal,
                rate=rate,
                tenure_months=tenure,
                emi_amount=emi,
                outstanding=outstanding,
                emi_day=rng.randint(1, 10),
                disbursed_at=disbursed.isoformat(),
                status="closed" if outstanding <= 0 else "active",
                dpd_bucket=dpd,
            ))

    return uni


def save(uni: Universe, path: str = "universe.json") -> None:
    payload = {
        "customers": [asdict(c) for c in uni.customers],
        "accounts": [asdict(a) for a in uni.accounts],
        "loans": [asdict(l) for l in uni.loans],
    }
    with open(path, "w") as f:
        json.dump(payload, f, indent=1)


def load(path: str = "universe.json") -> dict:
    with open(path) as f:
        return json.load(f)


if __name__ == "__main__":
    u = build()
    save(u)
    print(f"customers : {len(u.customers):,}")
    print(f"accounts  : {len(u.accounts):,}")
    print(f"loans     : {len(u.loans):,}")
    personas = {}
    for c in u.customers:
        personas[c.persona] = personas.get(c.persona, 0) + 1
    print("persona mix:", personas)

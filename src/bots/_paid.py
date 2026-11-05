"""Bots that spend money per lookup, and the one switch that runs them.

The accounts (BatchData, Enformion, Twilio) are unfunded on purpose until
Patrick is licensed and under a brokerage. With the switch off the runner
parks these bots (status parked_paid_data) and the alert report lists them
as parked instead of failing every night. Day one: fund the accounts and set
FALCO_PAID_DATA_ENABLED=1.
"""
from __future__ import annotations

import os

PAID_BOTS = frozenset({
    "middle_tn_skiptrace",      # BatchData skip trace
    "enformion_skip_trace",     # Enformion contact enrich
    "middle_tn_twilio_lookup",  # Twilio Lookup line type
    "batchdata_skip_trace",     # BatchData skip trace (legacy)
    "middle_tn_dial_probe",     # Twilio
})


def paid_data_enabled() -> bool:
    return os.environ.get("FALCO_PAID_DATA_ENABLED", "").strip() == "1"

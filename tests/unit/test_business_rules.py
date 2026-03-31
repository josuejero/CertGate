from datetime import datetime, timezone

import pandas as pd

from certgate.rules.business import (
    check_activity_object_references,
    check_inactive_owner_open_opportunities,
    check_recent_touches,
    check_same_email_multiple_accounts,
    check_stale_opportunity_stage,
)


def _owners_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "owner_id": ["O-1", "O-2"],
            "owner_name": ["Owner One", "Owner Two"],
            "owner_email": ["one@example.com", "two@example.com"],
            "team": ["AE", "SDR"],
            "manager": ["Mgr", "Mgr"],
            "is_active": [True, False],
        }
    )


def test_same_email_multiple_accounts_detects_conflict():
    leads_df = pd.DataFrame(
        {
            "lead_id": ["L-1", "L-2"],
            "email": ["same@example.com", "same@example.com"],
            "account_id": ["A-1", "A-2"],
            "owner_id": ["O-1", "O-2"],
        }
    )
    outcome = check_same_email_multiple_accounts(leads_df, _owners_df())
    assert not outcome.passed
    assert outcome.severity == "critical"
    assert outcome.details["affected_count"] == 2


def test_activity_reference_detects_missing_object():
    activities_df = pd.DataFrame(
        {
            "activity_id": ["ACT-1"],
            "object_type": ["opportunity"],
            "object_id": ["OP-404"],
            "activity_type": ["email"],
            "activity_at": ["2026-03-01T00:00:00Z"],
            "owner_id": ["O-1"],
            "outcome": ["Sent"],
        }
    )
    outcome = check_activity_object_references(
        activities_df=activities_df,
        leads_df=pd.DataFrame({"lead_id": ["L-1"]}),
        accounts_df=pd.DataFrame({"account_id": ["A-1"]}),
        opportunities_df=pd.DataFrame({"opportunity_id": ["OP-1"]}),
        owners_df=_owners_df(),
    )
    assert not outcome.passed
    assert outcome.severity == "critical"


def test_inactive_owner_on_open_opportunity_blocks():
    opportunities_df = pd.DataFrame(
        {
            "opportunity_id": ["OP-1"],
            "owner_id": ["O-2"],
            "stage": ["Negotiation"],
        }
    )
    outcome = check_inactive_owner_open_opportunities(opportunities_df, _owners_df())
    assert not outcome.passed
    assert outcome.severity == "critical"


def test_recent_touch_and_stale_stage_use_explicit_reference_time():
    reference_time = datetime(2026, 3, 30, tzinfo=timezone.utc)
    leads_df = pd.DataFrame(
        {
            "lead_id": ["L-1"],
            "lifecycle_stage": ["SQL"],
            "last_activity_at": [pd.Timestamp("2026-03-01T00:00:00Z")],
            "owner_id": ["O-1"],
        }
    )
    opportunities_df = pd.DataFrame(
        {
            "opportunity_id": ["OP-1"],
            "stage": ["Proposal"],
            "last_stage_change_at": [pd.Timestamp("2026-03-01T00:00:00Z")],
            "owner_id": ["O-1"],
        }
    )
    activities_df = pd.DataFrame(
        {
            "activity_id": ["ACT-1"],
            "object_type": ["opportunity"],
            "object_id": ["OP-1"],
            "activity_type": ["email"],
            "activity_at": [pd.Timestamp("2026-03-05T00:00:00Z")],
            "owner_id": ["O-1"],
            "outcome": ["Sent"],
        }
    )
    recent_touch_outcomes = check_recent_touches(
        leads_df=leads_df,
        opportunities_df=opportunities_df,
        activities_df=activities_df,
        owners_df=_owners_df(),
        reference_time=reference_time,
        recent_touch_days=14,
    )
    stale_stage = check_stale_opportunity_stage(
        opportunities_df=opportunities_df,
        owners_df=_owners_df(),
        reference_time=reference_time,
        stale_stage_days=21,
    )
    assert all(not outcome.passed for outcome in recent_touch_outcomes)
    assert not stale_stage.passed
    assert stale_stage.severity == "warning"

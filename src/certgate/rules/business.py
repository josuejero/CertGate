"""Pandas-based CRM business logic validations."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd

from ..ingest.loaders import LoadedTable
from .schema import RuleOutcome, Severity, build_rule_outcome

LEAD_LIFECYCLE_ORDER = (
    "Subscriber",
    "Lead",
    "MQL",
    "SQL",
    "Opportunity",
    "Customer",
)
OPPORTUNITY_STAGE_ORDER = (
    "Discovery",
    "Qualification",
    "Proposal",
    "Negotiation",
    "Closed Won",
    "Closed Lost",
)
CLOSED_STAGES = {"Closed Won", "Closed Lost"}
ACTIVE_LEAD_STAGES = {"MQL", "SQL", "Opportunity"}
ACTIVITY_OBJECT_MAP = {
    "lead": "leads",
    "account": "accounts",
    "opportunity": "opportunities",
}


def _coerce_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    ts = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(ts):
        return None
    if isinstance(ts, pd.Timestamp):
        ts = ts.to_pydatetime()
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts


def _bundle_reference_time(tables: Mapping[str, LoadedTable]) -> datetime:
    for table in tables.values():
        candidate = _coerce_timestamp(table.metadata.get("bundle_reference_time"))
        if candidate:
            return candidate
    return datetime.now(timezone.utc)


def _serialize_value(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if pd.isna(value) if not isinstance(value, (list, dict, tuple, set)) else False:
        return None
    if hasattr(value, "item"):
        try:
            return value.item()
        except (TypeError, ValueError):
            return str(value)
    return value


def _sample_records(
    df: pd.DataFrame,
    columns: Sequence[str],
    limit: int = 5,
) -> list[dict[str, Any]]:
    if df.empty:
        return []
    sample = df.loc[:, [column for column in columns if column in df.columns]].head(limit)
    return [
        {column: _serialize_value(value) for column, value in row.items()}
        for row in sample.to_dict(orient="records")
    ]


def _owner_lookup(owners_df: pd.DataFrame) -> dict[str, str]:
    if owners_df.empty:
        return {}
    lookup = owners_df.set_index("owner_id")["owner_name"].fillna("").to_dict()
    return {str(key): str(value) or str(key) for key, value in lookup.items()}


def _owner_issue_counts(
    df: pd.DataFrame,
    owners_df: pd.DataFrame,
    owner_column: str = "owner_id",
) -> dict[str, int]:
    if df.empty or owner_column not in df.columns:
        return {}
    owner_lookup = _owner_lookup(owners_df)
    counts = Counter(df[owner_column].dropna().astype("string"))
    return {
        owner_lookup.get(str(owner_id), str(owner_id)): int(count)
        for owner_id, count in counts.items()
    }


def _stage_issue_counts(df: pd.DataFrame, stage_column: str = "stage") -> dict[str, int]:
    if df.empty or stage_column not in df.columns:
        return {}
    counts = Counter(df[stage_column].dropna().astype("string"))
    return {str(stage): int(count) for stage, count in counts.items()}


def _issue_details(
    df: pd.DataFrame,
    owners_df: pd.DataFrame,
    sample_columns: Sequence[str],
    affected_count: int | None = None,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    details: dict[str, Any] = {
        "affected_count": int(affected_count if affected_count is not None else len(df)),
        "sample_records": _sample_records(df, sample_columns),
        "owner_issue_counts": _owner_issue_counts(df, owners_df),
        "stage_issue_counts": _stage_issue_counts(df),
    }
    if extra:
        details.update(extra)
    return details


def _foreign_key_reference(
    child_df: pd.DataFrame,
    parent_df: pd.DataFrame,
    child_column: str,
    parent_column: str,
    rule_id: str,
    description: str,
    severity: Severity,
    owners_df: pd.DataFrame,
    sample_columns: Sequence[str],
) -> RuleOutcome:
    populated = child_df[child_column].notna()
    parent_values = set(parent_df[parent_column].dropna().astype("string"))
    invalid_mask = populated & ~child_df[child_column].astype("string").isin(parent_values)
    failing = child_df.loc[invalid_mask]
    passed = failing.empty
    return build_rule_outcome(
        rule_id=rule_id,
        description=description if passed else f"{description} Missing references detected.",
        passed=passed,
        severity=severity if not passed else "info",
        details=_issue_details(
            failing,
            owners_df=owners_df,
            sample_columns=sample_columns,
            extra={
                "missing_values": sorted(failing[child_column].dropna().astype("string").unique().tolist()),
            },
        ),
    )


def check_allowed_value_sets(
    leads_df: pd.DataFrame,
    opportunities_df: pd.DataFrame,
    owners_df: pd.DataFrame,
) -> list[RuleOutcome]:
    outcomes: list[RuleOutcome] = []
    invalid_leads = leads_df.loc[~leads_df["lifecycle_stage"].isin(LEAD_LIFECYCLE_ORDER)]
    outcomes.append(
        build_rule_outcome(
            rule_id="crm-lifecycle-values",
            description="Lead lifecycle values stay within the supported CRM order."
            if invalid_leads.empty
            else "Lead lifecycle values contain unsupported stages.",
            passed=invalid_leads.empty,
            severity="critical" if not invalid_leads.empty else "info",
            details=_issue_details(
                invalid_leads,
                owners_df=owners_df,
                sample_columns=("lead_id", "lifecycle_stage", "owner_id"),
                extra={"allowed_values": list(LEAD_LIFECYCLE_ORDER)},
            ),
        )
    )
    invalid_opportunities = opportunities_df.loc[
        ~opportunities_df["stage"].isin(OPPORTUNITY_STAGE_ORDER)
    ]
    outcomes.append(
        build_rule_outcome(
            rule_id="crm-opportunity-stage-values",
            description="Opportunity stages stay within the supported CRM order."
            if invalid_opportunities.empty
            else "Opportunity stages contain unsupported values.",
            passed=invalid_opportunities.empty,
            severity="critical" if not invalid_opportunities.empty else "info",
            details=_issue_details(
                invalid_opportunities,
                owners_df=owners_df,
                sample_columns=("opportunity_id", "stage", "owner_id"),
                extra={"allowed_values": list(OPPORTUNITY_STAGE_ORDER)},
            ),
        )
    )
    return outcomes


def check_owner_foreign_keys(
    leads_df: pd.DataFrame,
    accounts_df: pd.DataFrame,
    opportunities_df: pd.DataFrame,
    activities_df: pd.DataFrame,
    owners_df: pd.DataFrame,
) -> list[RuleOutcome]:
    return [
        _foreign_key_reference(
            child_df=table_df,
            parent_df=owners_df,
            child_column="owner_id",
            parent_column="owner_id",
            rule_id=rule_id,
            description=description,
            severity="critical",
            owners_df=owners_df,
            sample_columns=sample_columns,
        )
        for table_df, rule_id, description, sample_columns in (
            (
                leads_df,
                "crm-lead-owner-fk",
                "Every lead owner_id resolves to an active owner record.",
                ("lead_id", "owner_id", "email"),
            ),
            (
                accounts_df,
                "crm-account-owner-fk",
                "Every account owner_id resolves to an owner record.",
                ("account_id", "owner_id", "company_domain"),
            ),
            (
                opportunities_df,
                "crm-opportunity-owner-fk",
                "Every opportunity owner_id resolves to an owner record.",
                ("opportunity_id", "owner_id", "stage"),
            ),
            (
                activities_df,
                "crm-activity-owner-fk",
                "Every activity owner_id resolves to an owner record.",
                ("activity_id", "owner_id", "object_type", "object_id"),
            ),
        )
    ]


def check_account_foreign_keys(
    leads_df: pd.DataFrame,
    accounts_df: pd.DataFrame,
    opportunities_df: pd.DataFrame,
    owners_df: pd.DataFrame,
) -> list[RuleOutcome]:
    return [
        _foreign_key_reference(
            child_df=opportunities_df,
            parent_df=accounts_df,
            child_column="account_id",
            parent_column="account_id",
            rule_id="crm-opportunity-account-fk",
            description="Every opportunity account_id resolves to an account record.",
            severity="critical",
            owners_df=owners_df,
            sample_columns=("opportunity_id", "account_id", "stage", "owner_id"),
        ),
        _foreign_key_reference(
            child_df=leads_df.loc[leads_df["account_id"].notna()],
            parent_df=accounts_df,
            child_column="account_id",
            parent_column="account_id",
            rule_id="crm-lead-account-fk",
            description="Every linked lead account_id resolves to an account record.",
            severity="critical",
            owners_df=owners_df,
            sample_columns=("lead_id", "account_id", "email", "owner_id"),
        ),
    ]


def check_activity_object_references(
    activities_df: pd.DataFrame,
    leads_df: pd.DataFrame,
    accounts_df: pd.DataFrame,
    opportunities_df: pd.DataFrame,
    owners_df: pd.DataFrame,
) -> RuleOutcome:
    normalized_types = activities_df["object_type"].fillna("").astype("string").str.lower()
    invalid_type_mask = ~normalized_types.isin(ACTIVITY_OBJECT_MAP)

    valid_type_rows = activities_df.loc[~invalid_type_mask].copy()
    valid_type_rows["object_type_normalized"] = normalized_types.loc[~invalid_type_mask]
    missing_rows = []
    for object_type, table_name in ACTIVITY_OBJECT_MAP.items():
        subset = valid_type_rows.loc[valid_type_rows["object_type_normalized"] == object_type]
        if subset.empty:
            continue
        table_key = {
            "lead": "lead_id",
            "account": "account_id",
            "opportunity": "opportunity_id",
        }[object_type]
        existing_ids = set(
            {
                str(value)
                for value in {
                    *leads_df["lead_id"].dropna().astype("string"),
                    *accounts_df["account_id"].dropna().astype("string"),
                    *opportunities_df["opportunity_id"].dropna().astype("string"),
                }
            }
        )
        if table_name == "leads":
            existing_ids = set(leads_df[table_key].dropna().astype("string"))
        elif table_name == "accounts":
            existing_ids = set(accounts_df[table_key].dropna().astype("string"))
        elif table_name == "opportunities":
            existing_ids = set(opportunities_df[table_key].dropna().astype("string"))
        invalid_rows = subset.loc[~subset["object_id"].astype("string").isin(existing_ids)]
        if not invalid_rows.empty:
            missing_rows.append(invalid_rows)

    failing = pd.concat([activities_df.loc[invalid_type_mask], *missing_rows], ignore_index=True) if invalid_type_mask.any() or missing_rows else pd.DataFrame(columns=activities_df.columns)
    passed = failing.empty
    return build_rule_outcome(
        rule_id="crm-activity-object-reference",
        description="Every activity references a valid CRM object type and object_id."
        if passed
        else "Activities reference missing or unsupported CRM objects.",
        passed=passed,
        severity="critical" if not passed else "info",
        details=_issue_details(
            failing,
            owners_df=owners_df,
            sample_columns=("activity_id", "object_type", "object_id", "owner_id"),
            extra={
                "allowed_object_types": sorted(ACTIVITY_OBJECT_MAP),
            },
        ),
    )


def check_same_email_multiple_accounts(
    leads_df: pd.DataFrame,
    owners_df: pd.DataFrame,
) -> RuleOutcome:
    populated = leads_df.loc[leads_df["email"].notna() & leads_df["account_id"].notna()]
    multi_account_emails = (
        populated.groupby("email")["account_id"].nunique().loc[lambda series: series > 1]
    )
    failing = populated.loc[populated["email"].isin(multi_account_emails.index)]
    return build_rule_outcome(
        rule_id="crm-email-multiple-accounts",
        description="Lead emails map to a single account."
        if failing.empty
        else "Lead emails are linked to multiple accounts.",
        passed=failing.empty,
        severity="critical" if not failing.empty else "info",
        details=_issue_details(
            failing,
            owners_df=owners_df,
            sample_columns=("lead_id", "email", "account_id", "owner_id"),
        ),
    )


def check_same_domain_multiple_reps(
    leads_df: pd.DataFrame,
    owners_df: pd.DataFrame,
) -> RuleOutcome:
    populated = leads_df.loc[leads_df["company_domain"].notna() & leads_df["owner_id"].notna()]
    domain_owner_counts = (
        populated.groupby("company_domain")["owner_id"].nunique().loc[lambda series: series > 1]
    )
    failing = populated.loc[populated["company_domain"].isin(domain_owner_counts.index)]
    return build_rule_outcome(
        rule_id="crm-domain-multiple-reps",
        description="Company domains are routed to a single rep."
        if failing.empty
        else "Company domains are split across multiple reps.",
        passed=failing.empty,
        severity="warning" if not failing.empty else "info",
        details=_issue_details(
            failing,
            owners_df=owners_df,
            sample_columns=("lead_id", "company_domain", "owner_id"),
        ),
    )


def check_closed_won_amounts(
    opportunities_df: pd.DataFrame,
    owners_df: pd.DataFrame,
) -> RuleOutcome:
    invalid = opportunities_df.loc[
        (opportunities_df["stage"] == "Closed Won")
        & (opportunities_df["amount"].isna() | (opportunities_df["amount"] <= 0))
    ]
    return build_rule_outcome(
        rule_id="crm-closed-won-amount",
        description="Closed Won opportunities carry a positive amount."
        if invalid.empty
        else "Closed Won opportunities have missing or non-positive amounts.",
        passed=invalid.empty,
        severity="critical" if not invalid.empty else "info",
        details=_issue_details(
            invalid,
            owners_df=owners_df,
            sample_columns=("opportunity_id", "stage", "amount", "owner_id"),
        ),
    )


def check_close_date_chronology(
    opportunities_df: pd.DataFrame,
    owners_df: pd.DataFrame,
) -> RuleOutcome:
    invalid = opportunities_df.loc[
        opportunities_df["close_date"].notna()
        & opportunities_df["created_at"].notna()
        & (opportunities_df["close_date"] < opportunities_df["created_at"])
    ]
    return build_rule_outcome(
        rule_id="crm-close-date-chronology",
        description="Opportunity close dates occur on or after created_at."
        if invalid.empty
        else "Opportunity close dates precede created_at.",
        passed=invalid.empty,
        severity="critical" if not invalid.empty else "info",
        details=_issue_details(
            invalid,
            owners_df=owners_df,
            sample_columns=("opportunity_id", "created_at", "close_date", "owner_id"),
        ),
    )


def check_inactive_owner_open_opportunities(
    opportunities_df: pd.DataFrame,
    owners_df: pd.DataFrame,
) -> RuleOutcome:
    owner_status = owners_df.set_index("owner_id")["is_active"].to_dict()
    open_opportunities = opportunities_df.loc[~opportunities_df["stage"].isin(CLOSED_STAGES)].copy()
    open_opportunities["owner_is_active"] = open_opportunities["owner_id"].map(owner_status)
    invalid = open_opportunities.loc[open_opportunities["owner_is_active"] == False]  # noqa: E712
    return build_rule_outcome(
        rule_id="crm-inactive-owner-open-opportunity",
        description="Open opportunities are assigned to active owners."
        if invalid.empty
        else "Open opportunities are assigned to inactive owners.",
        passed=invalid.empty,
        severity="critical" if not invalid.empty else "info",
        details=_issue_details(
            invalid,
            owners_df=owners_df,
            sample_columns=("opportunity_id", "owner_id", "stage"),
        ),
    )


def check_missing_employee_count_for_enterprise(
    accounts_df: pd.DataFrame,
    owners_df: pd.DataFrame,
) -> RuleOutcome:
    invalid = accounts_df.loc[
        accounts_df["segment"].eq("Enterprise") & accounts_df["employee_count"].isna()
    ]
    return build_rule_outcome(
        rule_id="crm-enterprise-employee-count",
        description="Enterprise accounts include employee_count."
        if invalid.empty
        else "Enterprise accounts are missing employee_count.",
        passed=invalid.empty,
        severity="warning" if not invalid.empty else "info",
        details=_issue_details(
            invalid,
            owners_df=owners_df,
            sample_columns=("account_id", "account_name", "segment", "owner_id"),
        ),
    )


def check_missing_next_step_on_open_opportunities(
    opportunities_df: pd.DataFrame,
    owners_df: pd.DataFrame,
) -> RuleOutcome:
    next_step = opportunities_df["next_step"].fillna("").astype("string").str.strip()
    invalid = opportunities_df.loc[
        ~opportunities_df["stage"].isin(CLOSED_STAGES) & next_step.eq("")
    ]
    return build_rule_outcome(
        rule_id="crm-open-opportunity-next-step",
        description="Open opportunities contain a next_step."
        if invalid.empty
        else "Open opportunities are missing next_step.",
        passed=invalid.empty,
        severity="warning" if not invalid.empty else "info",
        details=_issue_details(
            invalid,
            owners_df=owners_df,
            sample_columns=("opportunity_id", "stage", "owner_id", "next_step"),
        ),
    )


def check_recent_touches(
    leads_df: pd.DataFrame,
    opportunities_df: pd.DataFrame,
    activities_df: pd.DataFrame,
    owners_df: pd.DataFrame,
    reference_time: datetime,
    recent_touch_days: int = 14,
) -> list[RuleOutcome]:
    threshold = reference_time - timedelta(days=recent_touch_days)

    stale_leads = leads_df.loc[
        leads_df["lifecycle_stage"].isin(ACTIVE_LEAD_STAGES)
        & (
            leads_df["last_activity_at"].isna()
            | (leads_df["last_activity_at"] < threshold)
        )
    ]

    opportunity_activities = activities_df.loc[
        activities_df["object_type"].fillna("").astype("string").str.lower().eq("opportunity")
    ]
    latest_activities = (
        opportunity_activities.groupby("object_id")["activity_at"].max().to_dict()
        if not opportunity_activities.empty
        else {}
    )
    open_opportunities = opportunities_df.loc[
        ~opportunities_df["stage"].isin(CLOSED_STAGES)
    ].copy()
    open_opportunities["latest_activity_at"] = open_opportunities["opportunity_id"].map(latest_activities)
    open_opportunities["latest_activity_at"] = pd.to_datetime(
        open_opportunities["latest_activity_at"], utc=True, errors="coerce"
    )
    stale_opportunities = open_opportunities.loc[
        open_opportunities["latest_activity_at"].isna()
        | (open_opportunities["latest_activity_at"] < threshold)
    ]

    return [
        build_rule_outcome(
            rule_id="crm-lead-recent-touch",
            description="Active MQL/SQL/Opportunity leads have recent touches."
            if stale_leads.empty
            else "Active MQL/SQL/Opportunity leads are missing recent touches.",
            passed=stale_leads.empty,
            severity="warning" if not stale_leads.empty else "info",
            details=_issue_details(
                stale_leads,
                owners_df=owners_df,
                sample_columns=("lead_id", "lifecycle_stage", "last_activity_at", "owner_id"),
                extra={"threshold": threshold.isoformat()},
            ),
        ),
        build_rule_outcome(
            rule_id="crm-opportunity-recent-touch",
            description="Open opportunities have recent touches."
            if stale_opportunities.empty
            else "Open opportunities are missing recent touches.",
            passed=stale_opportunities.empty,
            severity="warning" if not stale_opportunities.empty else "info",
            details=_issue_details(
                stale_opportunities,
                owners_df=owners_df,
                sample_columns=("opportunity_id", "stage", "latest_activity_at", "owner_id"),
                extra={"threshold": threshold.isoformat()},
            ),
        ),
    ]


def check_stale_opportunity_stage(
    opportunities_df: pd.DataFrame,
    owners_df: pd.DataFrame,
    reference_time: datetime,
    stale_stage_days: int = 21,
) -> RuleOutcome:
    threshold = reference_time - timedelta(days=stale_stage_days)
    invalid = opportunities_df.loc[
        ~opportunities_df["stage"].isin(CLOSED_STAGES)
        & (
            opportunities_df["last_stage_change_at"].isna()
            | (opportunities_df["last_stage_change_at"] < threshold)
        )
    ]
    return build_rule_outcome(
        rule_id="crm-stale-opportunity-stage",
        description="Open opportunity stages are updated within the stale-stage window."
        if invalid.empty
        else "Open opportunities have stale stage ages.",
        passed=invalid.empty,
        severity="warning" if not invalid.empty else "info",
        details=_issue_details(
            invalid,
            owners_df=owners_df,
            sample_columns=("opportunity_id", "stage", "last_stage_change_at", "owner_id"),
            extra={"threshold": threshold.isoformat()},
        ),
    )


def check_lead_account_domain_mismatch(
    leads_df: pd.DataFrame,
    accounts_df: pd.DataFrame,
    owners_df: pd.DataFrame,
) -> RuleOutcome:
    linked = leads_df.loc[leads_df["account_id"].notna()].merge(
        accounts_df[["account_id", "company_domain"]],
        on="account_id",
        how="left",
        suffixes=("_lead", "_account"),
    )
    invalid = linked.loc[
        linked["company_domain_account"].notna()
        & linked["company_domain_lead"].notna()
        & (linked["company_domain_lead"] != linked["company_domain_account"])
    ]
    return build_rule_outcome(
        rule_id="crm-lead-account-domain-mismatch",
        description="Linked lead and account domains agree."
        if invalid.empty
        else "Linked lead and account domains disagree.",
        passed=invalid.empty,
        severity="warning" if not invalid.empty else "info",
        details=_issue_details(
            invalid,
            owners_df=owners_df,
            sample_columns=("lead_id", "account_id", "company_domain_lead", "company_domain_account", "owner_id"),
        ),
    )


def default_crm_business_rules(
    tables: Mapping[str, LoadedTable],
    recent_touch_days: int = 14,
    stale_stage_days: int = 21,
    reference_time: datetime | None = None,
) -> list[RuleOutcome]:
    leads_df = tables["leads"].df
    accounts_df = tables["accounts"].df
    opportunities_df = tables["opportunities"].df
    activities_df = tables["activities"].df
    owners_df = tables["owners"].df
    bundle_time = reference_time or _bundle_reference_time(tables)

    outcomes: list[RuleOutcome] = []
    outcomes.extend(check_allowed_value_sets(leads_df, opportunities_df, owners_df))
    outcomes.extend(
        check_owner_foreign_keys(
            leads_df=leads_df,
            accounts_df=accounts_df,
            opportunities_df=opportunities_df,
            activities_df=activities_df,
            owners_df=owners_df,
        )
    )
    outcomes.extend(
        check_account_foreign_keys(
            leads_df=leads_df,
            accounts_df=accounts_df,
            opportunities_df=opportunities_df,
            owners_df=owners_df,
        )
    )
    outcomes.append(
        check_activity_object_references(
            activities_df=activities_df,
            leads_df=leads_df,
            accounts_df=accounts_df,
            opportunities_df=opportunities_df,
            owners_df=owners_df,
        )
    )
    outcomes.append(check_same_email_multiple_accounts(leads_df, owners_df))
    outcomes.append(check_same_domain_multiple_reps(leads_df, owners_df))
    outcomes.append(check_closed_won_amounts(opportunities_df, owners_df))
    outcomes.append(check_close_date_chronology(opportunities_df, owners_df))
    outcomes.append(check_inactive_owner_open_opportunities(opportunities_df, owners_df))
    outcomes.append(check_missing_employee_count_for_enterprise(accounts_df, owners_df))
    outcomes.append(check_missing_next_step_on_open_opportunities(opportunities_df, owners_df))
    outcomes.extend(
        check_recent_touches(
            leads_df=leads_df,
            opportunities_df=opportunities_df,
            activities_df=activities_df,
            owners_df=owners_df,
            reference_time=bundle_time,
            recent_touch_days=recent_touch_days,
        )
    )
    outcomes.append(
        check_stale_opportunity_stage(
            opportunities_df=opportunities_df,
            owners_df=owners_df,
            reference_time=bundle_time,
            stale_stage_days=stale_stage_days,
        )
    )
    outcomes.append(check_lead_account_domain_mismatch(leads_df, accounts_df, owners_df))
    return outcomes


__all__ = [
    "ACTIVE_LEAD_STAGES",
    "ACTIVITY_OBJECT_MAP",
    "CLOSED_STAGES",
    "LEAD_LIFECYCLE_ORDER",
    "OPPORTUNITY_STAGE_ORDER",
    "check_account_foreign_keys",
    "check_activity_object_references",
    "check_allowed_value_sets",
    "check_close_date_chronology",
    "check_closed_won_amounts",
    "check_inactive_owner_open_opportunities",
    "check_lead_account_domain_mismatch",
    "check_missing_employee_count_for_enterprise",
    "check_missing_next_step_on_open_opportunities",
    "check_owner_foreign_keys",
    "check_recent_touches",
    "check_same_domain_multiple_reps",
    "check_same_email_multiple_accounts",
    "check_stale_opportunity_stage",
    "default_crm_business_rules",
]

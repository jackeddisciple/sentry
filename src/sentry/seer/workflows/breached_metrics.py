from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

from django.db import router, transaction
from django.utils import timezone
from pydantic import BaseModel, PositiveInt, ValidationError

from sentry import features
from sentry.constants import ObjectStatus
from sentry.incidents.grouptype import MetricIssue
from sentry.models.group import Group
from sentry.models.groupopenperiodactivity import GroupOpenPeriodActivity, OpenPeriodActivityType
from sentry.seer.models.workflow import (
    SeerWorkflowConfig,
    SeerWorkflowRun,
    SeerWorkflowRunExecution,
    SeerWorkflowStrategy,
    SeerWorkflowTriggerBinding,
)
from sentry.seer.workflows.admission import admission_rejection_reason, matches_scope
from sentry.services.eventstore.models import GroupEvent
from sentry.workflow_engine.models import (
    Action,
    DataConditionGroupAction,
    DetectorGroup,
    DetectorWorkflow,
    Workflow,
)


@dataclass(frozen=True)
class AcceptedMetricBreach:
    run: SeerWorkflowRun
    created: bool


@dataclass(frozen=True)
class IgnoredMetricBreach:
    reason: Literal[
        "unsupported_trigger",
        "invalid_source",
        "feature_disabled",
        "configuration_unavailable",
        "out_of_scope",
        "workflow_unavailable",
    ]


type MetricBreachRejectionReason = Literal["quota_exceeded"]


@dataclass(frozen=True)
class RejectedMetricBreach:
    run: SeerWorkflowRun
    reason: MetricBreachRejectionReason
    created: bool


class MetricBreachTrigger(BaseModel):
    event_id: str
    open_period_id: PositiveInt
    occurrence_id: str
    detector_id: PositiveInt
    evidence: dict[str, Any]
    observed_at: datetime
    environment: str | None = None


@dataclass(frozen=True)
class MetricBreachWorkflowOrigin:
    workflow_id: int
    action_id: int


def accept_metric_breach(
    group: Group,
    event: GroupEvent,
    *,
    config_id: int,
    origin: MetricBreachWorkflowOrigin,
) -> AcceptedMetricBreach | RejectedMetricBreach | IgnoredMetricBreach:
    """Check eligibility, then record an idempotent admission decision for this attempt."""
    if group.type != MetricIssue.type_id or not isinstance(event, GroupEvent):
        return IgnoredMetricBreach("unsupported_trigger")
    occurrence = event.occurrence
    if (
        event.group_id != group.id
        or event.project_id != group.project_id
        or occurrence is None
        or occurrence.type != MetricIssue
        or occurrence.project_id != group.project_id
        or occurrence.event_id != event.event_id
    ):
        return IgnoredMetricBreach("invalid_source")
    if not features.has("organizations:seer-automations", group.organization):
        return IgnoredMetricBreach("feature_disabled")
    opened = (
        GroupOpenPeriodActivity.objects.filter(
            group_open_period__group=group,
            type=OpenPeriodActivityType.OPENED,
            event_id=event.event_id,
        )
        .select_related("group_open_period")
        .first()
    )
    if opened is None:
        return IgnoredMetricBreach("invalid_source")
    period = opened.group_open_period
    try:
        trigger = MetricBreachTrigger(
            event_id=event.event_id,
            open_period_id=period.id,
            occurrence_id=occurrence.id,
            detector_id=occurrence.evidence_data.get("detector_id"),
            evidence=dict(occurrence.evidence_data),
            observed_at=occurrence.detection_time,
            environment=event.get_tag("environment"),
        )
    except ValidationError:
        return IgnoredMetricBreach("invalid_source")
    if not _source_is_valid(group, trigger.detector_id):
        return IgnoredMetricBreach("invalid_source")

    with transaction.atomic(using=router.db_for_write(SeerWorkflowRun)):
        config = (
            SeerWorkflowConfig.objects.select_for_update()
            .filter(
                id=config_id,
                organization_id=group.organization.id,
                strategy=SeerWorkflowStrategy.BREACHED_METRIC_INVESTIGATION,
                enabled=True,
            )
            .first()
        )
        if config is None:
            return IgnoredMetricBreach("configuration_unavailable")
        if not matches_scope(config, group.project_id, trigger.environment):
            return IgnoredMetricBreach("out_of_scope")
        binding = SeerWorkflowTriggerBinding.objects.filter(
            workflow_config=config,
            trigger_action_id=origin.action_id,
            trigger_action__type=Action.Type.SEER_WORKFLOW,
            trigger_action__status=ObjectStatus.ACTIVE,
        ).first()
        if binding is None or not _workflow_is_valid(
            group, trigger.detector_id, origin.workflow_id, origin.action_id
        ):
            return IgnoredMetricBreach("workflow_unavailable")
        attempt_id = f"event:{event.event_id}"
        runs = SeerWorkflowRun.objects.filter(
            organization_id=group.organization.id,
            workflow_config=config,
            trigger_open_period=period,
        )
        previous_attempt = runs.filter(extras__admission__request_id=attempt_id).first()
        if previous_attempt is not None and previous_attempt.extras["status"] == "rejected":
            return RejectedMetricBreach(
                run=previous_attempt,
                reason=previous_attempt.extras["admission"]["reason"],
                created=False,
            )
        run = previous_attempt or runs.exclude(extras__status="rejected").first()
        created = run is None
        if run is None:
            reason = admission_rejection_reason(config)
            decided_at = timezone.now()
            run = SeerWorkflowRun.objects.create(
                organization_id=group.organization.id,
                workflow_config=config,
                trigger_open_period=period,
                date_completed=decided_at,
                extras={
                    "status": "rejected" if reason is not None else "complete",
                    "config_version": config.version,
                    "admission": {
                        "status": "rejected" if reason is not None else "accepted",
                        "reason": reason,
                        "decided_at": decided_at.isoformat(),
                        "request_id": attempt_id,
                    },
                    "trigger": {
                        **trigger.dict(),
                        "observed_at": trigger.observed_at.isoformat(),
                        "group_id": group.id,
                        "project_id": group.project_id,
                        "invocation_source": "wfe",
                        "binding_id": binding.id,
                        "workflow_id": origin.workflow_id,
                        "action_id": origin.action_id,
                    },
                },
            )
            if reason is not None:
                return RejectedMetricBreach(run=run, reason=reason, created=True)
            # Investigation dispatch will replace this explicit no-op execution.
            SeerWorkflowRunExecution.objects.create(
                run=run, extras={"status": "complete", "result": "noop"}
            )
        return AcceptedMetricBreach(run=run, created=created)


def _source_is_valid(group: Group, detector_id: int) -> bool:
    return DetectorGroup.objects.filter(
        group=group,
        detector_id=detector_id,
        detector__type=MetricIssue.slug,
        detector__project_id=group.project_id,
        detector__status=ObjectStatus.ACTIVE,
    ).exists()


def _workflow_is_valid(group: Group, detector_id: int, workflow_id: int, action_id: int) -> bool:
    return (
        Workflow.objects.filter(
            id=workflow_id,
            organization_id=group.organization.id,
            enabled=True,
            status=ObjectStatus.ACTIVE,
        ).exists()
        and DetectorWorkflow.objects.filter(
            detector_id=detector_id, workflow_id=workflow_id
        ).exists()
        and DataConditionGroupAction.objects.filter(
            action_id=action_id,
            action__type=Action.Type.SEER_WORKFLOW,
            action__status=ObjectStatus.ACTIVE,
            condition_group__workflowdataconditiongroup__workflow_id=workflow_id,
        ).exists()
    )

from typing import Literal

from sentry.seer.models.workflow import SeerWorkflowConfig
from sentry.seer.workflows.workflow import SeerWorkflow


def matches_scope(config: SeerWorkflowConfig, project_id: int, environment: str | None) -> bool:
    """Check the automation's scope before recording an admission decision."""
    workflow = SeerWorkflow(config)
    return (
        config.enabled
        and project_id in workflow.get_project_ids()
        and (not workflow.filters.environments or environment in workflow.filters.environments)
    )


def admission_rejection_reason(
    config: SeerWorkflowConfig,
) -> Literal["quota_exceeded"] | None:
    """Accept eligible invocations; quota reservation will be added here."""
    return None

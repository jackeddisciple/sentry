import logging
from typing import override

from sentry.seer.models.workflow import SeerWorkflowTriggerBinding
from sentry.seer.workflows.breached_metrics import (
    AcceptedMetricBreach,
    BreachedMetricWorkflow,
    MetricBreachWorkflowOrigin,
    RejectedMetricBreach,
)
from sentry.services.eventstore.models import GroupEvent
from sentry.workflow_engine.models import Action
from sentry.workflow_engine.registry import action_handler_registry
from sentry.workflow_engine.types import ActionHandler, ActionInvocation

logger = logging.getLogger(__name__)


@action_handler_registry.register(Action.Type.SEER_WORKFLOW)
class SeerWorkflowActionHandler(ActionHandler):
    group = ActionHandler.Group.OTHER
    config_schema = {"type": "object", "properties": {}, "additionalProperties": False}
    data_schema = {"type": "object", "properties": {}, "additionalProperties": False}

    @staticmethod
    @override
    def execute(invocation: ActionInvocation) -> None:
        event = invocation.event_data.event
        state = invocation.event_data.group_state
        if not isinstance(event, GroupEvent) or state is None or not state["is_new"]:
            return
        group = invocation.event_data.group
        binding = (
            SeerWorkflowTriggerBinding.objects.filter(
                workflow_config__organization_id=group.project.organization_id,
                trigger_action=invocation.action,
            )
            .select_related("workflow_config")
            .first()
        )
        if binding is None:
            return

        admission = BreachedMetricWorkflow(binding.workflow_config).accept(
            group,
            event,
            origin=MetricBreachWorkflowOrigin(
                workflow_id=invocation.workflow_id, action_id=invocation.action.id
            ),
        )
        extra: dict[str, object] = {
            "workflow_id": invocation.workflow_id,
            "action_id": invocation.action.id,
        }
        if isinstance(admission, AcceptedMetricBreach):
            outcome = "accepted"
            extra.update(run_id=admission.run.id, run_created=admission.created)
        elif isinstance(admission, RejectedMetricBreach):
            outcome = "rejected"
            extra.update(run_id=admission.run.id, reason=admission.reason)
        else:
            outcome = "ignored"
            extra.update(reason=admission.reason)
        logger.info("seer.workflow.trigger.%s", outcome, extra=extra)

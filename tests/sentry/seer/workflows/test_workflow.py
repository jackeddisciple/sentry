from unittest.mock import patch

import pytest
from django.core.cache import cache

from sentry.constants import ObjectStatus
from sentry.models.organization import Organization
from sentry.models.project import Project
from sentry.seer.models.workflow import (
    SeerWorkflowConfig,
    SeerWorkflowConfigProject,
    SeerWorkflowFilters,
    SeerWorkflowStrategy,
)
from sentry.seer.workflows.workflow import AllProjects, SeerWorkflow, SelectedProjects
from sentry.service_accounts.models import ServiceAccount
from sentry.service_accounts.services.service_account import RpcServiceAccount
from sentry.service_accounts.services.service_account.service import service_account_service
from sentry.testutils.cases import TestCase
from sentry.testutils.factories import Factories
from sentry.testutils.outbox import outbox_runner
from sentry.testutils.silo import assume_test_silo_mode_of, cell_silo_test


class SeerWorkflowEnsureDefaultsTest(TestCase):
    def test_seeds_defaults_once_per_organization(self) -> None:
        other = self.create_organization()
        Factories.create_seer_workflow_config(
            self.organization, strategy=SeerWorkflowStrategy.AGENTIC_TRIAGE
        )
        first = self.create_project()

        SeerWorkflow.ensure_defaults(self.organization.id)
        (config,) = self.v2_configs()
        later = self.create_project()
        SeerWorkflow.ensure_defaults(self.organization.id)

        workflow = SeerWorkflow(config)
        assert self.v2_configs() == [config]
        assert self.v2_configs(other) == []
        assert workflow.filters.all_projects is True
        assert workflow.filters.environments == []
        assert workflow.extras.seeded_default == SeerWorkflowStrategy.AGENTIC_TRIAGE_V2
        assert workflow.get_project_ids() == [first.id, later.id]

    def test_existing_disabled_config_is_not_reseeded_or_changed(self) -> None:
        selected = self.create_project()
        existing = Factories.create_seer_workflow_config(
            self.organization,
            strategy=SeerWorkflowStrategy.AGENTIC_TRIAGE_V2,
            enabled=False,
            filters={"environments": ["production"]},
            version=3,
        )
        Factories.create_seer_workflow_config_project(existing, selected)

        SeerWorkflow.ensure_defaults(self.organization.id)

        (config,) = self.v2_configs()
        assert config.id == existing.id
        assert config.enabled is False
        assert config.filters == {"environments": ["production"]}
        assert config.version == 3
        assert SeerWorkflow(config).extras.seeded_default is None
        assert SeerWorkflow(config).get_project_ids() == [selected.id]

    def v2_configs(self, organization: Organization | None = None) -> list[SeerWorkflowConfig]:
        return list(
            SeerWorkflowConfig.objects.filter(
                organization=organization or self.organization,
                strategy=SeerWorkflowStrategy.AGENTIC_TRIAGE_V2,
            ).order_by("id")
        )


class SeerWorkflowProjectSelectionTest(TestCase):
    def test_selected_projects_exclude_inactive_and_deleted_projects(self) -> None:
        first, second = self.create_project(), self.create_project()
        self.create_project()
        workflow = SeerWorkflow(self.create_config(first, second))
        assert workflow.get_project_ids() == [first.id, second.id]

        first.update(status=ObjectStatus.PENDING_DELETION)
        assert workflow.get_project_ids() == [second.id]

        second.delete()
        assert workflow.get_project_ids() == []
        assert workflow.filters.all_projects is False

    def test_replaces_selection_preserving_latest_filters_and_version(self) -> None:
        previous, selected = self.create_project(), self.create_project()
        config = self.create_config(previous)
        other_config = self.create_config(previous)
        workflow = SeerWorkflow(config)
        SeerWorkflowConfig.objects.filter(id=config.id, organization=self.organization).update(
            filters=SeerWorkflowFilters(environments=["production"]).dict()
            | {"min_event_count": 10},
            version=3,
        )

        workflow.set_project_selection(SelectedProjects(project_ids=(selected.id, selected.id)))

        config.refresh_from_db()
        assert config.filters == {
            "all_projects": False,
            "environments": ["production"],
            "min_event_count": 10,
        }
        assert config.version == workflow.config.version == 4
        assert workflow.get_project_ids() == [selected.id]
        assert SeerWorkflow(other_config).get_project_ids() == [previous.id]

    def test_switching_to_all_projects_clears_links_and_scopes_active_projects(self) -> None:
        selected = self.create_project()
        self.create_project(organization=self.create_organization())
        self.create_project(status=ObjectStatus.PENDING_DELETION)
        config = self.create_config(selected)
        workflow = SeerWorkflow(config)
        initial_filters = workflow.filters
        assert initial_filters.all_projects is False

        workflow.set_project_selection(AllProjects())
        later = self.create_project()

        config.refresh_from_db()
        assert SeerWorkflow(config).filters.all_projects is True
        assert workflow.filters.all_projects is True
        assert workflow.get_project_ids() == [selected.id, later.id]
        assert not SeerWorkflowConfigProject.objects.filter(config=config).exists()
        assert config.version == 2

    def test_empty_selection_switches_from_all_to_no_projects(self) -> None:
        self.create_project()
        config = self.create_config(all_projects=True)
        workflow = SeerWorkflow(config)

        workflow.set_project_selection(SelectedProjects(project_ids=()))

        config.refresh_from_db()
        assert SeerWorkflow(config).filters.all_projects is False
        assert workflow.get_project_ids() == []
        assert not SeerWorkflowConfigProject.objects.filter(config=config).exists()
        assert config.version == 2

    def test_rejects_cross_organization_projects_without_changing_selection(self) -> None:
        previous, valid = self.create_project(), self.create_project()
        foreign = self.create_project(organization=self.create_organization())
        config = self.create_config(previous)
        workflow = SeerWorkflow(config)

        with pytest.raises(ValueError, match="Selected projects must exist"):
            workflow.set_project_selection(SelectedProjects(project_ids=(valid.id, foreign.id)))

        config.refresh_from_db()
        assert workflow.get_project_ids() == [previous.id]
        assert config.filters == {"all_projects": False}
        assert config.version == 1

    def test_failed_insert_restores_previous_selection(self) -> None:
        previous, selected = self.create_project(), self.create_project()
        config = self.create_config(previous)
        workflow = SeerWorkflow(config)

        with (
            patch.object(
                SeerWorkflowConfigProject.objects, "bulk_create", side_effect=RuntimeError("failed")
            ),
            pytest.raises(RuntimeError, match="failed"),
        ):
            workflow.set_project_selection(SelectedProjects(project_ids=(selected.id,)))

        config.refresh_from_db()
        assert workflow.get_project_ids() == [previous.id]
        assert config.filters == {"all_projects": False}
        assert config.version == 1

    def create_config(self, *selected: Project, all_projects: bool = False) -> SeerWorkflowConfig:
        config = Factories.create_seer_workflow_config(
            self.organization,
            strategy=SeerWorkflowStrategy.AGENTIC_TRIAGE_V2,
            filters={"all_projects": all_projects},
        )
        for project in selected:
            Factories.create_seer_workflow_config_project(config, project)
        return config


@cell_silo_test
class SeerWorkflowServiceAccountTest(TestCase):
    def setUp(self) -> None:
        super().setUp()
        cache.clear()
        self.account = Factories.create_service_account(
            self.organization, scope_list=["org:read", "project:read"]
        )
        self.config = Factories.create_seer_workflow_config(
            self.organization,
            strategy=SeerWorkflowStrategy.AGENTIC_TRIAGE_V2,
            filters={"environments": ["production"]},
            output_policy={"review": True},
        )
        self.workflow = SeerWorkflow(self.config)

    def test_assigns_account_to_independent_configurations(self) -> None:
        other = Factories.create_seer_workflow_config(
            self.organization, strategy=SeerWorkflowStrategy.AGENTIC_TRIAGE_V2
        )

        self.workflow.set_service_account(self.account.id)

        self.config.refresh_from_db()
        other.refresh_from_db()
        assert self.config.service_account_id == self.account.id
        assert self.config.version == self.workflow.config.version == 2
        assert other.service_account_id is None
        assert other.version == 1

        SeerWorkflow(other).set_service_account(self.account.id)
        other.refresh_from_db()
        assert other.service_account_id == self.config.service_account_id

    def test_replaces_account_preserving_latest_settings_and_version(self) -> None:
        self.workflow.set_service_account(self.account.id)
        replacement = Factories.create_service_account(self.organization)
        SeerWorkflowConfig.objects.filter(id=self.config.id, organization=self.organization).update(
            filters={"all_projects": True}, enabled=True, version=4
        )

        self.workflow.set_service_account(replacement.id)

        self.config.refresh_from_db()
        assert self.config.service_account_id == replacement.id
        assert self.config.version == self.workflow.config.version == 5
        assert self.config.filters == {"all_projects": True}
        assert self.config.output_policy == {"review": True}
        assert self.config.enabled is True

    def test_no_op_assignment_preserves_version_and_timestamp(self) -> None:
        self.workflow.set_service_account(self.account.id)
        timestamp = self.workflow.config.date_updated

        self.workflow.set_service_account(self.account.id)

        self.config.refresh_from_db()
        assert self.config.version == self.workflow.config.version == 2
        assert self.config.date_updated == timestamp

    def test_clear_does_not_look_up_former_account(self) -> None:
        self.workflow.set_service_account(self.account.id)

        with patch.object(service_account_service, "get") as get_account:
            self.workflow.set_service_account(None)
            timestamp = self.workflow.config.date_updated
            self.workflow.set_service_account(None)

        get_account.assert_not_called()
        self.config.refresh_from_db()
        assert self.config.service_account_id is None
        assert self.config.version == self.workflow.config.version == 3
        assert self.config.date_updated == timestamp
        assert self.config.filters == {"environments": ["production"]}

    def test_rejects_missing_account(self) -> None:
        self.assert_account_rejected(None, "does not exist")

    def test_rejects_inactive_account(self) -> None:
        self.assert_account_rejected(
            RpcServiceAccount(
                id=self.account.id, organization_id=self.organization.id, is_active=False
            ),
            "inactive",
        )

    def test_rejects_cross_organization_account(self) -> None:
        self.assert_account_rejected(
            RpcServiceAccount(
                id=self.account.id, organization_id=self.organization.id + 1, is_active=True
            ),
            "must belong to the workflow's organization",
        )

    def assert_account_rejected(self, account: RpcServiceAccount | None, message: str) -> None:
        self.workflow.set_service_account(self.account.id)

        with (
            patch.object(service_account_service, "get", return_value=account),
            pytest.raises(ValueError, match=message),
        ):
            self.workflow.set_service_account(self.account.id)
        with (
            patch.object(service_account_service, "get", return_value=account),
            pytest.raises(ValueError, match=message),
        ):
            self.workflow.resolve_service_account()

        self.config.refresh_from_db()
        assert self.config.service_account_id == self.account.id
        assert self.config.version == 2

    def test_lookup_failure_does_not_mutate_config(self) -> None:
        self.workflow.set_service_account(self.account.id)
        with (
            patch.object(service_account_service, "get", side_effect=RuntimeError("RPC failed")),
            pytest.raises(RuntimeError, match="RPC failed"),
        ):
            self.workflow.set_service_account(self.account.id)
        with (
            patch.object(service_account_service, "get", side_effect=RuntimeError("RPC failed")),
            pytest.raises(RuntimeError, match="RPC failed"),
        ):
            self.workflow.resolve_service_account()

        self.config.refresh_from_db()
        assert self.config.service_account_id == self.account.id
        assert self.config.version == 2

    def test_unset_association_fails_without_lookup(self) -> None:
        with (
            patch.object(service_account_service, "get") as get_account,
            pytest.raises(ValueError, match="no service account configured"),
        ):
            self.workflow.resolve_service_account()

        get_account.assert_not_called()

    def test_resolves_current_account_state_after_invalidation(self) -> None:
        self.workflow.set_service_account(self.account.id)
        resolved = self.workflow.resolve_service_account()
        assert resolved.id == self.account.id
        assert resolved.organization_id == self.organization.id
        assert resolved.scope_list == ["org:read", "project:read"]

        with assume_test_silo_mode_of(ServiceAccount), outbox_runner():
            self.account.update(is_active=False)

        with pytest.raises(ValueError, match="inactive"):
            self.workflow.resolve_service_account()

    def test_rejects_deleted_account_before_association_cleanup(self) -> None:
        self.workflow.set_service_account(self.account.id)
        assert self.workflow.resolve_service_account().id == self.account.id

        with assume_test_silo_mode_of(ServiceAccount), outbox_runner():
            self.account.delete()

        self.config.refresh_from_db()
        assert self.config.service_account_id is not None
        with pytest.raises(ValueError, match="does not exist"):
            self.workflow.resolve_service_account()

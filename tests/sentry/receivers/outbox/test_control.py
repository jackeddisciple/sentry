from unittest.mock import MagicMock, patch

from sentry.deletions.tasks.hybrid_cloud import process_hybrid_cloud_foreign_key_cascade_batch
from sentry.integrations.models.integration import Integration
from sentry.models.apiapplication import ApiApplication
from sentry.models.tombstone import CellTombstone
from sentry.receivers.outbox.control import (
    process_api_application_updates,
    process_identity_updates,
    process_integration_updates,
    process_sentry_app_updates,
    process_service_account_updates,
)
from sentry.seer.models.workflow import SeerWorkflowConfig, SeerWorkflowStrategy
from sentry.service_accounts.models import ServiceAccount
from sentry.service_accounts.services.service_account.service import service_account_service
from sentry.testutils.cases import TestCase
from sentry.testutils.factories import Factories
from sentry.testutils.outbox import outbox_runner
from sentry.testutils.silo import (
    assume_test_silo_mode_of,
    cell_silo_test,
    control_silo_test,
)
from sentry.types.cell import Cell
from sentry.users.models.identity import Identity

_TEST_CELL = Cell("eu", 1, "http://eu.testserver")


@control_silo_test(cells=[_TEST_CELL])
class ProcessControlOutboxTest(TestCase):
    identifier = 1

    @patch("sentry.receivers.outbox.control.maybe_process_tombstone")
    def test_process_integration_updates(self, mock_maybe_process: MagicMock) -> None:
        process_integration_updates(object_identifier=self.identifier, cell_name=_TEST_CELL.name)
        mock_maybe_process.assert_called_with(
            Integration, self.identifier, cell_name=_TEST_CELL.name
        )

    @patch("sentry.receivers.outbox.control.maybe_process_tombstone")
    def test_process_identity_updates(self, mock_maybe_process: MagicMock) -> None:
        process_identity_updates(object_identifier=self.identifier, cell_name=_TEST_CELL.name)
        mock_maybe_process.assert_called_with(Identity, self.identifier, cell_name=_TEST_CELL.name)

    @patch("sentry.receivers.outbox.control.maybe_process_tombstone")
    def test_process_api_application_updates(self, mock_maybe_process: MagicMock) -> None:
        process_api_application_updates(
            object_identifier=self.identifier, cell_name=_TEST_CELL.name
        )
        mock_maybe_process.assert_called_with(
            ApiApplication, self.identifier, cell_name=_TEST_CELL.name
        )

    @patch("sentry.sentry_apps.tasks.sentry_apps.cell_caching_service")
    def test_process_sentry_app_updates(self, mock_caching: MagicMock) -> None:
        org = self.create_organization()
        sentry_app = self.create_sentry_app()
        install = self.create_sentry_app_installation(slug=sentry_app.slug, organization=org)
        install_dupe = self.create_sentry_app_installation(slug=sentry_app.slug, organization=org)

        org_two = self.create_organization()
        install_two = self.create_sentry_app_installation(
            slug=sentry_app.slug, organization=org_two
        )

        with self.tasks():
            process_sentry_app_updates(object_identifier=sentry_app.id, cell_name=_TEST_CELL.name)
        mock_caching.clear_key.assert_any_call(
            key=f"app_service.get_installation:{install.id}", cell_name=_TEST_CELL.name
        )
        mock_caching.clear_key.assert_any_call(
            key=f"app_service.get_installation:{install_dupe.id}", cell_name=_TEST_CELL.name
        )
        mock_caching.clear_key.assert_any_call(
            key=f"app_service.get_installation:{install_two.id}", cell_name=_TEST_CELL.name
        )
        mock_caching.clear_key.assert_any_call(
            key=f"app_service.get_by_application_id:{sentry_app.application_id}",
            cell_name=_TEST_CELL.name,
        )
        mock_caching.clear_key.assert_any_call(
            key=f"app_service.get_installed_for_organization:{org.id}",
            cell_name=_TEST_CELL.name,
        )
        mock_caching.clear_key.assert_any_call(
            key=f"app_service.get_installed_for_organization:{org_two.id}",
            cell_name=_TEST_CELL.name,
        )


@cell_silo_test(cells=[_TEST_CELL])
class ProcessServiceAccountUpdatesTest(TestCase):
    def test_deletion_preserves_workflows_and_replays_safely(self) -> None:
        account = Factories.create_service_account(self.organization)
        account_id = account.id
        other_account = Factories.create_service_account(self.organization)
        first = Factories.create_seer_workflow_config(
            self.organization,
            strategy=SeerWorkflowStrategy.AGENTIC_TRIAGE_V2,
            service_account_id=account.id,
            filters={"environments": ["production"]},
            output_policy={"review": True},
            enabled=True,
        )
        second = Factories.create_seer_workflow_config(
            self.organization,
            strategy=SeerWorkflowStrategy.AGENTIC_TRIAGE_V2,
            service_account_id=account.id,
        )
        unrelated = Factories.create_seer_workflow_config(
            self.organization,
            strategy=SeerWorkflowStrategy.AGENTIC_TRIAGE_V2,
            service_account_id=other_account.id,
        )
        assert service_account_service.get(service_account_id=account_id) is not None

        with assume_test_silo_mode_of(ServiceAccount), outbox_runner():
            account.delete()

        assert service_account_service.get(service_account_id=account_id) is None
        assert CellTombstone.objects.filter(
            table_name=ServiceAccount._meta.db_table, object_identifier=account_id
        ).exists()
        with self.tasks():
            process_hybrid_cloud_foreign_key_cascade_batch(
                app_name="seer", model_name="SeerWorkflowConfig", field_name="service_account_id"
            )

        first.refresh_from_db()
        second.refresh_from_db()
        unrelated.refresh_from_db()
        assert first.service_account_id is None
        assert second.service_account_id is None
        assert first.filters == {"environments": ["production"]}
        assert first.output_policy == {"review": True}
        assert first.enabled is True
        assert first.version == 1
        assert unrelated.service_account_id == other_account.id

        with assume_test_silo_mode_of(ServiceAccount):
            process_service_account_updates(account_id, cell_name=_TEST_CELL.name)
        with self.tasks():
            process_hybrid_cloud_foreign_key_cascade_batch(
                app_name="seer", model_name="SeerWorkflowConfig", field_name="service_account_id"
            )

        assert (
            SeerWorkflowConfig.objects.filter(
                organization=self.organization,
                id__in=[first.id, second.id],
                service_account_id=None,
            ).count()
            == 2
        )
        unrelated.refresh_from_db()
        assert unrelated.service_account_id == other_account.id

    def test_account_and_project_updates_keep_association_and_invalidate_cache(self) -> None:
        account = Factories.create_service_account(self.organization)
        config = Factories.create_seer_workflow_config(
            self.organization,
            strategy=SeerWorkflowStrategy.AGENTIC_TRIAGE_V2,
            service_account_id=account.id,
        )
        cached = service_account_service.get(service_account_id=account.id)
        assert cached is not None
        assert cached.project_ids == []
        assert cached.is_active

        project = self.create_project()
        with outbox_runner():
            Factories.create_service_account_project(account, project)
        with assume_test_silo_mode_of(ServiceAccount), outbox_runner():
            account.update(is_active=False)

        current = service_account_service.get(service_account_id=account.id)
        assert current is not None
        assert current.project_ids == [project.id]
        assert not current.is_active
        config.refresh_from_db()
        assert config.service_account_id == account.id
        assert not CellTombstone.objects.filter(
            table_name=ServiceAccount._meta.db_table, object_identifier=account.id
        ).exists()

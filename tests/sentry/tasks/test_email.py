from datetime import timedelta

from sentry.issues.action_log.types import GroupActionType
from sentry.issues.models.groupactionlogentry import GroupActionLogEntry
from sentry.models.activity import Activity
from sentry.tasks.email import process_inbound_email
from sentry.testutils.cases import TestCase
from sentry.testutils.helpers.features import with_feature
from sentry.testutils.skips import requires_snuba
from sentry.types.activity import ActivityType

pytestmark = [requires_snuba]


class ProcessInboundEmailTest(TestCase):
    def test_invalid_text(self) -> None:
        group = self.create_group()

        for payload in ("", " \t\n", "hello\x00world"):
            process_inbound_email(mailfrom=self.user.email, group_id=group.id, payload=payload)

        assert not Activity.objects.filter(group=group, type=ActivityType.NOTE.value).exists()

    @with_feature("projects:issue-action-log-write-to-db")
    def test_duplicate_delivery(self) -> None:
        group = self.create_group()

        process_inbound_email(
            mailfrom=self.user.email, group_id=group.id, payload=" \thello world!\n"
        )
        # Email delivery remains idempotent beyond the API's one-hour window.
        original = Activity.objects.get(group=group, type=ActivityType.NOTE.value)
        original.update(datetime=original.datetime - timedelta(hours=2))
        process_inbound_email(mailfrom=self.user.email, group_id=group.id, payload="hello world!")

        activity = Activity.objects.get(group=group, type=ActivityType.NOTE.value)
        assert activity.data == {"text": "hello world!"}
        entry = GroupActionLogEntry.objects.get(group_id=group.id, type=GroupActionType.COMMENT)
        assert entry.data == {"comment_id": activity.id}
        assert entry.source == "email"
        assert entry.actor_id == self.user.id

    def test_simple(self) -> None:
        group = self.create_group()

        process_inbound_email(mailfrom=self.user.email, group_id=group.id, payload="hello world!")

        activity = Activity.objects.get(group=group, type=ActivityType.NOTE.value)
        assert activity.user_id == self.user.id
        assert activity.data["text"] == "hello world!"

    def test_handle_unknown_address(self) -> None:
        group = self.create_group()

        process_inbound_email(
            mailfrom="invalid@example.com", group_id=group.id, payload="hello world!"
        )

        assert not Activity.objects.filter(group=group, type=ActivityType.NOTE.value).exists()

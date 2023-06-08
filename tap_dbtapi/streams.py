"""Stream type classes for tap-dbtapi."""

from __future__ import annotations
from pathlib import Path
from singer_sdk import typing as th  # JSON Schema typing helpers
from tap_dbtapi.client import dbtAPIStream
from typing import Dict, Any, Optional
from singer_sdk.helpers._typing import TypeConformanceLevel
import requests

class AccountStream(dbtAPIStream):
    """Define custom stream."""

    name = "accounts"
    path = "/accounts/{account_id}"
    primary_keys = ["id"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("docs_job_id", th.IntegerType),
        th.Property("freshness_job_id", th.IntegerType),
        th.Property("lock_reason", th.StringType),
        th.Property("unlock_if_subscription_renewed", th.BooleanType),
        th.Property("read_only_seats", th.IntegerType),
        th.Property("id", th.IntegerType),
        th.Property("name", th.StringType),
        th.Property("state", th.IntegerType),
        th.Property("plan", th.StringType),
        th.Property("pending_cancel", th.BooleanType),
        th.Property("run_slots", th.IntegerType),
        th.Property("developer_seats", th.IntegerType),
        th.Property("queue_limit", th.IntegerType),
        th.Property("pod_memory_request_mebibytes", th.NumberType),
        th.Property("enterprise_authentication_method", th.StringType),
        th.Property("enterprise_login_slug", th.StringType),
        th.Property("enterprise_unique_identifier", th.StringType),
        th.Property("billing_email_address", th.StringType),
        th.Property("locked", th.BooleanType),
        th.Property("develop_file_system", th.BooleanType),
        th.Property("unlocked_at", th.DateTimeType),
        th.Property("created_at", th.DateTimeType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("starter_repo_url", th.StringType),
        th.Property("sso_reauth", th.BooleanType),
        th.Property("docs_job", th.IntegerType),
        th.Property("freshness_job", th.IntegerType),
        th.Property("enterprise_login_url", th.StringType),
    ).to_dict()


class JobsStream(dbtAPIStream):
    """Define custom stream."""

    name = "jobs"
    path = "/accounts/{account_id}/jobs"
    primary_keys = ["id"]

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("account_id", th.IntegerType),
        th.Property("project_id", th.IntegerType),
        th.Property("environment_id", th.IntegerType),
        th.Property("name", th.StringType),
        th.Property("dbt_version", th.StringType),
        th.Property("triggers", th.ObjectType()),
        th.Property("execute_steps", th.ArrayType(wrapped_type=th.StringType)),
        th.Property("settings", th.ObjectType()),
        th.Property("state", th.IntegerType),
        th.Property("generate_docs", th.BooleanType),
        th.Property("schedule", th.ObjectType()),
        th.Property("execution", th.ObjectType()),
        th.Property("created_at", th.DateTimeType),
        th.Property("updated_at", th.DateTimeType()),
        th.Property("deferring_job_definition_id", th.IntegerType()),
        th.Property("lifecycle_webhooks", th.BooleanType()),
        th.Property("lifecycle_webhooks_url", th.StringType()),
        th.Property("is_deferrable", th.BooleanType()),
        th.Property("generate_sources", th.BooleanType()),
        th.Property("cron_humanized", th.StringType()),
        th.Property("next_run", th.DateTimeType()),
        th.Property("next_run_humanized", th.StringType())
    ).to_dict()

    def get_url_params(
        self,
        partition: Optional[dict],
        next_page_token: int,
    ) -> Dict[str, Any]:
        return {"order_by": "updated_at"}


class ProjectsStream(dbtAPIStream):
    """Define custom stream."""

    name = "projects"
    path = "/accounts/{account_id}/projects"
    primary_keys = ["id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("account_id", th.IntegerType),
        th.Property("connection", th.ObjectType()),
        th.Property("connection_id", th.IntegerType),
        th.Property("dbt_project_subdirectory", th.StringType),
        th.Property("name", th.StringType),
        th.Property("repository", th.ObjectType()),
        th.Property("repository_id", th.IntegerType),
        th.Property("state", th.IntegerType),
        th.Property("created_at", th.DateTimeType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("skipped_setup", th.BooleanType),
        th.Property("group_permissions", th.ArrayType(wrapped_type=th.ObjectType())),
        th.Property("docs_job_id", th.IntegerType),
        th.Property("freshness_job_id", th.IntegerType),
        th.Property("docs_job", th.ObjectType()),
        th.Property("freshness_job", th.ObjectType())
    ).to_dict()


class RunsStream(dbtAPIStream):
    """Define custom stream."""

    name = "runs"
    path = "/accounts/{account_id}/runs"
    primary_keys = ["id"]
    page_size = 100
    TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.ROOT_ONLY

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("trigger_id", th.IntegerType),
        th.Property("account_id", th.IntegerType),
        th.Property("project_id", th.IntegerType),
        th.Property("job_definition_id", th.IntegerType),
        th.Property("status", th.IntegerType),
        th.Property("git_branch", th.StringType),
        th.Property("git_sha", th.StringType),
        th.Property("status_message", th.StringType),
        th.Property("dbt_version", th.StringType),
        th.Property("created_at", th.DateTimeType),
        th.Property("updated_at",th.DateTimeType),
        th.Property("dequeued_at", th.DateTimeType),
        th.Property("should_start_at", th.DateTimeType),
        th.Property("started_at", th.DateTimeType),
        th.Property("finished_at", th.DateTimeType),
        th.Property("last_checked_at", th.DateTimeType),
        th.Property("last_heartbeat_at", th.DateTimeType),
        th.Property("owner_thread_id", th.StringType),
        th.Property("executed_by_thread_id", th.StringType),
        th.Property("artifacts_saved", th.BooleanType),
        th.Property("artifacts_s3_path", th.StringType),
        th.Property("has_docs_generated", th.BooleanType),
        th.Property("trigger", th.ObjectType()),
        th.Property("job", th.ObjectType()),
        th.Property("duration", th.StringType),
        th.Property("queued_duration", th.StringType),
        th.Property("run_duration", th.StringType),
        th.Property("duration_humanized", th.StringType),
        th.Property("queued_duration_humanized", th.StringType),
        th.Property("run_duration_humanized", th.StringType),
        th.Property("status_humanized", th.StringType),
        th.Property("created_at_humanized", th.StringType),
        th.Property("environment_id", th.IntegerType),
        th.Property("deferring_run_id", th.IntegerType),
        th.Property("artifact_s3_path", th.StringType),
        th.Property("has_sources_generated", th.BooleanType),
        th.Property("notifications_sent", th.BooleanType),
        th.Property("blocked_by", th.ArrayType(wrapped_type=th.IntegerType)),
        th.Property("scribe_enabled", th.BooleanType),
        th.Property("environment", th.StringType),
        th.Property("run_steps", th.ArrayType(wrapped_type=th.StringType)),
        th.Property("in_progress", th.BooleanType),
        th.Property("is_complete", th.BooleanType),
        th.Property("is_success", th.BooleanType),
        th.Property("is_error", th.BooleanType),
        th.Property("is_cancelled", th.BooleanType),
        th.Property("href", th.StringType),
        th.Property("finished_at_humanized", th.StringType),
        th.Property("job_id", th.IntegerType),
    ).to_dict()

    def get_url_params(
        self,
        partition: Optional[dict],
        next_page_token: int,
    ) -> Dict[str, Any]:

        return {
            "order_by": "finished_at",
            "limit": self.page_size,
            "offset": next_page_token,
        }

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Any:
        previous_token = previous_token or 0
        data = response.json()

        if len(data["data"]):
            return previous_token + self.page_size

        return None


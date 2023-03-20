"""dbtAPI tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_dbtapi import streams


class TapdbtAPI(Tap):
    """dbtAPI tap class."""

    name = "tap-dbtapi"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "auth_token",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="The token to authenticate against the API service",
        ),
        th.Property(
            "account_id",
            th.StringType,
            required=True,
            description="Account ID to read",
        ),
        th.Property(
            "api_url",
            th.StringType,
            default="https://api.mysample.com",
            description="The url for the API service",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.dbtAPIStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.AccountStream(self),
            streams.ProjectsStream(self),
            streams.JobsStream(self),
            streams.RunsStream(self)
        ]


if __name__ == "__main__":
    TapdbtAPI.cli()

"""REST client handling, including SnapchatAdsStream base class."""

from __future__ import annotations

import sys
import typing as t
from datetime import datetime, timedelta, UTC
from typing import Any, Callable, Iterable

import backoff
import pendulum
import requests
import logging
from dateutil import parser
from dateutil.parser import parse
from requests.exceptions import ConnectionError, Timeout
from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator, TPageToken  # noqa: TCH002
from singer_sdk.streams import RESTStream

if sys.version_info >= (3, 9):
    import importlib.resources as importlib_resources
else:
    import importlib_resources

_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = importlib_resources.files(__package__) / "schemas"

API_URL = "https://adsapi.snapchat.com/"
REFRESH_URL = "https://accounts.snapchat.com/login/oauth2/access_token"
API_VERSION = "v1"
LOGGER = logging.getLogger(__name__)


class Server5xxError(Exception):
    """5xx Server Error. We had a problem with our server. Try again later."""


class Server429Error(Exception):
    """429 Too Many Requests. You're requesting too many kittens! Slow down!"""

class SnapchatError(Exception):
    """Base class for Snapchat errors."""


class SnapchatBadRequestError(SnapchatError):
    """400	Bad Request. Your request sucks."""


class SnapchatUnauthorizedError(SnapchatError):
    """401	Unauthorized. Your API key is wrong."""


class SnapchatNotFoundError(SnapchatError):
    """404	Not Found. The specified kitten is not found."""


class SnapchatMethodNotAllowedError(SnapchatError):
    """405 Method Not Allowed. You tried to access a kitten with an invalid method."""


class SnapchatNotAcceptableError(SnapchatError):
    """406 	Not Acceptable. You requested a format that isn't json."""


class SnapchatGoneError(SnapchatError):
    """410 Gone. The kitten requested has been removed from our servers."""


class SnapchatTeapotError(SnapchatError):
    """410 Gone. I'm a teapot."""


class SnapchatForbiddenError(SnapchatError):
    """403 Forbidden. The kitten requested is hidden for administrators only."""


class SnapchatInternalServiceError(Server5xxError):
    """500 Internal Server Error. We had a problem with our server. Try again later."""


class SnapchatServiceUnavailableError(Server5xxError):
    """503 Service Unavailable. We're temporarily offline for maintenance. Please try again later."""  # noqa: E501


# Error Codes: https://developers.snapchat.com/api/docs/#errors
ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": SnapchatBadRequestError,
        "message": "The request is missing or has a bad parameter.",
    },
    401: {
        "raise_exception": SnapchatUnauthorizedError,
        "message": "Unauthorized access for the URL.",
    },
    403: {
        "raise_exception": SnapchatForbiddenError,
        "message": "User does not have permission to access the resource.",
    },
    404: {
        "raise_exception": SnapchatNotFoundError,
        "message": "The resource you have specified cannot be found.",
    },
    405: {
        "raise_exception": SnapchatMethodNotAllowedError,
        "message": "The provided HTTP method is not supported by the URL.",
    },
    406: {
        "raise_exception": SnapchatNotAcceptableError,
        "message": "You requested a format that isn’t json.",
    },
    410: {
        "raise_exception": SnapchatGoneError,
        "message": "Access to the Snapchat is no longer available.",
    },
    418: {
        "raise_exception": SnapchatTeapotError,
        "message": "The server refuses to brew coffee because it is, permanently, a teapot.",
    },
    429: {
        "raise_exception": Server429Error,
        "message": "You are requesting to many requests.",
    },
    500: {
        "raise_exception": SnapchatInternalServiceError,
        "message": "An error has occurred at Snapchat's end.",
    },
    503: {
        "raise_exception": SnapchatServiceUnavailableError,
        "message": "API service is currently unavailable.",
    },
}


def get_exception_for_error_code(status_code):
    return ERROR_CODE_EXCEPTION_MAPPING.get(status_code, {}).get(
        "raise_exception", SnapchatError
    )


# Error message example:
# {
#   "request_status": "ERROR",
#   "request_id": "5ebc40...",
#   "debug_message": "Resource can not be found",
#   "display_message": "We're sorry, but the requested resource is not available at this time",
#   "error_code": "E3003"
# }
def raise_for_error(response):
    try:
        response.raise_for_status()
    except (requests.HTTPError, requests.ConnectionError) as error:
        try:
            status_code = response.status_code
            # get json response if present, some status codes does not contains
            # json response, thus set to empty if not found
            try:
                response_json = response.json()
            except Exception:  # noqa: BLE001
                response_json = {}

            error_code = response_json.get("error_code", "")
            if error_code:
                error_code = ", " + error_code
            debug_message = response_json.get(
                "debug_message",
                response_json.get(
                    "error_description",
                    ERROR_CODE_EXCEPTION_MAPPING.get(status_code, {}).get(
                        "message", "Unknown Error"
                    ),
                ),
            )
            error_message = f"{status_code}{error_code}: {debug_message}"
            LOGGER.exception(error_message)
            if status_code > 500 and status_code != 503:  # noqa: PLR2004
                exception = Server5xxError
            else:
                exception = get_exception_for_error_code(status_code)
            raise exception(error_message) from error
        except (ValueError, TypeError) as err:
            raise SnapchatError(err) from err


class SnapchatAdsStream(RESTStream):
    """SnapchatAds stream class."""

    json_key_array = ""  # Override in subclasses
    json_key_record = ""  # Override in subclasses

    @property
    def url_base(self) -> str:
        """Return the API URL root."""
        return API_URL + API_VERSION

    records_jsonpath = "$[*]"  # Or override `parse_response`.

    # Set this value or override `get_new_paginator`.
    next_page_token_jsonpath = "$.next_link"  # noqa: S105
    pagination_limit = 200
    __access_token = None
    __expires = -1

    @backoff.on_exception(
        backoff.expo,
        (ConnectionError, Timeout, Server5xxError, Server429Error),
        max_tries=7,
        factor=3,
    )
    def get_access_token(self) -> str:
        """Get the access token."""
        if self.__access_token is not None and self.__expires > datetime.now(UTC):
            return self.__access_token
        response = requests.post(
            REFRESH_URL,
            data={
                "client_id": self.config.get("client_id"),
                "client_secret": self.config.get("client_secret"),
                "grant_type": "refresh_token",
                "refresh_token": self.config.get("refresh_token"),
            },
            timeout=60,
        )
        data = response.json()
        self.__access_token = data.get("access_token")
        expires_in = int(data.get("expires_in", "3600"))
        self.__expires = datetime.now(UTC) + timedelta(seconds=expires_in)
        return self.__access_token

    @property
    def authenticator(self) -> BearerTokenAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return BearerTokenAuthenticator.create_for_stream(
            self,
            token=self.get_access_token(),
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        # If not using an authenticator, you may also provide inline auth headers:
        # headers["Private-Token"] = self.config.get("auth_token")  # noqa: ERA001
        return headers

    def get_new_paginator(self) -> BaseAPIPaginator:
        """Create a new pagination helper instance.

        If the source API can make use of the `next_page_token_jsonpath`
        attribute, or it contains a `X-Next-Page` header in the response
        then you can remove this method.

        If you need custom pagination that uses page numbers, "next" links, or
        other approaches, please read the guide: https://sdk.meltano.com/en/v0.25.0/guides/pagination-classes.html.

        Returns:
            A pagination helper instance.
        """
        return super().get_new_paginator()

    def prepare_request(
        self, context: dict | None, next_page_token: str | None
    ) -> requests.PreparedRequest:
        """Prepare a request object for this stream.

        If partitioning is supported, the `context` object will contain the partition
        definitions. Pagination information can be parsed from `next_page_token` if
        `next_page_token` is not None.

        Args:
            context: Stream partition or context dictionary.
            next_page_token: Token, page number or any request argument to request the
                next page of data.

        Returns:
            Build a request with the stream's URL, path, query parameters,
            HTTP headers and authenticator.
        """
        http_method = self.rest_method
        if next_page_token:
            url = next_page_token
        else:
            url: str = self.get_url(context)
        params: dict | str = self.get_url_params(context, next_page_token)
        request_data = self.prepare_request_payload(context, next_page_token)
        headers = self.http_headers

        return self.build_prepared_request(
            method=http_method,
            url=url,
            params=params,
            headers=headers,
            json=request_data,
        )

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        response.raise_for_status()

        res = response.json()

        data = res.get(self.json_key_array, [])

        sub_data = []
        [sub_data.append(record[self.json_key_record]) for record in data]

        yield from sub_data

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary, or ``None`` to skip the record.
        """
        # TODO: Delete this method if not needed.
        return row


class SnapchatStatsStream(SnapchatAdsStream):
    """Parent class for stat based streams.

    Requires _sdc_timezone in context.
    """

    date_window_size = 30
    fields: str = ""
    granularity: str = "DAY"

    class TemporalPaginator(BaseAPIPaginator):
        """Day paginator class."""

        def __init__(
            self,
            json_key_array: str,
            json_key_record: str,
        ) -> None:
            """Initialize the paginator with a json_key to access the payload."""
            super().__init__(None)
            self.json_key_array = json_key_array
            self.json_key_record = json_key_record

        def get_next(self, response: requests.Response) -> TPageToken | None:
            """Return the next page URL."""
            arr = response.json().get(self.json_key_array)

            if not arr:
                return None

            record = arr[0].get(self.json_key_record)
            return record.get("end_time")

        def has_more(self, response: requests.Response) -> bool:
            """Return True if there are more records."""
            arr = response.json().get(self.json_key_array)

            if not arr:
                return False

            record = arr[0].get(self.json_key_record)
            end_time = record.get("end_time")

            tz = pendulum.timezone("UTC")
            return not timedelta(days=1) < datetime.now(tz=tz) - parse(end_time)

    def parse_response(self, response: requests.Response) -> t.Iterable[dict[str, Any]]:
        """Parse the response and return an iterator of result records."""
        res = super().parse_response(response)
        # Each 'row' has upto a month worth of data, so we need to break it down
        # Unpack the timeseries object, contains both start_date, end_date and stats
        for row in res:
            finalized_data_end_time = row.get("finalized_data_end_time", None)
            extras = {}
            if finalized_data_end_time:
                extras["finalized_data_end_time"] = finalized_data_end_time
            for ts in row["timeseries"]:
                stats = ts.get("stats", {})
                yield {
                    "id": row["id"],
                    "type": row["type"],
                    "granularity": row["granularity"],
                    "swipe_up_attribution_window": row["swipe_up_attribution_window"],
                    "view_attribution_window": row["view_attribution_window"],
                    "start_time": ts["start_time"],
                    "end_time": ts["end_time"],
                    **extras,
                    **stats,
                }

    def get_url_params(
        self, context: dict | None, next_page_token: str | None
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        start_time = next_page_token
        if start_time is None:
            start_time = self.get_starting_timestamp(context)
        else:
            start_time = parse(start_time)
        tz = pendulum.timezone(context["_sdc_timezone"])
        start_time = start_time.astimezone(tz=tz)
        start_time = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
        start_time = start_time.astimezone(pendulum.timezone("UTC"))

        end_time = datetime.now(tz=tz) - timedelta(days=1)
        min_timeframe = timedelta(days=1)
        if (end_time - start_time).days < min_timeframe.days:
            return None

        # Truncate the end time to the max allowed timeframe
        max_timeframe = timedelta(days=self.date_window_size)
        if (end_time - start_time).days > max_timeframe.days:
            end_time = start_time + max_timeframe

        end_time = end_time.astimezone(tz=tz)
        end_time = end_time.replace(hour=0, minute=0, second=0, microsecond=0)
        end_time = end_time.astimezone(pendulum.timezone("UTC"))

        start_time = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_time = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")

        return {
            "start_time": start_time,
            "end_time": end_time,
            "fields": self.fields,
            "granularity": self.granularity,
            "omit_empty": False,
            "conversion_source_types": "web,app,total",
            "swipe_up_attribution_window": self.config.get(
                "swipe_up_attribution_window", "28_DAY"
            ),
            "view_attribution_window": self.config.get(
                "view_attribution_window", "1_DAY"
            ),
        }

    def get_new_paginator(self) -> BaseAPIPaginator:
        """Return a new paginator instance."""
        return self.TemporalPaginator(
            self.json_key_array,
            self.json_key_record,
        )

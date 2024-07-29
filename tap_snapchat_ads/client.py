"""REST client handling, including SnapchatAdsStream base class."""

from __future__ import annotations

import sys
import typing as t
import urllib.parse
from datetime import UTC, datetime, timedelta
from typing import Any, Callable, Iterable

import backoff
import pendulum
import requests
from dateutil.parser import ParserError, parse
from requests.exceptions import ConnectionError, Timeout
from singer_sdk import metrics
from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.pagination import BaseAPIPaginator, TPageToken
from singer_sdk.streams import RESTStream

if t.TYPE_CHECKING:
    from requests import Response
    from singer_sdk.streams.core import Context
if sys.version_info >= (3, 9):
    import importlib.resources as importlib_resources
else:
    import importlib_resources

_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]

SCHEMAS_DIR = importlib_resources.files(__package__) / "schemas"

API_URL = "https://adsapi.snapchat.com/"
REFRESH_URL = "https://accounts.snapchat.com/login/oauth2/access_token"
API_VERSION = "v1"


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
        "message": "The server refuses to brew coffee because it is, permanently, a teapot.",  # noqa: E501
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


def raise_for_error(response):
    """Error message example:
    {
    "request_status": "ERROR",
    "request_id": "5ebc40...",
    "debug_message": "Resource can not be found",
    "display_message": "We're sorry, but the requested resource is not available at this time",
    "error_code": "E3003"
    }.
    """  # noqa: D205, E501
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
        if (
            SnapchatAdsStream.__access_token is not None
            and SnapchatAdsStream.__expires > datetime.now(UTC)
        ):
            return SnapchatAdsStream.__access_token
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
        SnapchatAdsStream.__access_token = data.get("access_token")
        expires_in = int(data.get("expires_in", "3600"))
        SnapchatAdsStream.__expires = datetime.now(UTC) + timedelta(seconds=expires_in)
        return SnapchatAdsStream.__access_token

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
        if urllib.parse.urlparse(next_page_token).scheme in ["http", "https"]:
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
        res = response.json()

        data = res.get(self.json_key_array, [])

        sub_data = []
        [sub_data.append(record[self.json_key_record]) for record in data]

        yield from sub_data


class SnapchatStatsStream(SnapchatAdsStream):
    """Parent class for stat based streams.

    Requires _sdc_timezone in context.
    """

    date_window_size = 30
    fields: str = ""
    granularity: str = "DAY"

    class TemporalPaginator(BaseAPIPaginator):
        """Day paginator class."""

        def __init__(self, json_key_array: str, json_key_record: str) -> None:
            """Initialize the paginator with a json_key to access the payload."""
            super().__init__(None)
            self.json_key_array = json_key_array
            self.json_key_record = json_key_record

        def advance(self, response: Response, context: Context) -> None:
            """Get a new page value and advance the current one.

            Args:
                response: API response object.
                context: Stream context object.

            Raises:
                RuntimeError: If a loop in pagination is detected. That is, when two
                    consecutive pagination tokens are identical.
            """
            self._page_count += 1

            if not self.has_more(response, context):
                self._finished = True
                return

            new_value = self.get_next(response)

            if new_value and new_value == self._value:
                msg = (
                    f"Loop detected in pagination. Pagination token {new_value} is "
                    "identical to prior token."
                )
                raise RuntimeError(msg)

            # Stop if new value None, empty string, 0, etc.
            if not new_value:
                self._finished = True
            else:
                self._value = new_value

        def get_next(self, response: requests.Response) -> TPageToken | None:
            """Return the next page URL."""
            super().get_next(response)
            arr = response.json().get(self.json_key_array)

            if not arr:
                return None

            record = arr[0].get(self.json_key_record)
            return record.get("end_time")

        def has_more(self, response: requests.Response, context: Context) -> bool:
            """Return True if there are more records."""
            try:
                arr = response.json().get(self.json_key_array, [])
                if not arr:
                    return False

                record = arr[0].get(self.json_key_record, {})

                min_date = datetime(1, 1, 1, tzinfo=pendulum.timezone("UTC"))
                start_time = parse(record.get("start_time", min_date))
                end_time = parse(record.get("end_time", min_date))

                valid_time = start_time and end_time

                min_timeframe = timedelta(days=1)
                if valid_time and (end_time - start_time).days < min_timeframe.days:
                    return False

                try:
                    if context_end_time := context.get("_sdc_end_time"):
                        return parse(end_time) < context_end_time

                except (ParserError, OverflowError, TypeError):
                    return False

                tz = pendulum.timezone("UTC")
                end_time_dt = parse(end_time).astimezone(tz)

                return end_time_dt < datetime.now(tz)

            except (KeyError, ValueError, TypeError):
                return False

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
        next_page_start_time = next_page_token

        min_date = datetime(1, 1, 1, tzinfo=pendulum.timezone("UTC"))
        next_page_start_time = (
            parse(next_page_start_time) if next_page_start_time else min_date
        )

        contexual_start_time = context.get("_sdc_start_time", min_date)
        replication_start_time = self.get_starting_timestamp(context)

        start_time = max(
            next_page_start_time, contexual_start_time, replication_start_time
        )

        tz = pendulum.timezone(context["_sdc_timezone"])
        start_time = start_time.astimezone(tz=tz)
        start_time = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
        start_time = start_time.astimezone(pendulum.timezone("UTC"))

        end_time = pendulum.now(tz=tz) - timedelta(days=1)

        # Truncate the end time to the max allowed timeframe
        max_timeframe = timedelta(days=self.date_window_size)
        if (end_time - start_time).days > max_timeframe.days:
            end_time = start_time + max_timeframe


        if context.get("_sdc_end_time") and end_time > context["_sdc_end_time"]:
            end_time = context["_sdc_end_time"]

        end_time = end_time.astimezone(tz=tz)
        end_time = end_time.replace(hour=0, minute=0, second=0, microsecond=0)
        end_time = end_time.astimezone(pendulum.timezone("UTC"))

        # Timeframe needs to be minimum of 1 day
        min_timeframe = timedelta(days=1)
        if (end_time - start_time).days < min_timeframe.days:
            start_time = end_time - min_timeframe
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

    def request_records(self, context: Context | None) -> t.Iterable[dict]:
        """Request records from REST endpoint(s), returning response records.

        If pagination is detected, pages will be recursed automatically.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            An item for every record in the response.
        """
        paginator = self.get_new_paginator()
        decorated_request = self.request_decorator(self._request)
        pages = 0

        with metrics.http_request_counter(self.name, self.path) as request_counter:
            request_counter.context = context

            while not paginator.finished:
                prepared_request = self.prepare_request(
                    context,
                    next_page_token=paginator.current_value,
                )
                resp = decorated_request(prepared_request, context)
                request_counter.increment()
                self.update_sync_costs(prepared_request, resp, context)
                records = iter(self.parse_response(resp))
                try:
                    first_record = next(records)
                except StopIteration:
                    self.logger.info(
                        "Pagination stopped after %d pages because no records were "
                        "found in the last response",
                        pages,
                    )
                    break
                yield first_record
                yield from records
                pages += 1

                paginator.advance(resp, context)

    def get_new_paginator(self) -> TemporalPaginator:
        """Return a new paginator instance."""
        return self.TemporalPaginator(self.json_key_array, self.json_key_record)

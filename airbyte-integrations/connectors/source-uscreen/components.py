from __future__ import annotations

import logging
import threading
import time
from collections import Counter
from typing import Any

from airbyte_cdk.sources.declarative.requesters.http_requester import HttpRequester


LOGGER = logging.getLogger("airbyte.uscreen")
SUMMARY_INTERVAL = 50
SLOW_REQUEST_SECONDS = 10.0


class UscreenHttpRequester(HttpRequester):
    def __post_init__(self, *args: Any, **kwargs: Any) -> None:
        super().__post_init__(*args, **kwargs)
        self._metrics_lock = threading.Lock()
        self._requests = 0
        self._errors = 0
        self._ignored = 0
        self._pagination_stops_total_count = 0
        self._pagination_stops_empty_page = 0
        self._elapsed_total = 0.0
        self._elapsed_max = 0.0
        self._statuses: Counter[int] = Counter()

    @property
    def _stream_name(self) -> str:
        return str(getattr(self, "name", "unknown"))

    @staticmethod
    def _is_empty_result(response: Any) -> bool:
        try:
            payload = response.json()
        except (TypeError, ValueError):
            return False

        if isinstance(payload, list):
            return not payload

        if isinstance(payload, dict):
            for key in ("data", "items", "results", "records"):
                value = payload.get(key)
                if isinstance(value, list):
                    return not value

        return False

    @staticmethod
    def _remove_next_link(response: Any) -> None:
        headers = getattr(response, "headers", None)
        if headers is not None:
            headers.pop("Link", None)

    def _apply_pagination_guard(self, response: Any) -> str | None:
        headers = getattr(response, "headers", None)
        if not headers or not headers.get("Link"):
            return None

        total_count = headers.get("Total-Count")
        if total_count is not None and str(total_count).strip() == "0":
            self._remove_next_link(response)
            return "total_count"

        if self._is_empty_result(response):
            self._remove_next_link(response)
            return "empty_page"

        return None

    def _record_result(
        self,
        response: Any,
        elapsed: float,
        stop_reason: str | None,
        error: bool = False,
    ) -> None:
        status = getattr(response, "status_code", None) if response is not None else None

        with self._metrics_lock:
            self._requests += 1
            self._elapsed_total += elapsed
            self._elapsed_max = max(self._elapsed_max, elapsed)

            if error:
                self._errors += 1
            elif response is None:
                self._ignored += 1
            elif isinstance(status, int):
                self._statuses[status] += 1

            if stop_reason == "total_count":
                self._pagination_stops_total_count += 1
            elif stop_reason == "empty_page":
                self._pagination_stops_empty_page += 1

            should_log = self._requests % SUMMARY_INTERVAL == 0
            if should_log:
                requests = self._requests
                errors = self._errors
                ignored = self._ignored
                total_count_stops = self._pagination_stops_total_count
                empty_page_stops = self._pagination_stops_empty_page
                average_elapsed = self._elapsed_total / self._requests
                max_elapsed = self._elapsed_max
                statuses = dict(sorted(self._statuses.items()))

        if should_log:
            LOGGER.info(
                "[uscreen] stream=%s requests=%s statuses=%s ignored=%s errors=%s "
                "avg_elapsed=%.3fs max_elapsed=%.3fs pagination_stops_total_count=%s "
                "pagination_stops_empty_page=%s",
                self._stream_name,
                requests,
                statuses,
                ignored,
                errors,
                average_elapsed,
                max_elapsed,
                total_count_stops,
                empty_page_stops,
            )

    def send_request(self, *args: Any, **kwargs: Any):
        started = time.monotonic()
        try:
            response = super().send_request(*args, **kwargs)
        except Exception:
            elapsed = time.monotonic() - started
            self._record_result(None, elapsed, None, error=True)
            LOGGER.exception(
                "[uscreen] stream=%s request_failed elapsed=%.3fs",
                self._stream_name,
                elapsed,
            )
            raise

        elapsed = time.monotonic() - started
        stop_reason = self._apply_pagination_guard(response) if response is not None else None
        self._record_result(response, elapsed, stop_reason)

        if elapsed >= SLOW_REQUEST_SECONDS:
            status = getattr(response, "status_code", None) if response is not None else None
            LOGGER.warning(
                "[uscreen] stream=%s slow_request elapsed=%.3fs status=%s",
                self._stream_name,
                elapsed,
                status,
            )

        if stop_reason == "empty_page":
            LOGGER.warning(
                "[uscreen] stream=%s stopped_pagination_on_empty_page_without_zero_total_count",
                self._stream_name,
            )

        return response

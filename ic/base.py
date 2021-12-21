import logging
import time

import requests

from ic.exceptions import IEXQueryError


logger = logging.getLogger(__name__)


class ICBase:
    """
    Base class for retrieving equities information from IEX Cloud.
    Conducts query operations including preparing and executing queries from
    the API.

    Attributes
    ----------
    retry_count: int, default 3, optional
        Desired number of retries if a request fails
    pause: float default 0.5, optional
        Pause time between retry attempts
    session: requests_cache.session, default None, optional
        A cached requests-cache session
    output_format: str -> default  "pandas" -> optional
        Desired output format (json or pandas DataFrame).
        set using the environment variable ``IEX_OUTPUT_FORMAT``.
    token: str, optional
        Authentication token (required for use with IEX Cloud). If not
        specified, use the environment variable ``IEX_TOKEN``
    """

    _URLS = {
        "stable": "https://cloud.iexapis.com/stable/",
        "v1": "https://cloud.iexapis.com/v1/",
        "beta": "https://cloud.iexapis.com/beta/",
        "sandbox": "https://sandbox.iexapis.com/stable/",
    }

    _VALID_FORMATS = ("json", "pandas")
    _VALID_API_VERSIONS = ("stable", "sandbox", "beta", "v1")

    def __init__(
        self,
        token,
        retry,
        pause,
        session,
        output_format,
        api_version,
        **kwargs
    ):

        self.retry_count = retry
        self.pause = pause
        self.session = self._init_session(session)
        self.token = token

        self.output_format = output_format
        if self.output_format not in self._VALID_FORMATS:
            message = "Please enter a valid output format (json or pandas)."
            raise ValueError(message)

        if api_version not in self._VALID_API_VERSIONS:
            message = (
                "Please select a valid API version"
                "(stable, sandbox, beta, v1)"
            )
            raise ValueError(message)
        self.base_url = self._URLS[api_version]

    @staticmethod
    def _init_session(session):
        if session is None:
            session = requests.session()
        return session

    @property
    def params(self):
        return {}

    @property
    def url(self):
        raise NotImplementedError

    def _validate_response(self, response):
        """Ensures response from IEX server is valid.

        Parameters
        ----------
        response: requests.response
            A requests.response object

        Returns
        -------
        response: Parsed JSON
            A json-formatted response

        Raises
        ------
        ValueError
            If a single Share symbol is invalid
        IEXQueryError
            If the JSON response is empty or throws an error

        """
        # log the number of messages used
        key = "iexcloud-messages-used"
        if key in response.headers:
            msg = response.headers[key]
        else:
            msg = "N/A"
        logger.info(f"MESSAGES USED: {msg}")

        if response.text == "Unknown symbol":
            raise IEXQueryError(response.status_code, response.text)
        try:
            data = response.json()
            if isinstance(data, str) and ("Error Message" in data):
                raise IEXQueryError(response.status_code, response.text)
        except ValueError:
            raise IEXQueryError(response.status_code, response.text)
        return data

    def _execute_iex_query(self, url):
        """Executes HTTP Request
        Given a URL, execute HTTP request from IEX server. If request is
        unsuccessful, attempt is made self.retry_count times with pause of
        self.pause in between.

        Parameters
        ----------
        url: str
            A properly-formatted url

        Returns
        -------
        response: requests.response
            Sends requests.response object to validator

        Raises
        ------
        IEXQueryError
            If problems arise when making the query
        """
        params = self.params
        params["token"] = self.token
        headers = {"project": "iex (Language=Python)"}
        for _ in range(self.retry_count + 1):
            response = self.session.get(
                url=url, params=params, headers=headers)
            logger.debug(f"REQUEST: {response.request.url}")
            logger.debug(f"RESPONSE: {response.status_code}")
            if response.status_code == requests.codes.ok:
                return self._validate_response(response)
            time.sleep(self.pause)
        return self._handle_error(response)

    def _handle_error(self, response):
        """
        Handles all responses which return an error status code
        """
        raise IEXQueryError(response.status_code, response.text)

    def _prepare_query(self):
        """Prepares the query URL

        Returns
        -------
        url: str
            A formatted URL
        """
        return f"{self.base_url}{self.url}"

    def fetch(self, format=None):
        """Fetches latest data

        Prepares the query URL based on self.params and executes the request

        Returns
        -------
        response: requests.response
            A response object
        """
        url = self._prepare_query()
        data = self._execute_iex_query(url)
        return self._format_output(data, format=format)

    def _format_output(self, out, format=None):
        """
        Output formatting handler
        """

        # If JSON output format, return exactly as received
        if self.output_format == "json":
            return out
        # Use custom formatter if supplied
        elif format is not None:
            return format(out)
        # Use pandas output format
        else:
            import pandas as pd
            return pd.DataFrame(out)

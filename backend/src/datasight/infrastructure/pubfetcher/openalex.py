import json
import logging
import os
import time
from typing import Dict, List, Optional, Tuple

import requests
from tqdm import tqdm

from datasight.config import OPENALEX_API_KEY, OPENALEX_API_URL

logger = logging.getLogger(__name__)

class OpenAlexClient:
    """
    Client for fetching publications from the OpenAlex API.
    ...
    Attributes:
    ----------
    api_key : str
        The API key for authenticating with the OpenAlex API.
    """

    # API paid plan: 1 / 100 = 0.01 seconds = 10 ms between requests minimum
    _REQUEST_DELAY = 0.15

    def __init__(self, api_key:str | None = None):
        self.api_key = api_key or OPENALEX_API_KEY
        self.base_url = OPENALEX_API_URL
        self.headers = {}

        if self.api_key:
            self.

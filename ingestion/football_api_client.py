import requests
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class FootballAPIClient:
    """
    Client สำหรับ football-data.org API
    Free tier: 10 calls/minute
    """

    BASE_URL = "https://api.football-data.org/v4"

    def __init__(self, api_key: str):
        self.session = requests.Session()
        self.session.headers.update({
            "X-Auth-Token": api_key
        })

    def get_matches(
        self,
        competition: str = "PL",
        date_from: Optional[str] = None,
        date_to: Optional[str] = None
    ) -> dict:
        """
        ดึงข้อมูล matches
        date format: YYYY-MM-DD
        """
        params = {}

        if date_from:
            params["dateFrom"] = date_from
        if date_to:
            params["dateTo"] = date_to

        try:
            response = self.session.get(
                f"{self.BASE_URL}/competitions/{competition}/matches",
                params=params,
                timeout=30
            )
            response.raise_for_status()
            logger.info(f"Fetched matches for {competition}")
            return response.json()

        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error: {e.response.status_code}")
            raise
        except requests.exceptions.Timeout:
            logger.error("Request timed out")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise

    def get_standings(self, competition: str = "PL") -> dict:
        """ดึง league standings"""
        try:
            response = self.session.get(
                f"{self.BASE_URL}/competitions/{competition}/standings",
                timeout=30
            )
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get standings: {e}")
            raise
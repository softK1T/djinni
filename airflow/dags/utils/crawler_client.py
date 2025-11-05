import requests
import time
import base64
import gzip
from typing import List, Dict, Any, Optional
import logging
from urllib.parse import quote

logger = logging.getLogger(__name__)


class CrawlerClient:
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url.rstrip('/')

    def test_connection(self) -> bool:
        try:
            response = requests.get(f"{self.base_url}/health", timeout=5)
            return response.status_code == 200
        except:
            return False

    def download_single(self, url: str) -> Optional[str]:
        try:
            logger.info(f"Downloading URL: {url}")

            job_payload = {
                'url': url,
                'timeout': 30
            }

            response = requests.post(f'{self.base_url}/v1/jobs/', json=job_payload)

            if response.status_code != 202:
                logger.error(f"Failed to create job: {response.status_code} - {response.text}")
                return None
            print(response.text)
            job_id = response.json()['job_id']
            logger.info(f"Created job: {job_id} for URL: {url}")

            for attempt in range(12):
                time.sleep(10)

                result_response = requests.get(f'{self.base_url}/v1/jobs/{job_id}/result')

                if result_response.status_code == 200:
                    result = result_response.json()

                    if result.get('error'):
                        logger.error(f"Crawler error: {result['error']}")
                        return None

                    body = result.get('body', '')
                    if body:
                        try:
                            decoded = base64.b64decode(body)
                            html = gzip.decompress(decoded).decode('utf-8')
                            logger.info(f"Successfully downloaded {len(html)} characters")
                            return html
                        except:
                            logger.info(f"Downloaded plain text: {len(body)} characters")
                            return body

                logger.info(f"Waiting for result... {attempt + 1}/12")

            logger.warning("Timeout waiting for result")
            return None

        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")
            return None

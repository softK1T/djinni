import requests
import time
import logging
import base64
import gzip

logger = logging.getLogger(__name__)


def decode_html_content(body: str) -> str:
    try:
        compressed_data = base64.b64decode(body)
        html = gzip.decompress(compressed_data).decode('utf-8')
        return html
    except Exception as e:
        logger.warning(f"Failed to decode content: {e}")
        return body


class CrawlerClient:
    def __init__(self, base_url: str = "http://host.docker.internal:8000", timeout: int = 30):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def download_single(self, url: str) -> str | None:
        resp = requests.post(f"{self.base_url}/v1/jobs/", json={"url": url}, timeout=self.timeout)
        resp.raise_for_status()
        job_id = resp.json()["job_id"]

        for _ in range(120):
            r = requests.get(f"{self.base_url}/v1/jobs/{job_id}/status", timeout=self.timeout)
            r.raise_for_status()
            data = r.json()
            state = data.get("state")

            if state == "SUCCESS":
                result_resp = requests.get(f"{self.base_url}/v1/jobs/{job_id}/result", timeout=self.timeout)
                result_resp.raise_for_status()
                result_data = result_resp.json()
                body = result_data.get("body")

                if body:
                    return decode_html_content(body)
                return body

            if state in ("FAILURE", "REVOKED"):
                logger.warning(f"Job {job_id} failed for {url}: state={state}")
                return None

            time.sleep(0.5)

        logger.warning(f"Timeout waiting job {job_id} for {url}")
        return None

    def download_batch(self, urls: list[str], headers: dict | None = None, per_request_timeout: int | None = None,
                       poll_interval: float = 0.5, max_wait_seconds: int = 120) -> dict[str, str | None]:
        if not urls:
            return {}

        payload = {
            "urls": urls,
            "headers": headers or {"User-Agent": "Mozilla/5.0"},
            "timeout": per_request_timeout or self.timeout
        }

        resp = requests.post(f"{self.base_url}/v1/batches/", json=payload, timeout=self.timeout)
        resp.raise_for_status()
        batch_data = resp.json()
        batch_id = batch_data["batch_id"]

        deadline = time.time() + max_wait_seconds
        while time.time() < deadline:
            s = requests.get(f"{self.base_url}/v1/batches/{batch_id}/status", timeout=self.timeout)
            s.raise_for_status()
            status = s.json()

            completed = status.get("completed", 0)
            total = status.get("total", 0)

            if completed >= total:
                break
            time.sleep(poll_interval)

        r = requests.get(f"{self.base_url}/v1/batches/{batch_id}/results", timeout=self.timeout)
        r.raise_for_status()
        data = r.json()

        results: dict[str, str | None] = {}

        if isinstance(data, list):
            items = data
        elif isinstance(data, dict) and "results" in data:
            items = data["results"]
        else:
            logger.error(f"Unexpected batch results format: {type(data)}")
            return results

        for item in items:
            url = item.get("url")
            body = item.get("body")
            error = item.get("error")

            if body and not error:
                results[url] = decode_html_content(body)
            else:
                results[url] = None

        return results

import logging
from utils.crawler_client import CrawlerClient
from utils.job_parser import extract_job_urls_from_catalog_page

logger = logging.getLogger(__name__)


class ExtractTasks:
    def __init__(self, max_pages=3, max_jobs=20):
        self.max_pages = max_pages
        self.max_jobs = max_jobs

    def extract_catalog_urls(self, **context):
        crawler = CrawlerClient()
        base_url = "https://djinni.co/jobs/"
        all_job_urls = []

        for page in range(1, self.max_pages + 1):
            catalog_url = f"{base_url}?page={page}"
            logger.info(f"Processing catalog page {page}: {catalog_url}")

            html = crawler.download_single(catalog_url)
            if not html:
                logger.warning(f"Failed to download page {page}")
                continue
            logger.info(f"Page {page} HTML length: {len(html)} chars")
            if len(html) < 1000:
                logger.warning(f"Page {page} HTML too short, content: {html[:500]}")

            job_urls = extract_job_urls_from_catalog_page(html)
            all_job_urls.extend(job_urls)
            logger.info(f"Page {page}: found {len(job_urls)} jobs")

        unique_urls = list(set(all_job_urls))
        logger.info(f"Total unique job URLs found: {len(unique_urls)}")

        limited_urls = unique_urls[:self.max_jobs]
        context['task_instance'].xcom_push(key='job_urls', value=limited_urls)
        return f"Extracted {len(unique_urls)} job URLs (processing first {len(limited_urls)})"

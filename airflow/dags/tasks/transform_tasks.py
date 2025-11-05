import logging
from utils.crawler_client import CrawlerClient
from utils.job_parser import parse_job_html

logger = logging.getLogger(__name__)


class TransformTasks:
    @staticmethod
    def scrape_jobs(**context):
        job_urls = context['task_instance'].xcom_pull(key='job_urls', task_ids='extract_catalog')

        if not job_urls:
            return "No URLs to scrape"

        crawler = CrawlerClient()
        scraped_jobs = []

        batch_size = 10
        # job_urls = job_urls[:10]

        for i in range(0, len(job_urls), batch_size):
            batch_urls = job_urls[i:i + batch_size]
            logger.info(f"Processing batch {i // batch_size + 1}: {len(batch_urls)} URLs")

            html_map = crawler.download_batch(batch_urls)

            for url in batch_urls:
                html = html_map.get(url)
                if not html:
                    logger.warning(f"Failed to download {url}")
                    continue

                try:
                    job_data = parse_job_html(html, url)
                    if job_data.get('error'):
                        logger.error(f"Parsing error for {url}: {job_data['error']}")
                        continue

                    scraped_jobs.append(job_data)
                    logger.info(f"{len(scraped_jobs)}: {job_data.get('title')} at {job_data.get('company_name')}")

                except Exception as e:
                    logger.error(f"Error processing {url}: {e}")

        context['task_instance'].xcom_push(key='scraped_jobs', value=scraped_jobs)
        return f"Scraped {len(scraped_jobs)} jobs successfully"

import logging
import time

from utils.crawler_client import CrawlerClient
from utils.job_parser import parse_job_html

logger = logging.getLogger(__name__)


class TransformTasks:
    @staticmethod
    def scrape_jobs(**context):
        import json
        import os
        from datetime import datetime

        job_urls = context['task_instance'].xcom_pull(key='job_urls', task_ids='extract_catalog')

        if not job_urls:
            return "No URLs to scrape"

        date_str = datetime.now().strftime('%Y-%m-%d_%H-%M')
        output_dir = f"/opt/airflow/data/scraped_jobs/{date_str}"
        os.makedirs(output_dir, exist_ok=True)

        crawler = CrawlerClient()
        batch_size = 50
        processed = 0
        failed = 0
        failed_batches = []

        for i in range(0, len(job_urls), batch_size):
            batch_urls = job_urls[i:i + batch_size]
            batch_num = i // batch_size + 1

            logger.info(f"Processing batch {batch_num}: {len(batch_urls)} URLs")

            try:
                html_map = crawler.download_batch(batch_urls, max_wait_seconds=180)

                for url in batch_urls:
                    html = html_map.get(url)
                    if html:
                        job_data = parse_job_html(html, url)
                        if not job_data.get('error'):
                            djinni_id = job_data.get('djinni_id')
                            if djinni_id:
                                filename = f"{output_dir}/job_{djinni_id}.json"
                                with open(filename, 'w', encoding='utf-8') as f:
                                    json.dump(job_data, f, ensure_ascii=False, indent=2)
                                processed += 1
                            else:
                                failed += 1
                        else:
                            failed += 1
                    else:
                        failed += 1

            except Exception as e:
                logger.error(f"Batch {batch_num} failed: {e}")
                failed += len(batch_urls)
                failed_batches.append((batch_num, batch_urls))

        if failed_batches:
            logger.info(f"Retrying {len(failed_batches)} failed batches...")

            for batch_num, batch_urls in failed_batches:
                try:
                    logger.info(f"RETRY batch {batch_num}")
                    time.sleep(10)
                    html_map = crawler.download_batch(batch_urls, max_wait_seconds=300)
                except Exception as e:
                    logger.error(f"RETRY batch {batch_num} also failed: {e}")

        metadata = {
            'total_urls': len(job_urls),
            'processed': processed,
            'failed': failed,
            'timestamp': datetime.now().isoformat(),
            'output_dir': output_dir
        }

        with open(f"{output_dir}/metadata.json", 'w') as f:
            json.dump(metadata, f, indent=2)

        context['task_instance'].xcom_push(key='scraped_data_dir', value=output_dir)

        return f"Scraped {processed} jobs, {failed} failed. Saved to {output_dir}"

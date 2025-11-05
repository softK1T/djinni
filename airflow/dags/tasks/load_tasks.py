import logging
from utils.database import get_db_connection, upsert_company, upsert_job

logger = logging.getLogger(__name__)


class LoadTasks:
    def save_catalog_to_db(self, **context):
        job_urls = context['task_instance'].xcom_pull(key='job_urls', task_ids='extract_catalog')

        if not job_urls:
            return "No URLs to save"

        with get_db_connection() as conn:
            cursor = conn.cursor()
            saved_count = 0

            for i, url in enumerate(job_urls):
                try:
                    djinni_id = int(url.split('/jobs/')[1].split('-')[0])
                    cursor.execute("""
                                   INSERT INTO job_catalog (djinni_id, url, page_number, position_on_page, global_position)
                                   VALUES (%s, %s, %s, %s, %s)
                                   ON CONFLICT (djinni_id, catalog_scraped_at) DO NOTHING
                                   """, (djinni_id, url, 1, i + 1, i + 1))
                    saved_count += 1
                except Exception as e:
                    logger.error(f"Error saving URL {url}: {e}")

            conn.commit()

        return f"Saved {saved_count} URLs to catalog"

    def save_jobs_to_db(self, **context):
        scraped_jobs = context['task_instance'].xcom_pull(key='scraped_jobs', task_ids='scrape_jobs')

        if not scraped_jobs:
            return "No jobs to save"

        with get_db_connection() as conn:
            cursor = conn.cursor()
            companies_saved = 0
            jobs_saved = 0

            for job in scraped_jobs:
                try:
                    company_id = None
                    if job.get('company_name'):
                        company_id = upsert_company(cursor, job)
                        if company_id:
                            companies_saved += 1

                    upsert_job(cursor, job, company_id)
                    jobs_saved += 1

                except Exception as e:
                    logger.error(f"Error saving job {job.get('djinni_id')}: {e}")

            conn.commit()

        return f"Saved {companies_saved} companies and {jobs_saved} jobs"

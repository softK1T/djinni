from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import sys

sys.path.append('/opt/airflow/dags')

from utils.crawler_client import CrawlerClient
from utils.job_parser import parse_job_html, extract_job_urls_from_catalog_page
import psycopg2
import logging

logger = logging.getLogger(__name__)


def extract_catalog_urls(**context):
    crawler = CrawlerClient()

    base_url = "https://djinni.co/jobs/"
    max_pages = 3
    all_job_urls = []

    for page in range(1, max_pages + 1):
        catalog_url = f"{base_url}?page={page}"
        logger.info(f"Processing catalog page {page}: {catalog_url}")

        html = crawler.download_single(catalog_url)
        if not html:
            logger.warning(f"Failed to download page {page}")
            continue

        job_urls = extract_job_urls_from_catalog_page(html)
        all_job_urls.extend(job_urls)

        logger.info(f"Page {page}: found {len(job_urls)} jobs")

    unique_urls = list(set(all_job_urls))
    logger.info(f"Total unique job URLs found: {len(unique_urls)}")

    context['task_instance'].xcom_push(key='job_urls', value=unique_urls[:20])
    return f"Extracted {len(unique_urls)} job URLs (processing first 20)"


def save_catalog_to_db(**context):
    """Step 2: Save catalog URLs to job_catalog table"""
    job_urls = context['task_instance'].xcom_pull(key='job_urls', task_ids='extract_catalog')

    if not job_urls:
        return "No URLs to save"

    conn = psycopg2.connect(
        host='postgres',
        database='djinni_analytics',
        user='djinni_user',
        password='djinni_pass'
    )
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
    conn.close()

    return f"Saved {saved_count} URLs to catalog"


def scrape_jobs(**context):
    job_urls = context['task_instance'].xcom_pull(key='job_urls', task_ids='extract_catalog')

    if not job_urls:
        return "No URLs to scrape"

    crawler = CrawlerClient()
    scraped_jobs = []

    for i, url in enumerate(job_urls[:10], 1):
        logger.info(f"Scraping job {i}/{len(job_urls)}: {url}")

        try:
            html = crawler.download_single(url)
            if not html:
                logger.warning(f"Failed to download {url}")
                continue

            job_data = parse_job_html(html, url)
            if job_data.get('error'):
                logger.error(f"Parsing error for {url}: {job_data['error']}")
                continue

            scraped_jobs.append(job_data)
            logger.info(f"✅ {i}: {job_data.get('title')} at {job_data.get('company_name')}")

        except Exception as e:
            logger.error(f"Error processing {url}: {e}")

    context['task_instance'].xcom_push(key='scraped_jobs', value=scraped_jobs)
    return f"Scraped {len(scraped_jobs)} jobs successfully"


def save_jobs_to_db(**context):
    scraped_jobs = context['task_instance'].xcom_pull(key='scraped_jobs', task_ids='scrape_jobs')

    if not scraped_jobs:
        return "No jobs to save"

    conn = psycopg2.connect(
        host='postgres',
        database='djinni_analytics',
        user='djinni_user',
        password='djinni_pass'
    )
    cursor = conn.cursor()

    companies_saved = 0
    jobs_saved = 0

    for job in scraped_jobs:
        try:
            company_id = None
            if job.get('company_name'):
                cursor.execute("""
                               INSERT INTO companies (name, domain)
                               VALUES (%s, %s)
                               ON CONFLICT (name) DO NOTHING
                               RETURNING id
                               """, (job['company_name'], job.get('domain')))

                result = cursor.fetchone()
                if result:
                    company_id = result[0]
                    companies_saved += 1
                else:
                    cursor.execute("SELECT id FROM companies WHERE name = %s", (job['company_name'],))
                    result = cursor.fetchone()
                    if result:
                        company_id = result[0]

            cursor.execute("""
                           INSERT INTO jobs (djinni_id, url, title, company_id, role,
                                             salary_min, salary_max, salary_currency,
                                             location, remote_info, countries,
                                             experience_required, language_requirements,
                                             domain, product_type, hiring_type,
                                             description, tags,
                                             last_views, last_applications, last_read, last_responded,
                                             is_active)
                           VALUES (%s, %s, %s, %s, %s,
                                   %s, %s, %s,
                                   %s, %s, %s,
                                   %s, %s,
                                   %s, %s, %s,
                                   %s, %s,
                                   %s, %s, %s, %s,
                                   %s)
                           ON CONFLICT (djinni_id) DO UPDATE SET title             = EXCLUDED.title,
                                                                 company_id        = EXCLUDED.company_id,
                                                                 salary_min        = EXCLUDED.salary_min,
                                                                 salary_max        = EXCLUDED.salary_max,
                                                                 last_views        = EXCLUDED.last_views,
                                                                 last_applications = EXCLUDED.last_applications,
                                                                 last_read         = EXCLUDED.last_read,
                                                                 last_responded    = EXCLUDED.last_responded,
                                                                 updated_at        = NOW()
                           """, (
                               job.get('djinni_id'), job.get('url'), job.get('title'), company_id, job.get('role'),
                               job.get('salary_min'), job.get('salary_max'), job.get('salary_currency'),
                               job.get('location'), job.get('remote_info'), job.get('countries'),
                               job.get('experience_required'), job.get('language_requirements'),
                               job.get('domain'), job.get('product_type'), job.get('hiring_type'),
                               job.get('description'), job.get('tags'),
                               job.get('last_views'), job.get('last_applications'),
                               job.get('last_read'), job.get('last_responded'),
                               True
                           ))

            jobs_saved += 1

        except Exception as e:
            logger.error(f"Error saving job {job.get('djinni_id')}: {e}")

    conn.commit()
    conn.close()

    return f"Saved {companies_saved} companies and {jobs_saved} jobs"


def generate_report(**context):
    conn = psycopg2.connect(
        host='postgres',
        database='djinni_analytics',
        user='djinni_user',
        password='djinni_pass'
    )
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM companies")
    companies_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM jobs")
    jobs_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM jobs WHERE created_at >= NOW() - INTERVAL '1 day'")
    jobs_today = cursor.fetchone()[0]

    cursor.execute("""
                   SELECT domain, COUNT(*) as count
                   FROM jobs
                   WHERE domain IS NOT NULL
                   GROUP BY domain
                   ORDER BY count DESC
                   LIMIT 5
                   """)
    top_domains = cursor.fetchall()

    cursor.execute("""
                   SELECT c.name, COUNT(*) as count
                   FROM jobs j
                            JOIN companies c ON j.company_id = c.id
                   GROUP BY c.name
                   ORDER BY count DESC
                   LIMIT 5
                   """)
    top_companies = cursor.fetchall()

    conn.close()

    report = f"""
    📊 DJINNI ETL PIPELINE REPORT
    =================================

    📈 TOTALS:
    - Companies: {companies_count}
    - Jobs: {jobs_count}
    - Jobs added today: {jobs_today}

    🏆 TOP DOMAINS:
    """ + "\n".join([f"    - {domain}: {count} jobs" for domain, count in top_domains]) + f"""

    🏢 TOP COMPANIES:
    """ + "\n".join([f"    - {company}: {count} jobs" for company, count in top_companies]) + f"""

    ✅ Pipeline completed successfully!
    """

    logger.info(report)
    return report


default_args = {
    'owner': 'djinni',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'djinni_etl_pipeline',
    default_args=default_args,
    description='Complete ETL pipeline for djinni.co job scraping',
    schedule_interval=timedelta(hours=6),  # Run every 6 hours
    catchup=False,
    max_active_runs=1,
    tags=['djinni', 'etl', 'jobs']
)

extract_task = PythonOperator(
    task_id='extract_catalog',
    python_callable=extract_catalog_urls,
    dag=dag
)

save_catalog_task = PythonOperator(
    task_id='save_catalog',
    python_callable=save_catalog_to_db,
    dag=dag
)

scrape_task = PythonOperator(
    task_id='scrape_jobs',
    python_callable=scrape_jobs,
    dag=dag
)

save_jobs_task = PythonOperator(
    task_id='save_jobs',
    python_callable=save_jobs_to_db,
    dag=dag
)

report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag
)

extract_task >> save_catalog_task >> scrape_task >> save_jobs_task >> report_task

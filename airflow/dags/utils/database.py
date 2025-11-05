import psycopg2
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': 'postgres',
    'database': 'djinni_analytics',
    'user': 'djinni_user',
    'password': 'djinni_pass'
}


@contextmanager
def get_db_connection():
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        yield conn
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Database error: {e}")
        raise
    finally:
        if conn:
            conn.close()


def upsert_company(cursor, company_data):
    cursor.execute("""
                   INSERT INTO companies (name, domain)
                   VALUES (%s, %s)
                   ON CONFLICT (name) DO NOTHING
                   RETURNING id
                   """, (company_data.get('company_name'), company_data.get('domain')))

    result = cursor.fetchone()
    if result:
        return result[0]

    cursor.execute("SELECT id FROM companies WHERE name = %s", (company_data.get('company_name'),))
    result = cursor.fetchone()
    return result[0] if result else None


def upsert_job(cursor, job_data, company_id):
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
                       job_data.get('djinni_id'), job_data.get('url'), job_data.get('title'),
                       company_id, job_data.get('role'),
                       job_data.get('salary_min'), job_data.get('salary_max'), job_data.get('salary_currency'),
                       job_data.get('location'), job_data.get('remote_info'), job_data.get('countries'),
                       job_data.get('experience_required'), job_data.get('language_requirements'),
                       job_data.get('domain'), job_data.get('product_type'), job_data.get('hiring_type'),
                       job_data.get('description'), job_data.get('tags'),
                       job_data.get('last_views'), job_data.get('last_applications'),
                       job_data.get('last_read'), job_data.get('last_responded'),
                       True
                   ))

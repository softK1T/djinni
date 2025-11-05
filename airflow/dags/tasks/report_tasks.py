import logging
from utils.database import get_db_connection

logger = logging.getLogger(__name__)


class ReportTasks:
    def generate_report(self, **context):
        with get_db_connection() as conn:
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

        report = f"""
        DJINNI ETL PIPELINE REPORT
        =================================

        TOTALS:
        - Companies: {companies_count}
        - Jobs: {jobs_count}
        - Jobs added today: {jobs_today}

        TOP DOMAINS:
        """ + "\n".join([f"    - {domain}: {count} jobs" for domain, count in top_domains]) + f"""

        TOP COMPANIES:
        """ + "\n".join([f"    - {company}: {count} jobs" for company, count in top_companies]) + f"""

        Pipeline completed successfully.
        """

        logger.info(report)
        return report

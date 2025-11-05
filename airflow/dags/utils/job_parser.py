import json
import re
import logging
from typing import Any, Dict, Optional
from lxml import html

logger = logging.getLogger(__name__)

XPATHS = {
    "title": "//ol/following-sibling::h1",
    "description": "//div[contains(@class, 'job-post__description')]",
    "company_links": "//a[contains(@href, '/companies/')]",
    "breadcrumbs": "//li[contains(@class,'breadcrumb-item')]",
    "language": "//li[.//*[contains(text(),'Beginner') or contains(text(), 'Intermediate') or contains(text(), 'Advanced') or contains(text(), 'Fluent')]]",
    "experience": "//li[.//*[contains(text(),'experience')]]",
    "remote": "//li[.//*[contains(text(),'Remote')]]",
    "countries": "//li[.//*[contains(text(),'Countries')]]/*[not(div)]",
    "location": "//li[.//*[contains(@class,'geo')]]",
    "role": "//ul[.//*[contains(@class,'folder')]]/li[1]",
    "tags": "//ul[.//*[contains(@class,'folder')]]/li[2]",
    "domain": "//li[.//*[contains(text(),'Domain')]]",
    "product": "//li[.//*[contains(@class,'briefcase')]]",
    "hiring": "//li[.//*[contains(@class,'journal')]]",
    "jsonld": "//script[contains(@type, 'application/ld+json')]/text()",
    "stats": {
        "views": "//*[contains(text(), 'view')]",
        "applications": "//div[@id='rate-current']//div[contains(text(), 'application')]",
        "read": "//*[contains(text(), 'read')]",
        "responded": "//*[contains(text(), 'responded')]"
    }
}


def text_or_none(el: Optional[Any]) -> Optional[str]:
    if el is None:
        return None
    if isinstance(el, str):
        return el.strip() or None
    try:
        txt = el.text_content().strip()
        return txt or None
    except Exception:
        return None


def first_text(doc, xp: str) -> Optional[str]:
    try:
        found = doc.xpath(xp)
        if not found:
            return None
        node = found[0] if isinstance(found, list) else found
        return text_or_none(node)
    except Exception:
        return None


def extract_djinni_id_from_url(url: str) -> Optional[int]:
    try:
        match = re.search(r'/jobs/(\d+)-', url)
        return int(match.group(1)) if match else None
    except Exception:
        return None


def extract_company_name(doc) -> Optional[str]:
    try:
        company_links = doc.xpath(XPATHS["company_links"])
        if company_links:
            company_name = text_or_none(company_links[0])
            if company_name:
                return company_name

        breadcrumbs = doc.xpath(XPATHS["breadcrumbs"])
        if len(breadcrumbs) >= 2:
            company_name = text_or_none(breadcrumbs[1])
            if company_name:
                return company_name

        return None
    except Exception as e:
        logger.error(f"Error extracting company: {e}")
        return None


def extract_last_publication_stats(doc) -> Dict[str, Optional[int]]:
    def to_int(s: Optional[str]) -> Optional[int]:
        if not s:
            return None
        m = re.search(r"(\d+)", s.replace(",", ""))
        return int(m.group(1)) if m else None

    def to_percentage(s: Optional[str]) -> Optional[int]:
        if not s:
            return None
        m = re.search(r"(\d+)%", s.replace(",", ""))
        return int(m.group(1)) if m else None

    stats = {
        "last_views": None,
        "last_applications": None,
        "last_read": None,
        "last_responded": None
    }

    try:
        for stat_type, xpath in XPATHS["stats"].items():
            elements = doc.xpath(xpath)

            for element in elements:
                text = text_or_none(element)
                if not text:
                    continue

                if stat_type == "views" and "view" in text.lower():
                    stats["last_views"] = to_int(text)
                    logger.info(f"Found views: {text} -> {stats['last_views']}")

                elif stat_type == "applications" and "application" in text.lower():
                    stats["last_applications"] = to_int(text)
                    logger.info(f"Found applications: {text} -> {stats['last_applications']}")

                elif stat_type == "read" and "read" in text.lower():
                    stats["last_read"] = to_percentage(text)
                    logger.info(f"Found read: {text} -> {stats['last_read']}%")

                elif stat_type == "responded" and "responded" in text.lower():
                    stats["last_responded"] = to_percentage(text)
                    logger.info(f"Found responded: {text} -> {stats['last_responded']}%")

        if not any(stats.values()):
            logger.info("No stats found with specific selectors, trying broader search...")

            all_text = doc.text_content() if hasattr(doc, 'text_content') else ""

            patterns = {
                "last_views": r"(\d+)\s*view",
                "last_applications": r"(\d+)\s*application",
                "last_read": r"(\d+)%\s*read",
                "last_responded": r"(\d+)%\s*responded"
            }

            for stat_key, pattern in patterns.items():
                match = re.search(pattern, all_text, re.IGNORECASE)
                if match:
                    stats[stat_key] = int(match.group(1))
                    logger.info(f"Found {stat_key} via pattern: {match.group(0)} -> {stats[stat_key]}")

    except Exception as e:
        logger.error(f"Error extracting last publication stats: {e}")

    return stats


def parse_job_html(html_text: str, job_url: str = "") -> Dict[str, Any]:
    try:
        doc = html.fromstring(html_text)

        data = {
            'djinni_id': extract_djinni_id_from_url(job_url),
            'url': job_url,
            'title': first_text(doc, XPATHS["title"]),
            'company_name': extract_company_name(doc),
            'description': first_text(doc, XPATHS["description"]),
            'location': first_text(doc, XPATHS["location"]),
            'remote_info': first_text(doc, XPATHS["remote"]),
            'countries': first_text(doc, XPATHS["countries"]),
            'experience_required': first_text(doc, XPATHS["experience"]),
            'language_requirements': first_text(doc, XPATHS["language"]),
            'role': first_text(doc, XPATHS["role"]),
            'tags': first_text(doc, XPATHS["tags"]),
            'domain': (first_text(doc, XPATHS["domain"]) or "").replace("Domain: ", ""),
            'product_type': first_text(doc, XPATHS["product"]),
            'hiring_type': first_text(doc, XPATHS["hiring"]),
            'is_active': True
        }

        last_stats = extract_last_publication_stats(doc)
        data.update(last_stats)

        try:
            jsonld_scripts = doc.xpath(XPATHS["jsonld"])
            for script in jsonld_scripts:
                jsonld_data = json.loads(script)
                salary_info = jsonld_data.get('estimatedSalary', {})
                if salary_info:
                    data['salary_min'] = salary_info.get('minValue')
                    data['salary_max'] = salary_info.get('maxValue')
                    data['salary_currency'] = salary_info.get('currency', 'USD')
                    break
        except:
            data['salary_min'] = None
            data['salary_max'] = None
            data['salary_currency'] = 'USD'

        logger.info(f"Successfully parsed: {data.get('title')} at {data.get('company_name')}")
        return data

    except Exception as e:
        logger.error(f"Error parsing job {job_url}: {e}")
        return {'error': str(e), 'url': job_url}


def extract_job_urls_from_catalog_page(html_text: str) -> list[str]:
    try:
        doc = html.fromstring(html_text)
        job_links = doc.xpath("//a[contains(@href, '/jobs/') and contains(@href, '-')]/@href")

        job_urls = []
        for link in job_links:
            if '/jobs/' in link and '-' in link:
                if link.startswith('/'):
                    full_url = f"https://djinni.co{link}"
                else:
                    full_url = link
                job_urls.append(full_url)

        unique_urls = list(set(job_urls))
        logger.info(f"Extracted {len(unique_urls)} job URLs from catalog page")
        return unique_urls

    except Exception as e:
        logger.error(f"Error extracting job URLs: {e}")
        return []


def test_parser():
    from crawler_client import CrawlerClient

    crawler = CrawlerClient()
    test_url = "https://djinni.co/jobs/725028-senior-go-engineer/"

    print(f"🔍 Testing final parser: {test_url}")

    html = crawler.download_single(test_url)
    if not html:
        print("❌ Download failed")
        return

    result = parse_job_html(html, test_url)

    print(result)
    return result


if __name__ == "__main__":
    test_parser()

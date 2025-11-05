import json
import re
import logging
from typing import Any, Dict, Optional, List
from lxml import html

logger = logging.getLogger(__name__)


def extract_jsonld_data(doc) -> Dict[str, Any]:
    try:
        jsonld_scripts = doc.xpath("//script[contains(@type, 'application/ld+json')]/text()")
        for script in jsonld_scripts:
            data = json.loads(script)
            if data.get('@type') == 'JobPosting':
                result = {
                    'title': data.get('title'),
                    'description': data.get('description'),
                    'djinni_id': data.get('identifier'),
                    'url': data.get('url'),
                    'date_posted': data.get('datePosted', '').split('T')[0] if data.get('datePosted') else None,
                    'employment_type': data.get('employmentType'),
                    'industry': data.get('industry'),
                }

                if data.get('estimatedSalary'):
                    salary = data['estimatedSalary']
                    result['salary_min'] = salary.get('minValue')
                    result['salary_max'] = salary.get('maxValue')
                    result['salary_currency'] = salary.get('currency', 'USD')

                if data.get('experienceRequirements'):
                    months = data['experienceRequirements'].get('monthsOfExperience')
                    if months:
                        result[
                            'experience_required'] = f"{months / 12:.1f} years" if months >= 12 else f"{int(months)} months"

                if data.get('hiringOrganization'):
                    org = data['hiringOrganization']
                    result['company_name'] = org.get('name')
                    result['company_website'] = org.get('sameAs')

                return result
    except:
        pass
    return {}


def safe_xpath_text(doc, xpath: str) -> Optional[str]:
    try:
        found = doc.xpath(xpath)
        if found:
            return found[0].text_content().strip() or None
    except:
        pass
    return None


def extract_skills(doc) -> Optional[str]:
    skills = []
    try:
        skill_rows = doc.xpath("//ul[@id='job_extra_info']//table//tr")
        for row in skill_rows:
            skill_cells = row.xpath(".//td")
            if len(skill_cells) >= 2:
                skill_name = skill_cells[0].text_content().strip()
                skill_experience = skill_cells[1].text_content().strip()
                if skill_name:
                    if skill_experience:
                        skills.append(f"{skill_name}: {skill_experience}")
                    else:
                        skills.append(skill_name)
    except:
        pass
    return ", ".join(skills) if skills else None


def extract_stats(doc) -> Dict[str, Optional[int]]:
    stats = {}
    try:
        text = doc.text_content()
        if m := re.search(r'(\d+)\s*view', text, re.I):
            stats['last_views'] = int(m.group(1))
        if m := re.search(r'(\d+)\s*application', text, re.I):
            stats['last_applications'] = int(m.group(1))
        if m := re.search(r'(\d+)%\s*read', text, re.I):
            stats['last_read'] = int(m.group(1))
        if m := re.search(r'(\d+)%\s*responded', text, re.I):
            stats['last_responded'] = int(m.group(1))
    except:
        pass
    return stats


def extract_company_djinni_url(doc) -> Optional[str]:
    try:
        company_link = doc.xpath("//a[@data-analytics='company_page']")
        if company_link:
            href = company_link[0].get('href')
            if href and href.startswith('/'):
                return f"https://djinni.co{href}"
    except:
        pass
    return None


def extract_xpath_only_fields(doc) -> Dict[str, Any]:
    result = {
        'location': safe_xpath_text(doc, "//li[.//*[contains(@class,'geo')]]"),
        'remote_info': safe_xpath_text(doc, "//li[.//*[contains(text(),'Remote')]]"),
        'role': safe_xpath_text(doc, "//ul[@id='job_extra_info']//div[@class='fw-medium']"),
        'language_requirements': safe_xpath_text(doc,
                                                 "//li[.//*[contains(text(),'Beginner') or contains(text(), 'Intermediate')]]"),
        'product_type': safe_xpath_text(doc, "//div[contains(span/@class, 'briefcase ')]/following::div"),
        'skills': extract_skills(doc),
        'djinni_company_url': extract_company_djinni_url(doc),
        'countries': safe_xpath_text(doc, "//span[contains(@class,'location-text')]"),
    }

    result.update(extract_stats(doc))

    return {k: v for k, v in result.items() if v is not None}


def parse_job_html(html_text: str, job_url: str = "") -> Dict[str, Any]:
    try:
        doc = html.fromstring(html_text)

        data = extract_jsonld_data(doc)
        xpath_data = extract_xpath_only_fields(doc)

        for key, value in xpath_data.items():
            if key not in data:
                data[key] = value

        data.setdefault('is_active', True)
        data.setdefault('salary_currency', 'USD')

        if not data.get('djinni_id') and job_url:
            if m := re.search(r'/jobs/(\d+)-', job_url):
                data['djinni_id'] = int(m.group(1))

        data.setdefault('url', job_url)

        return data

    except Exception as e:
        logger.error(f"Parse error {job_url}: {e}")
        return {'error': str(e), 'url': job_url}


def extract_job_urls_from_catalog_page(html_text: str) -> List[str]:
    try:
        doc = html.fromstring(html_text)
        links = doc.xpath("//a[contains(@class, 'job-item__title-link')]/@href")
        urls = []
        for link in links:
            if '/jobs/' in link:
                url = f"https://djinni.co{link}" if link.startswith('/') else link
                urls.append(url)
        return list(set(urls))
    except:
        return []


def test_parser():
    import requests
    from pprint import pprint

    test_urls = [
        "https://djinni.co/jobs/781469-senior-devops-engineer/",
        "https://djinni.co/jobs/725028-senior-go-engineer/",
    ]

    for url in test_urls:
        print(f"\n{'=' * 60}")
        print(f"🔍 TESTING: {url}")
        print('=' * 60)

        try:
            response = requests.get(url, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            })
            response.raise_for_status()
            html_content = response.text
            doc = html.fromstring(html_content)

            print("📄 JSON-LD DATA:")
            jsonld_data = extract_jsonld_data(doc)
            if jsonld_data:
                pprint(jsonld_data, width=80)
            else:
                print("   ❌ No JSON-LD found")

            print("\n🔧 XPATH-ONLY DATA:")
            xpath_data = extract_xpath_only_fields(doc)
            if xpath_data:
                pprint(xpath_data, width=80)
            else:
                print("   ❌ No XPath data found")

            print("\n🎯 FINAL RESULT:")
            result = parse_job_html(html_content, url)
            if 'error' in result:
                print(f"   ❌ ERROR: {result['error']}")
            else:
                pprint(result, width=80)

                print(f"\n📊 SUMMARY:")
                print(f"   Title: {result.get('title', 'N/A')}")
                print(f"   Company: {result.get('company_name', 'N/A')}")
                print(f"   Company Website: {result.get('company_website', 'N/A')}")
                print(
                    f"   Salary: {result.get('salary_min', 'N/A')}-{result.get('salary_max', 'N/A')} {result.get('salary_currency', 'N/A')}")
                print(f"   Skills: {result.get('skills', 'N/A')}")
                print(f"   Product Type: {result.get('product_type', 'N/A')}")
                print(f"   Djinni Company URL: {result.get('djinni_company_url', 'N/A')}")
                print(
                    f"   Stats: views={result.get('last_views', 'N/A')}, apps={result.get('last_applications', 'N/A')}")

        except Exception as e:
            print(f"❌ FAILED: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("🚀 Starting parser test...")
    test_parser()

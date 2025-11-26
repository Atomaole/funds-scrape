import csv
import re
import random, time
from datetime import datetime
from typing import List, Dict, Any, Optional, Set
import requests
from io import BytesIO
import pdfplumber
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.service import Service
from webdriver_manager.firefox import GeckoDriverManager
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, WebDriverException

ISIN_RE = re.compile(r"\b([A-Z]{2}[A-Z0-9]{9}[0-9])\b")
NAME_LABEL_RES = [
    re.compile(r"(?:Fund\s*Name|Master\s*Fund\s*Name)\s*[:：]\s*(.+)", re.IGNORECASE),
    re.compile(r"(?:ชื่อกองทุน(?:\s*หลัก)?|ชื่อกองทุนแม่)\s*[:：]\s*(.+)", re.IGNORECASE),
]
def _clean_txt(s: str) -> str:
    if not s:
        return ""
    s = s.replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()
def _norm_isin(s: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", (s or "").upper())
def extract_first_number(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    s = s.replace(",", "")
    m = re.search(r"[-+]?\d+(?:\.\d+)?", s)
    return m.group(0) if m else ""

def polite_sleep():
    t = random.uniform(1.1, 1.6) 
    time.sleep(t)
# ------------------ CONFIG: FINNOMENA ------------------

LIST_BASE_URL = "https://www.finnomena.com/fund/filter?size=1000&page={page}"
MAX_LIST_PAGES = 6
HEADLESS = False
PAGELOAD_TIMEOUT = 30
OUTPUT_CSV = "finnomena_funds.csv"
OUTPUT_HOLDINGS_CSV = "finnomena_holdings.csv"
LIMIT_FUNDS: Optional[int] = None 
MAX_PROFILE_RETRY = 2 
OUTPUT_CODES_CSV = "finnomena_codes.csv"
# ------------------ BASIC UTILS ------------------

def log(msg: str):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

def make_driver(headless: bool = HEADLESS):
    options = webdriver.FirefoxOptions()
    if headless:
        options.add_argument("-headless")
    options.set_preference("dom.disable_open_during_load", True)
    options.set_preference("dom.popup_maximum", 0)
    options.set_preference("dom.popup_allowed_events", "")
    options.set_preference("dom.webnotifications.enabled", False)
    options.set_preference("dom.push.enabled", False)
    options.set_preference("permissions.default.desktop-notification", 2)
    options.set_preference("privacy.trackingprotection.enabled", True)
    options.set_preference("privacy.trackingprotection.pbmode.enabled", True)
    return webdriver.Firefox(service=Service(GeckoDriverManager().install()), options=options)

def unlock_scroll(driver):
    js = """
    try {
      document.documentElement.style.overflow = 'auto';
      document.body.style.overflow = 'auto';
      document.documentElement.style.height = 'auto';
      document.body.style.height = 'auto';
      for (const el of [document.documentElement, document.body]) {
        el.classList.remove('modal-open','no-scroll','overflow-hidden','is-locked');
        el.style.overflow = 'auto';
        el.style.position = 'static';
      }
      document.querySelectorAll('.modal-backdrop,.overlay,[class*="overlay"],[id*="overlay"],[class*="backdrop"],[id*="backdrop"]').forEach(el=>{
        try{ el.remove(); }catch(e){ el.style.display='none'; }
      });
    } catch(e) {}
    """
    driver.execute_script(js)

def wait_visible(driver, by, value, timeout=15):
    return WebDriverWait(driver, timeout).until(
        EC.visibility_of_element_located((by, value))
    )

def find_text_by_xpath(driver, xpath: str) -> str:
    try:
        el = driver.find_element(By.XPATH, xpath)
        return (el.text or "").strip()
    except Exception:
        return ""

def find_first_text(driver, xpaths: List[str]) -> str:
    for xp in xpaths:
        t = find_text_by_xpath(driver, xp)
        if t:
            return t
    return ""

def scrape_fund_profile_with_retry(driver, url: str, max_attempts: int = MAX_PROFILE_RETRY) -> Dict[str, Any]:
    last_err = ""
    for attempt in range(1, max_attempts + 1):
        log(f">> attempt {attempt}/{max_attempts}")
        try:
            row = scrape_fund_profile(driver, url)
            err = (row.get("error") or "").lower()
            if err.startswith("load_fail"):
                last_err = err
                log(f"    >> load_fail, will retry")
                polite_sleep()
                continue
            return row

        except Exception as e:
            last_err = f"unexpected: {e}"
            log(f">> unexpected error: {e}  (will retry)")
            polite_sleep()

    return {
        "fund_url": url,
        "scraped_at": datetime.now().isoformat(timespec="seconds"),
        "error": f"retry_failed: {last_err}",
        "_pdf_holdings": [],
        "_pdf_codes": [],
    }

# ------------------ LIST PAGE: ดึง URL กองทุน ------------------

def get_all_fund_profile_urls(driver, max_pages: int = MAX_LIST_PAGES) -> List[str]:
    codes: Set[str] = set()

    for page in range(1, max_pages + 1):
        list_url = LIST_BASE_URL.format(page=page)
        log(f"เปิดหน้า list page={page} -> {list_url}")
        driver.get(list_url)
        unlock_scroll(driver)
        try:
            WebDriverWait(driver, 60).until(
                EC.presence_of_element_located(
                    (By.XPATH, "//a[contains(@href, '/fund/')]")
                )
            )
        except TimeoutException:
            log(f"page {page}:")
            continue

        links = driver.find_elements(By.XPATH, "//a[contains(@href, '/fund/')]")
        if not links:
            log(f"page {page}: ไม่เจอ fund หยุด")
            break

        before = len(codes)
        for a in links:
            href = a.get_attribute("href") or ""
            m = re.search(r"/fund/([A-Z0-9\-]+)/?$", href)
            if m:
                codes.add(m.group(1))

        added = len(codes) - before
        log(f"page {page}: เจอ code ใหม่ {added} ตัว (สะสม {len(codes)})")

        if added <= 0:
            log("ไม่มีแล้วหยุด")
            break

    urls = [f"https://www.finnomena.com/fund/{c}" for c in sorted(codes)]
    log(f"รวมลิงก์โปรไฟล์ได้ {len(urls)} รายการ")

    if LIMIT_FUNDS:
        urls = urls[:LIMIT_FUNDS]
        log(f"จำกัดตัวอย่างเหลือ {len(urls)} ลิงก์แรก")

    return urls
# ------------------ DETAIL-BLOCK HELPER ------------------

def get_detail_dict(driver) -> Dict[str, str]:
    details: Dict[str, str] = {}
    rows = driver.find_elements(By.CSS_SELECTOR, ".fund-detail .detail-row")
    for row in rows:
        try:
            left_el = row.find_element(By.CSS_SELECTOR, ".left")
            right_el = row.find_element(By.CSS_SELECTOR, ".right")
        except Exception:
            continue
        left = (left_el.text or "").strip()
        right = (right_el.text or "").strip()
        if left:
            details[left] = right
    return details


# ------------------ PDF HELPERS ------------------

def fetch_pdf_bytes(url: str, referer: Optional[str] = None, timeout: int = 25) -> Optional[bytes]:
    try:
        headers = {
            "User-Agent": "Mozilla/5.0",
        }
        if referer:
            headers["Referer"] = referer
        r = requests.get(url, headers=headers, timeout=timeout)
        if r.status_code == 200 and r.content:
            return r.content
    except Exception:
        pass
    return None

def extract_all_isins_from_pdf_bytes(pdf_bytes: bytes) -> List[str]:
    if not pdf_bytes:
        return []
    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            texts = []
            for p in pdf.pages:
                t = p.extract_text() or ""
                if t:
                    texts.append(t)
        full = "\n".join(texts)
        if not full.strip():
            return []

        found: Set[str] = set()
        for m in ISIN_RE.finditer(full):
            found.add(_norm_isin(m.group(1)))
        compact = re.sub(r"[\s\u200b\u00ad\-]+", "", full)
        for m in ISIN_RE.finditer(compact):
            found.add(_norm_isin(m.group(1)))

        return sorted(found)

    except Exception:
        return []

def extract_all_bloomberg_codes_from_pdf_bytes(pdf_bytes: bytes) -> str:
    if not pdf_bytes:
        return []
    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            texts = []
            for p in pdf.pages:
                t = p.extract_text() or ""
                if t:
                    texts.append(t)
        full = "\n".join(texts)
        if not full.strip():
            return []
        codes: List[str] = []
        for line in full.splitlines():
            line_clean = _clean_txt(line)
            if not line_clean:
                continue
            if "bloomberg" not in line_clean.lower():
                continue
            m = re.search(
                r"Bloomberg\s*(?:Code|Ticker)?\s*[:：]\s*([A-Z0-9]{3,12}\s+[A-Z]{2})",
                line_clean,
                flags=re.IGNORECASE,
            )
            if m:
                codes.append(m.group(1).strip())
                continue
            m2 = re.search(r"\b([A-Z0-9]{3,12}\s+[A-Z]{2})\b", line_clean)
            if m2:
                codes.append(m2.group(1).strip())
        seen = set()
        uniq = []
        for c in codes:
            if c not in seen:
                seen.add(c)
                uniq.append(c)
        return uniq
    except Exception:
        return []

def _extract_beta_from_lines(lines: List[str]) -> str:
    n = len(lines)
    for i in range(n):
        line = _clean_txt(lines[i])
        if not line:
            continue
        lower = line.lower()
        if ("sharpe ratio" in lower) and re.search(r"[-+]?\d+(?:\.\d+)?", line):
            for j in range(i, min(i + 10, n)):
                l2 = _clean_txt(lines[j])
                if not l2:
                    continue
                low2 = l2.lower()
                if "beta" not in low2:
                    continue
                if "n/a" in low2:
                    return ""
                m = re.search(
                    r"(?i)\bbeta\b[^\d\-+]{0,10}([-+]?\d+(?:\.\d+)?)(?:\s*%?\s*)$",
                    l2
                )
                if m:
                    return m.group(1).strip()
            break
    for raw in lines:
        line = _clean_txt(raw)
        if not line:
            continue
        lower = line.lower()
        if "beta" not in lower:
            continue
        if "ระดับและทิศทาง" in lower or "อัตราผลตอบแทน" in lower:
            continue
        if "n/a" in lower:
            return ""

        m = re.search(
            r"(?i)\bbeta\b[^\d\-+]{0,10}([-+]?\d+(?:\.\d+)?)(?:\s*%?\s*)$",
            line
        )
        if m:
            return m.group(1).strip()

    return ""


def extract_beta_from_pdf_bytes(
    pdf_bytes: bytes,
    fund_hints: Optional[List[str]] = None
) -> str:
    if not pdf_bytes:
        return ""

    fund_hints = fund_hints or []
    norm_hints: List[str] = []
    for h in fund_hints:
        if not h:
            continue
        h = h.strip()
        if not h:
            continue
        norm_hints.append(h.lower())
        norm_hints.append(re.sub(r"[\s\-]+", "", h.lower()))

    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            page_texts: List[str] = []
            for p in pdf.pages:
                t = p.extract_text() or ""
                page_texts.append(t)

        if not page_texts:
            return ""
        candidate_pages: Set[int] = set()

        if norm_hints:
            for idx, txt in enumerate(page_texts):
                low = txt.lower()
                low_nospace = re.sub(r"\s+", "", low)
                for h in norm_hints:
                    if h in low or h in low_nospace:
                        candidate_pages.add(idx)
                        if idx > 0:
                            candidate_pages.add(idx - 1)
                        if idx + 1 < len(page_texts):
                            candidate_pages.add(idx + 1)
                        break
        if candidate_pages:
            lines: List[str] = []
            for idx in sorted(candidate_pages):
                for ln in (page_texts[idx] or "").splitlines():
                    lines.append(ln)

            beta = _extract_beta_from_lines(lines)
            if beta:
                return beta

        all_lines: List[str] = []
        for txt in page_texts:
            for ln in (txt or "").splitlines():
                all_lines.append(ln)

        return _extract_beta_from_lines(all_lines)

    except Exception:
        return ""

def scrape_top_holdings_from_portfolio_page(
    driver,
    fund_url: str,
    fund_code: str
) -> List[Dict[str, Any]]:
    holdings: List[Dict[str, Any]] = []

    portfolio_url = fund_url.rstrip("/") + "/portfolio"
    try:
        driver.get(portfolio_url)
        unlock_scroll(driver)
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located(
                (By.XPATH, "//*[contains(., 'TOP 5 Holding')]")
            )
        )
    except Exception as e:
        log(f"  [HOLDING] load_fail: {e}")
        return holdings

    scraped_at = datetime.now().isoformat(timespec="seconds")
    as_of_raw = ""
    try:
        container = driver.find_element(
            By.XPATH,
            "//*[contains(., 'TOP 5 Holding')]/ancestor-or-self::*[1]"
        )
        candidates = container.find_elements(By.XPATH, ".//p | .//span")
        for el in candidates:
            t = (el.text or "").strip()
            if not t:
                continue
            if re.search(r"\d{1,2}\s*[ก-ฮ]\.", t) or re.search(r"\d{1,2}/\d{1,2}/\d{2,4}", t):
                as_of_raw = t
                break
    except Exception:
        pass
    try:
        items = driver.find_elements(By.CSS_SELECTOR, ".top-holding-item")
    except Exception:
        items = []

    for item in items:
        try:
            name = ""
            try:
                name_el = item.find_element(By.CSS_SELECTOR, ".title")
                name = (name_el.text or "").strip()
            except Exception:
                try:
                    name_el = item.find_element(By.XPATH, ".//p[last()]")
                    name = (name_el.text or "").strip()
                except Exception:
                    name = ""
            weight = ""
            pct_el = None
            try:
                pct_el = item.find_element(By.CSS_SELECTOR, ".percent")
            except Exception:
                try:
                    pct_el = item.find_element(By.XPATH, ".//*[contains(@class,'percent')]")
                except Exception:
                    pct_el = None

            if pct_el:
                pct_txt = (pct_el.text or "").strip().replace(",", "")
                m = re.search(r"[-+]?\d+(?:\.\d+)?", pct_txt)
                if m:
                    weight = m.group(0)

            if not name or not weight:
                continue

            holdings.append({
                "scraped_at": scraped_at,
                "fund_code": fund_code,
                "fund_url": fund_url,
                "holding_name": name,
                "weight_pct": weight,
                "as_of_raw": as_of_raw,
            })
        except Exception:
            continue
    return holdings

# ------------------ PARSE HELPERS ------------------

def parse_percent_str(s: str) -> str:
    if not s:
        return ""
    t = _clean_txt(s)
    if "N/A" in t.upper():
        return ""
    t = t.replace("%", "")
    m = re.search(r"[-+]?\d+(?:[.,]\d+)?", t)
    if not m:
        return ""
    num = m.group(0).replace(",", "")
    return num.strip()

THAI_MONTH_MAP = {
    "ม.ค.": 1, "ม.ค": 1, "มกราคม": 1,
    "ก.พ.": 2, "ก.พ": 2, "กุมภาพันธ์": 2,
    "มี.ค.": 3, "มี.ค": 3, "มีนาคม": 3,
    "เม.ย.": 4, "เม.ย": 4, "เมษายน": 4,
    "พ.ค.": 5, "พ.ค": 5, "พฤษภาคม": 5,
    "มิ.ย.": 6, "มิ.ย": 6, "มิถุนายน": 6,
    "ก.ค.": 7, "ก.ค": 7, "กรกฎาคม": 7,
    "ส.ค.": 8, "ส.ค": 8, "สิงหาคม": 8,
    "ก.ย.": 9, "ก.ย": 9, "กันยายน": 9,
    "ต.ค.": 10, "ต.ค": 10, "ตุลาคม": 10,
    "พ.ย.": 11, "พ.ย": 11, "พฤศจิกายน": 11,
    "ธ.ค.": 12, "ธ.ค": 12, "ธันวาคม": 12,
}
def parse_thai_finnomena_date(s: str) -> str:
    if not s:
        return ""
    s = _clean_txt(s)
    s = re.sub(r"(ข้อมูล\s*ณ\s*วันที่|ณ\s*วันที่|ข้อมูล ณ วันที่|วันที่)", "", s).strip()
    m = re.search(r"(\d{1,2})\s+([^\d\s]+)\s*(\d{2,4})", s)
    if m:
        day_str, month_str, year_str = m.groups()
        month_str = month_str.strip()
        month = THAI_MONTH_MAP.get(month_str)
        if month:
            try:
                day = int(day_str)
                year = int(year_str)
            except ValueError:
                return ""
            if year < 100: 
                year = (2500 + year) - 543
            elif year > 2400:
                year = year - 543

            try:
                d = datetime(year, month, day)
                return d.strftime("%Y-%m-%d")
            except ValueError:
                return ""
    m2 = re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})", s)
    if m2:
        d_str, m_str, y_str = m2.groups()
        try:
            day = int(d_str)
            month = int(m_str)
            year = int(y_str)
        except ValueError:
            return ""

        if year < 100:
            year = (2500 + year) - 543
        elif year > 2400:
            year = year - 543

        try:
            d = datetime(year, month, day)
            return d.strftime("%Y-%m-%d")
        except ValueError:
            return ""
    return ""
    
def extract_nav_and_date(driver, fund_code: str, url: str) -> tuple[str, str]:
    nav_value = ""
    nav_date_raw = ""

    deadline = time.time() + 25 

    while time.time() < deadline:
        nav_box = None
        try:
            nav_box = driver.find_element(By.CSS_SELECTOR, ".fund-nav-percent")
        except Exception:
            time.sleep(0.5)
            continue
        try:
            for el in nav_box.find_elements(By.TAG_NAME, "h3"):
                t = (el.text or "").strip()
                if not t:
                    continue
                if not re.search(r"\d", t):
                    continue
                nav_value = extract_first_number(t)
                if nav_value:
                    break
        except Exception:
            pass
        try:
            for el in nav_box.find_elements(By.CSS_SELECTOR, "p, span"):
                t = (el.text or "").strip()
                if not t:
                    continue
                if ("ข้อมูล ณ วันที่" in t) or re.search(r"\d{1,2}\s+[ก-ฮ]", t):
                    nav_date_raw = t
                    break
        except Exception:
            pass
        if nav_value and nav_date_raw:
            break
        time.sleep(0.5)

    if not nav_value:
        log(f"[NAV] ไม่มี NAV value -> {fund_code} {url}")
    if not nav_date_raw:
        log(f"[NAV] ไม่มี NAV date  -> {fund_code} {url}")

    return nav_value, nav_date_raw

def _extract_fee_pair_by_label(driver, label_keywords: List[str]) -> tuple[str, str]:
    for kw in label_keywords:
        try:
            row = driver.find_element(
                By.XPATH,
                (
                    "//div[contains(@class,'fin-row') "
                    "and .//*[contains(normalize-space(), '{kw}')]]"
                ).format(kw=kw)
            )
        except Exception:
            continue

        max_fee = ""
        actual_fee = ""

        subrows = row.find_elements(By.CSS_SELECTOR, ".fee-border .fin-row")
        if subrows:
            for idx, sub in enumerate(subrows):
                header_txt = ""
                try:
                    header_txt = (sub.find_element(
                        By.CSS_SELECTOR, ".fee-header-sm"
                    ).text or "").strip()
                except Exception:
                    pass
                fee_txt = ""
                try:
                    fee_txt = (sub.find_element(
                        By.CSS_SELECTOR, ".fee-text"
                    ).text or "").strip()
                except Exception:
                    pass

                val = parse_percent_str(fee_txt)
                if not val:
                    continue
                header_norm = header_txt.replace(" ", "")

                if "ตามหนังสือชี้ชวน" in header_norm:
                    max_fee = val
                elif "เก็บจริง" in header_norm:
                    actual_fee = val
                else:
                    if idx == 0 and not max_fee:
                        max_fee = val
                    elif idx == 1 and not actual_fee:
                        actual_fee = val
            return max_fee, actual_fee
        fee_elems = row.find_elements(By.CSS_SELECTOR, ".fee-text")
        vals: List[str] = []
        for el in fee_elems:
            txt = (el.text or "").strip()
            if not txt:
                continue
            if not re.search(r"\d", txt):
                continue
            vals.append(parse_percent_str(txt))

        if len(vals) >= 2:
            return vals[0], vals[1]
        elif len(vals) == 1:
            return vals[0], ""

    return "", ""


def scrape_fees_from_fee_page(driver, fund_url: str, data: Dict[str, Any]) -> None:
    fee_url = fund_url.rstrip("/") + "/fee"
    log(f"  เปิดหน้า fee -> {fee_url}")

    try:
        driver.get(fee_url)
        unlock_scroll(driver)
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".fee-text"))
        )
    except Exception as e:
        log(f"  [FEE] load_fail: {e}")
        return
    front_max, front_actual = _extract_fee_pair_by_label(
        driver,
        [
            "ค่าธรรมเนียมเมื่อซื้อหน่วยลงทุน",
            "Front-end Fee",
            "Front-end"
        ],
    )

    back_max, back_actual = _extract_fee_pair_by_label(
        driver,
        [
            "ค่าธรรมเนียมการรับซื้อคืนหน่วยลงทุน",
            "รับซื้อคืนหน่วยลงทุน",
            "Back-end Fee",
            "Back-end"
        ],
    )

    sw_in_max, sw_in_actual = _extract_fee_pair_by_label(
        driver,
        [
            "สับเปลี่ยนหน่วยลงทุนเข้า",
            "สับเปลี่ยนเข้า",
            "Switching-in Fee",
            "Switching-in",
        ],
    )

    sw_out_max, sw_out_actual = _extract_fee_pair_by_label(
        driver,
        [
            "สับเปลี่ยนหน่วยลงทุนออก",
            "สับเปลี่ยนออก",
            "Switching-out Fee",
            "Switching-out",
        ],
    )

    mgmt_max, mgmt_actual = _extract_fee_pair_by_label(
        driver,
        [
            "ค่าธรรมเนียมการจัดการ",
            "Management Fee",
        ],
    )

    ter_max, ter_actual = _extract_fee_pair_by_label(
        driver,
        [
            "ค่าธรรมเนียมและค่าใช้จ่ายรวมทั้งหมด",
            "Total Expense Ratio",
            "Total Expense",
            "TER",
        ],
    )
    
    if front_max:
        data["front_end_fee_max_percent"] = front_max
    if front_actual:
        data["front_end_fee_actual_percent"] = front_actual

    if back_max:
        data["back_end_fee_max_percent"] = back_max
    if back_actual:
        data["back_end_fee_actual_percent"] = back_actual

    if sw_in_max:
        data["switching_in_fee_max_percent"] = sw_in_max
    if sw_in_actual:
        data["switching_in_fee_actual_percent"] = sw_in_actual

    if sw_out_max:
        data["switching_out_fee_max_percent"] = sw_out_max
    if sw_out_actual:
        data["switching_out_fee_actual_percent"] = sw_out_actual

    if mgmt_max:
        data["management_fee_max_percent"] = mgmt_max
    if mgmt_actual:
        data["management_fee_actual_percent"] = mgmt_actual

    if ter_max:
        data["total_expense_ratio_max_percent"] = ter_max
    if ter_actual:
        data["total_expense_ratio_actual_percent"] = ter_actual
    log(
        f"  [FEE] front={data['front_end_fee_max_percent']}/{data['front_end_fee_actual_percent']}, "
        f"back={data['back_end_fee_max_percent']}/{data['back_end_fee_actual_percent']}, "
        f"sw_in={data['switching_in_fee_max_percent']}/{data['switching_in_fee_actual_percent']}, "
        f"sw_out={data['switching_out_fee_max_percent']}/{data['switching_out_fee_actual_percent']}"
        f"mgmt={data['management_fee_max_percent']}/{data['management_fee_actual_percent']}, "
        f"ter={data['total_expense_ratio_max_percent']}/{data['total_expense_ratio_actual_percent']}"
    )

# ------------------ SCRAPE ONE FUND (FINNOMENA) ------------------

def scrape_fund_profile(driver, url: str) -> Dict[str, Any]:
    data: Dict[str, Any] = {
        "fund_url": url,
        "scraped_at": datetime.now().isoformat(timespec="seconds"),
        "nav_value": "",
        "nav_date": "",
        "full_name_th": "",
        "fund_code": "",
        "aum_value": "",
        "risk_level": "",
        "aimc_categories": "",
        "is_dividend": "",
        "inception_date": "",
        "factsheet_pdf_url": "",
        "beta": "",
        "front_end_fee_max_percent": "",
        "front_end_fee_actual_percent": "",
        "back_end_fee_max_percent": "",
        "back_end_fee_actual_percent": "",
        "switching_in_fee_max_percent": "",
        "switching_in_fee_actual_percent": "",
        "switching_out_fee_max_percent": "",
        "switching_out_fee_actual_percent": "",
        "management_fee_max_percent": "",
        "management_fee_actual_percent": "",
        "total_expense_ratio_max_percent": "",
        "total_expense_ratio_actual_percent": "",
        "error": "",
    }
    try:
        driver.get(url)
        unlock_scroll(driver)
        try:
            wait_visible(driver, By.ID, "fund-header", timeout=40)
        except Exception:
            wait_visible(driver, By.XPATH, "//h1", timeout=40)
    except Exception as e:
        data["error"] = f"load_fail: {e}"
        data["_pdf_holdings"] = []
        return data

    # ---------- FUND CODE / ชื่อกองทุน ----------
    data["fund_code"] = find_first_text(driver, [
        "//header[@id='fund-header']//h1",
        "//h1[1]",
    ])

    data["full_name_th"] = find_first_text(driver, [
        "//header[@id='fund-header']//p[1]",
        "//h1/following-sibling::p[1]",
        "//h1/following::*[starts-with(normalize-space(),'กองทุน')][1]",
    ])

    # ---------- รายละเอียดกองทุน ----------
    details = get_detail_dict(driver)
    data["aimc_categories"] = details.get("ประเภทกอง", "")
    raw_risk = details.get("ค่าความเสี่ยง", "")
    risk_num = extract_first_number(raw_risk) 
    data["risk_level"] = risk_num
    data["is_dividend"]    = details.get("นโยบายการจ่ายปันผล", "")
    raw_inception = details.get("วันที่จดทะเบียนกองทุน", "")
    parsed_inception = parse_thai_finnomena_date(raw_inception)
    data["inception_date"] = parsed_inception or raw_inception
    raw_aum = details.get("มูลค่าทรัพย์สินสุทธิ", "")
    m = re.search(r"([\d,\.]+)", raw_aum)
    if m:
        data["aum_value"] = m.group(1).replace(",", "")
    raw_front = details.get("ค่าธรรมเนียมขาย", "")
    raw_back  = details.get("ค่าธรรมเนียมรับซื้อคืน", "")
    data["front_end_fee_actual_percent"] = parse_percent_str(raw_front)
    data["back_end_fee_actual_percent"]  = parse_percent_str(raw_back)

    # ---------- NAV ----------
    nav_value, nav_date_raw = extract_nav_and_date(
        driver,
        (data.get("fund_code") or ""),
        url,
    )

    data["nav_value"] = nav_value

    if nav_date_raw:
        parsed_nav_date = parse_thai_finnomena_date(nav_date_raw)
        data["nav_date"] = parsed_nav_date or nav_date_raw
    else:
        data["nav_date"] = ""
    # ---------- FEE PAGE ----------
    scrape_fees_from_fee_page(driver, url, data)

    # ----------factsheet (PDF) ----------
    try:
        pdf_link_el = driver.find_element(
            By.XPATH,
            "//a[contains(normalize-space(), 'หนังสือชี้ชวน')]"
        )
        pdf_url = pdf_link_el.get_attribute("href") or ""
    except Exception:
        pdf_url = ""
    data["factsheet_pdf_url"] = pdf_url
        # ---------- PDF: ISIN / Bloomberg / Beta ----------
    data["_pdf_holdings"] = []
    data["_pdf_codes"] = []

    codes_rows: List[Dict[str, Any]] = []
    fund_code_for_pdf = (data.get("fund_code") or "").strip()
    scraped_at = data["scraped_at"]

    if pdf_url:
        try:
            pdf_bytes = fetch_pdf_bytes(pdf_url, referer=url)
            if pdf_bytes:
                isins = extract_all_isins_from_pdf_bytes(pdf_bytes)
                for idx, code in enumerate(isins):
                    codes_rows.append({
                        "scraped_at": scraped_at,
                        "fund_code": fund_code_for_pdf,
                        "fund_url": url,
                        "code_type": "ISIN",
                        "code_value": _norm_isin(code),
                    })
                blooms = extract_all_bloomberg_codes_from_pdf_bytes(pdf_bytes)
                for idx, code in enumerate(blooms):
                    codes_rows.append({
                        "scraped_at": scraped_at,
                        "fund_code": fund_code_for_pdf,
                        "fund_url": url,
                        "code_type": "Bloomberg",
                        "code_value": code,
                    })
                hints = []
                if fund_code_for_pdf:
                    hints.append(fund_code_for_pdf)
                if data.get("full_name_th"):
                    hints.append(data["full_name_th"])
                data["beta"] = extract_beta_from_pdf_bytes(pdf_bytes, fund_hints=hints)

        except Exception as e:
            data["error"] = f"pdf_parse_fail: {e}"
            codes_rows = []

    data["_pdf_codes"] = codes_rows
    # ---------- HOLDING ----------
    try:
        html_holdings = scrape_top_holdings_from_portfolio_page(
            driver,
            url,
            fund_code_for_pdf or data.get("fund_code", "")
        )
        data["_pdf_holdings"] = html_holdings
    except Exception as e:
        log(f"  [HOLDING] portfolio_fail: {e}")
        data["_pdf_holdings"] = []

    return data
# ------------------ CSV ------------------

FIELDS_ORDER = [
    "scraped_at",        
    "nav_value",
    "nav_date",
    "full_name_th",
    "fund_code",
    "bid_price_per_unit",
    "offer_price_per_unit",
    "aum_value",
    "risk_level",
    "category_th",
    "aimc_categories",
    "is_dividend",
    "inception_date",
    "fx_hedging",
    "turnover_ratio",
    "factsheet_pdf_url",
    "beta",
    "front_end_fee_max_percent",
    "front_end_fee_actual_percent",
    "back_end_fee_max_percent",
    "back_end_fee_actual_percent",
    "switching_in_fee_max_percent",
    "switching_in_fee_actual_percent",
    "switching_out_fee_max_percent",
    "switching_out_fee_actual_percent",
    "management_fee_max_percent",
    "management_fee_actual_percent",
    "total_expense_ratio_max_percent",
    "total_expense_ratio_actual_percent",
    "error",
]

FIELDS_ORDER_HOLDINGS = [
    "scraped_at",
    "fund_code",
    "fund_url",
    "holding_name",
    "weight_pct",
    "as_of_raw",
]

FIELDS_ORDER_CODES = [
    "scraped_at",
    "fund_code",
    "fund_url",
    "code_type",
    "code_value",
    #"is_primary",
]

def save_to_csv(rows: List[Dict[str, Any]], csv_path: str):
    if not rows:
        log("nothing to save csv")
        return
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS_ORDER, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            row_out = {
                col: (r.get(col, "") if r.get(col, "") is not None else "")
                for col in FIELDS_ORDER
            }
            w.writerow(row_out)

    ok = sum(1 for r in rows if not r.get("error"))
    log(f"save csv -> {csv_path} (done: {ok} / total: {len(rows)})")

def save_holdings_to_csv(rows: List[Dict[str, Any]], csv_path: str):
    if not rows:
        log("nothing to save")
        return
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS_ORDER_HOLDINGS, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            row_out = {
                col: (r.get(col, "") if r.get(col, "") is not None else "")
                for col in FIELDS_ORDER_HOLDINGS
            }
            w.writerow(row_out)
            
def save_codes_to_csv(rows: List[Dict[str, Any]], csv_path: str):
    if not rows:
        log("ไม่มี codes จะบันทึก CSV")
        return
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS_ORDER_CODES, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            row_out = {col: (r.get(col, "") if r.get(col, "") is not None else "") for col in FIELDS_ORDER_CODES}
            w.writerow(row_out)
    log(f"save codes csv done -> {csv_path} (ทั้งหมด: {len(rows)})")
# ------------------ MAIN ------------------

def main():
    driver = None
    try:
        log("strart webdriver ...")
        driver = make_driver(HEADLESS)
        driver.set_page_load_timeout(PAGELOAD_TIMEOUT)

        urls = get_all_fund_profile_urls(driver)
        if not urls:
            log("can't find link fund")
            return

        results: List[Dict[str, Any]] = []
        holdings_rows: List[Dict[str, Any]] = []
        codes_rows: List[Dict[str, Any]] = []

        total = len(urls)
        for i, url in enumerate(urls, 1):
            log(f"[{i}/{total}] Scrape fund -> {url}")
            row = scrape_fund_profile_with_retry(driver, url)
            results.append(row)

            pdf_holdings = row.pop("_pdf_holdings", [])
            if pdf_holdings:
                holdings_rows.extend(pdf_holdings)
            pdf_codes = row.pop("_pdf_codes", [])
            if pdf_codes:
                codes_rows.extend(pdf_codes)
            if i < total:
                polite_sleep()

        log("========== SUMMARY ==========")
        log(f"total scrape: {total}")
        log(f"total holdings: {len(holdings_rows)}")
        
    except KeyboardInterrupt:
        log("Stop")
    except Exception as e:
        print("FAILED")
        print(f"Error: {e}")
    finally:
        if results:
            save_to_csv(results, OUTPUT_CSV)
        if holdings_rows:
            save_holdings_to_csv(holdings_rows, OUTPUT_HOLDINGS_CSV)
        if codes_rows:
            save_codes_to_csv(codes_rows, OUTPUT_CODES_CSV)
        log("close browser")
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

if __name__ == "__main__":
    main()

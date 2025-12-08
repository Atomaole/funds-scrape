import csv
import re
import random, time
from urllib.parse import quote
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
    if not s: return ""
    s = s.replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()
def _norm_isin(s: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", (s or "").upper())

LIST_PAGE_URL = "https://www.wealthmagik.com/funds"
FEE = "https://www.wealthmagik.com/funds/KSET50LTF-L/fee"
FUND_CODE_SELECTOR = ".fundCode"
HEADLESS = False
PAGELOAD_TIMEOUT = 15
LIST_MAX_SECONDS = 100
LIST_IDLE_ROUNDS = 8
OUTPUT_CSV = "wealthmagik_funds.csv"
LIMIT_FUNDS: Optional[int] = None
OUTPUT_HOLDINGS_CSV = "wealthmagik_holdings.csv"
MAX_PROFILE_RETRY = 3
OUTPUT_CODES_CSV = "wealthmagik_codes.csv"
OUTPUT_FAILED_CSV = "wealthmagik_failed_funds.csv"

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
def scrape_fund_profile_with_retry(driver, url: str, max_attempts: int = MAX_PROFILE_RETRY) -> Dict[str, Any]:
    last_err = ""
    for attempt in range(1, max_attempts + 1):
        if attempt > 1:
            log(f">> Retry attempt {attempt}/{max_attempts} for {url}")
        
        try:
            row = scrape_fund_profile(driver, url)
            err = (row.get("error") or "").lower()
            if err.startswith("load_fail") or err.startswith("load_timeout"):
                last_err = err
                log(f"    >> {err}, waiting to retry...")
                try:
                    driver.delete_all_cookies()
                except:
                    pass
                polite_sleep()
                continue
            return row

        except Exception as e:
            last_err = f"exception: {e}"
            log(f">> Error occurred: {e}. Retrying...")
            polite_sleep()
            continue
    return {
        "fund_url": url,
        "scraped_at": datetime.now().isoformat(timespec="seconds"),
        "error": f"retry_failed: {last_err}",
        "_holdings": [],
        "_pdf_codes": [],
    }
    
def polite_sleep():
    t = random.uniform(1.3, 2.0) 
    time.sleep(t)

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
      // common lock classes
      for (const el of [document.documentElement, document.body]) {
        el.classList.remove('modal-open','no-scroll','overflow-hidden','is-locked');
        el.style.overflow = 'auto';
        el.style.position = 'static';
      }
      // remove overlays/backdrops that might block scroll
      document.querySelectorAll('.modal-backdrop,.overlay,[class*="overlay"],[id*="overlay"],[class*="backdrop"],[id*="backdrop"]').forEach(el=>{
        try{ el.remove(); }catch(e){ el.style.display='none'; }
      });
    } catch(e) {}
    """
    driver.execute_script(js)

def close_ad_if_present(driver):
    try:
        log("waiting ad")
        btn = WebDriverWait(driver, 8).until(
            EC.element_to_be_clickable((By.ID, "popupAdsClose"))
        )
        btn.click()
        time.sleep(0.8)
    except TimeoutException:
        pass
    unlock_scroll(driver)

def wait_fund_items(driver, timeout: int = 20):
    WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.CSS_SELECTOR, FUND_CODE_SELECTOR)))

def describe_element(driver, el) -> str:
    return driver.execute_script("""
    const el = arguments[0];
    if (!el) return "null";
    const cs = getComputedStyle(el);
    const id = el.id ? ("#"+el.id) : "";
    const cls = el.className ? ("."+ (el.className.toString().trim().replace(/\\s+/g,'.'))) : "";
    return el.tagName + id + cls +
      " overflowY=" + cs.overflowY +
      " clientHeight=" + el.clientHeight +
      " scrollHeight=" + el.scrollHeight;
    """, el)

def nearest_scrollable_ancestor_of_last_item(driver):
    js = """
    const sel = arguments[0];
    const items = document.querySelectorAll(sel);
    if (!items.length) return null;
    let el = items[items.length-1];
    const isScrollable = (x) => {
      if (!x) return false;
      const cs = getComputedStyle(x);
      const oy = cs.overflowY;
      return (oy === 'auto' || oy === 'scroll') && x.scrollHeight - x.clientHeight > 10;
    };
    while (el && el !== document.body && el !== document.documentElement) {
      el = el.parentElement;
      if (isScrollable(el)) return el;
    }
    return document.scrollingElement || document.documentElement || document.body;
    """
    return driver.execute_script(js, FUND_CODE_SELECTOR)

def elements_count(driver, selector: str) -> int:
    try:
        return len(driver.find_elements(By.CSS_SELECTOR, selector))
    except Exception:
        return 0

def focus_container(driver, container):
    try:
        ActionChains(driver).move_to_element(container).click().perform()
    except Exception:
        pass

def scroll_container_once(driver, container):
    try:
        driver.execute_script("arguments[0].scrollTop = arguments[0].scrollTop + arguments[0].clientHeight*0.9;", container)
    except Exception:
        pass
    time.sleep(0.15)
    try:
        items = driver.find_elements(By.CSS_SELECTOR, FUND_CODE_SELECTOR)
        if items:
            driver.execute_script("arguments[0].scrollIntoView({block:'end', inline:'nearest'});", items[-1])
    except StaleElementReferenceException:
        pass
    time.sleep(0.15)
    try:
        driver.execute_script("""
            const el = arguments[0];
            el.dispatchEvent(new WheelEvent('wheel', {deltaY: 1600, bubbles: true}));
        """, container)
    except Exception:
        pass
    time.sleep(0.15)
    try:
        ActionChains(driver).send_keys(Keys.END).perform()
    except Exception:
        pass

def get_all_fund_profile_urls(driver) -> List[str]:
    driver.get(LIST_PAGE_URL)
    close_ad_if_present(driver)
    wait_fund_items(driver, 20)

    container = nearest_scrollable_ancestor_of_last_item(driver)
    if not container:
        container = driver.execute_script("return document.scrollingElement || document.documentElement || document.body;")
    focus_container(driver, container)
    
    start = time.time()
    last = elements_count(driver, FUND_CODE_SELECTOR)
    same = 0
    while True:
        if time.time() - start > LIST_MAX_SECONDS:
            log(f"Time limit reached: {LIST_MAX_SECONDS}s")
            break
        scroll_container_once(driver, container)
        new = elements_count(driver, FUND_CODE_SELECTOR)
        if new <= last:
            same += 1
            if same >= LIST_IDLE_ROUNDS:
                break
        else:
            last = new
            same = 0
    elems = driver.find_elements(By.CSS_SELECTOR, FUND_CODE_SELECTOR)
    codes: Set[str] = set()
    pat = re.compile(r"[A-Z0-9][A-Z0-9\-\s/&]{2,}") 

    for el in elems:
        raw_id = el.get_attribute("id") or ""
        prefix = "wmg.fundscreenerdetail.button.fundcode."
        
        if raw_id.startswith(prefix):
            code = raw_id.replace(prefix, "").strip()
            if code:
                codes.add(code)
                continue 
        t = (el.text or "").strip().replace("\n", " ")
        m = pat.search(t)
        if m:
            codes.add(m.group(0).strip())
    links = driver.find_elements(By.XPATH, "//a[contains(@href, '/funds/')]")
    for a in links:
        href = a.get_attribute("href") or ""
        m = re.search(r"/funds/([^/?#]+)", href) 
        if m:
            from urllib.parse import unquote
            codes.add(unquote(m.group(1)))
    urls = []
    for c in sorted(codes):
        c_clean = c.replace("&", " ") 
        safe_url_part = quote(c_clean, safe='')
        urls.append(f"https://www.wealthmagik.com/funds/{safe_url_part}/profile")
    if LIMIT_FUNDS:
        urls = urls[:LIMIT_FUNDS]
    log(f"found {len(urls)} fund urls")
    if urls:
        log("sample urls: " + ", ".join(urls[:5]))
    return urls

def wait_visible(driver, by, value, timeout=15):
    return WebDriverWait(driver, timeout).until(EC.visibility_of_element_located((by, value)))

def find_text_by_xpath(driver, xpath: str) -> str:
    try:
        el = driver.find_element(By.XPATH, xpath)
        return (el.text or "").strip()
    except Exception:
        return ""

def get_id_value_by_prefix(driver, prefix: str) -> str:
    try:
        el = driver.find_element(By.CSS_SELECTOR, f'[id^="{prefix}"]')
        raw_id = el.get_attribute("id") or ""
        return raw_id.replace(prefix, "")
    except Exception:
        return ""
    
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

def extract_all_bloomberg_codes_from_pdf_bytes(pdf_bytes: bytes) -> List[str]:
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

def _clean_thai_text(text: str) -> str:
    if not text: return ""
    text = re.sub(r"[\u0e30-\u0e4c]", "", text)
    text = re.sub(r"[\s\.]", "", text)
    text = re.sub(r"[^ก-ฮA-Za-z0-9]", "", text)
    return text

def _get_fuzzy_month_map() -> Dict[str, int]:
    standard_months = {
        "ม.ค.": 1, "มกราคม": 1, "JAN": 1, "JANUARY": 1,
        "ก.พ.": 2, "กุมภาพันธ์": 2, "FEB": 2, "FEBRUARY": 2,
        "มี.ค.": 3, "มีนาคม": 3, "MAR": 3, "MARCH": 3,
        "เม.ย.": 4, "เมษายน": 4, "APR": 4, "APRIL": 4,
        "พ.ค.": 5, "พฤษภาคม": 5, "MAY": 5,
        "มิ.ย.": 6, "มิถุนายน": 6, "JUN": 6, "JUNE": 6,
        "ก.ค.": 7, "กรกฎาคม": 7, "JUL": 7, "JULY": 7,
        "ส.ค.": 8, "สิงหาคม": 8, "AUG": 8, "AUGUST": 8,
        "ก.ย.": 9, "กันยายน": 9, "SEP": 9, "SEPTEMBER": 9,
        "ต.ค.": 10, "ตุลาคม": 10, "OCT": 10, "OCTOBER": 10,
        "พ.ย.": 11, "พฤศจิกายน": 11, "NOV": 11, "NOVEMBER": 11,
        "ธ.ค.": 12, "ธันวาคม": 12, "DEC": 12, "DECEMBER": 12,
    }
    
    fuzzy_map = {}
    for name, num in standard_months.items():
        clean_key = _clean_thai_text(name)
        if clean_key:
            fuzzy_map[clean_key] = num
            fuzzy_map[clean_key.lower()] = num
            
    return fuzzy_map

def extract_factsheet_date_from_pdf_bytes(pdf_bytes: bytes) -> str:

    if not pdf_bytes:
        return ""

    fuzzy_months = _get_fuzzy_month_map()

    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            page = pdf.pages[0]
            crop_height = page.height * 0.35
            header = page.crop((0, 0, page.width, crop_height))
            words = header.extract_words(x_tolerance=3, y_tolerance=3, keep_blank_chars=False)
            lines = {}
            for w in words:
                y_pos = int(round(w['top'] / 5) * 5)
                if y_pos not in lines: lines[y_pos] = []
                lines[y_pos].append(w)
            
            sorted_y = sorted(lines.keys())
            
            for y in sorted_y:
                line_words = sorted(lines[y], key=lambda w: w['x0'])
                line_text = " ".join([w['text'] for w in line_words])
                line_text = re.sub(r"\s+", " ", line_text).strip()
                if any(bad in line_text for bad in ["จดทะเบียน", "จัดตั้ง", "Inception", "Approve", "อนุมัติ"]):
                    continue
                matches = re.finditer(r"(\d{1,2})\s+(.*?)\s+(\d{4})", line_text)
                
                for m in matches:
                    d_raw, m_raw, y_raw = m.groups()
                    if len(m_raw) > 30: continue
                    m_clean = _clean_thai_text(m_raw)
                    if not m_clean: continue
                    month_val = 0
                    if m_clean in fuzzy_months:
                        month_val = fuzzy_months[m_clean]
                    else:
                        for k_map, v_map in fuzzy_months.items():
                            if len(k_map) > 2 and k_map in m_clean:
                                month_val = v_map
                                break
                    
                    if month_val != 0:
                        try:
                            y_val = int(y_raw)
                            if y_val > 2400: y_val -= 543  
                            elif y_val < 100: y_val += 2500 - 543 
                            if 1990 <= y_val <= 2100:
                                return datetime(y_val, month_val, int(d_raw)).strftime("%Y-%m-%d")
                        except:
                            pass

    except Exception as e:
        pass
    return ""
    
def extract_value_by_visual_alignment(pdf_bytes: bytes, keywords: List[str]) -> str:
    if not pdf_bytes:
        return ""
    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                words = page.extract_words(x_tolerance=2, y_tolerance=2)
                words.sort(key=lambda w: (w['top'], w['x0']))
                for i, word in enumerate(words):
                    text = word['text'].replace(" ", "").strip()
                    is_match = False
                    for kw in keywords:
                        if kw.lower() in text.lower():
                            is_match = True
                            break
                    if is_match:
                        target_top = word['top'] - 5
                        target_bottom = word['bottom'] + 5 
                        target_left = word['x1']
                        for next_word in words[i+1:]:
                            if next_word['top'] > target_bottom:
                                break
                            if (next_word['top'] >= target_top and 
                                next_word['bottom'] <= target_bottom and
                                next_word['x0'] >= target_left):
                                val_text = next_word['text'].strip()
                                if "n/a" in val_text.lower():
                                    return "N/A"
                                m = re.search(r"([-+]?\d+(?:\.\d+)?)", val_text.replace(",", ""))
                                if m:
                                    num_val_str = m.group(1)
                                    try:
                                        if abs(float(num_val_str)) < 3000:
                                            return num_val_str
                                    except:
                                        pass
        return ""
    except Exception:
        return ""

def extract_sharpe_from_pdf_bytes(pdf_bytes: bytes) -> str:
    return extract_value_by_visual_alignment(pdf_bytes, ["Sharpe"])

def extract_alpha_from_pdf_bytes(pdf_bytes: bytes) -> str:
    return extract_value_by_visual_alignment(pdf_bytes, ["Alpha"])

def extract_max_drawdown_from_pdf_bytes(pdf_bytes: bytes) -> str:
    return extract_value_by_visual_alignment(pdf_bytes, ["Drawdown"])

def extract_turnover_from_pdf_bytes(pdf_bytes: bytes) -> str:
    return extract_value_by_visual_alignment(pdf_bytes, ["Turnover", "หมุนเวียน"])

def extract_fx_hedging_from_pdf_bytes(pdf_bytes: bytes) -> str:
    keywords = ["Hedging", "ป้องกันความเสี่ยง", "FX"]
    return extract_value_by_visual_alignment(pdf_bytes, keywords)

def extract_recovering_period_from_pdf_bytes(pdf_bytes: bytes) -> str:
    target_keywords = ["Recovering", "ระยะเวลาฟื้นตัว"] 
    if not pdf_bytes:
        return ""    
    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                words = page.extract_words(x_tolerance=3, y_tolerance=3)
                words.sort(key=lambda w: (w['top'], w['x0']))
                for i, word in enumerate(words):
                    text = word['text'].replace(" ", "").strip()
                    is_match = False
                    for kw in target_keywords:
                        if kw.lower() in text.lower():
                            is_match = True
                            break
                    if is_match:
                        target_top = word['top'] - 15
                        target_bottom = word['bottom'] + 15 
                        target_left = word['x1']
                        found_texts = []
                        for next_word in words[i+1:]:
                            if next_word['top'] > target_bottom:
                                break
                            if (next_word['top'] >= target_top and 
                                next_word['bottom'] <= target_bottom and
                                next_word['x0'] >= target_left):
                                found_texts.append(next_word['text'].strip())
                        if found_texts:
                            full_string = " ".join(found_texts)
                            converted = _convert_period_to_days(full_string)
                            if converted:
                                return converted
                                
        return ""
    except Exception:
        return ""

def parse_date_yyyymmdd(s: str) -> str:
    s = s.strip()
    if not s:
        return ""
    try:
        d = datetime.strptime(s, "%Y%m%d")
        return d.strftime("%d-%m-%Y")
    except Exception:
        return s
def parse_thai_date(s: str) -> str:
    if not s:
        return ""
    s = _clean_txt(s)
    s = re.sub(r"(ข้อมูล\s*ณ\s*วันที่|ณ\s*วันที่|ข้อมูล ณ วันที่|วันที่)", "", s).strip()
    m = re.search(r"(\d{1,2})\s+([^\d\s]+)\s*(\d{2,4})", s)
    if m:
        day_str, month_str, year_str = m.groups()
        month = THAI_MONTH_MAP.get(month_str.strip())
        if month:
            try:
                day = int(day_str)
                year = int(year_str)
                if year > 2400: year -= 543
                return datetime(year, month, day).strftime("%Y-%m-%d")
            except: pass
    m2 = re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})", s)
    if m2:
        try:
            day, month, year = map(int, m2.groups())
            if year > 2400: year -= 543
            return datetime(year, month, day).strftime("%Y-%m-%d")
        except: pass
        
    return ""

def _convert_period_to_days(text: str) -> str:
    if not text:
        return ""
    raw = text.lower().replace(",", "")
    raw_nospace = raw.replace(" ", "")

    if "n/a" in raw_nospace or raw_nospace == "-":
        return "N/A"
    m = re.search(r"(\d+(?:\.\d+)?)", raw_nospace)
    if not m:
        return ""

    try:
        val = float(m.group(1))
    except ValueError:
        return ""

    days = val
    year_keywords = [
        "year", "yr", "ปี", "ป", "y.", "years"
    ]
    month_keywords = [
        "month", "mo", "m.", 
        "เดือน", "เดอน", "เดอืน", "เดือน", "ดือน", 
        "เ ดื อ น", "เ ดื อ น"
    ]
    week_keywords = [
        "week", "wk", "w.", "สัปดาห์", "สัปดาห"
    ]
    if any(kw in raw_nospace for kw in year_keywords):
        days = val * 365
    elif any(kw in raw_nospace for kw in month_keywords):
        days = val * 30
    elif any(kw in raw_nospace for kw in week_keywords):
        days = val * 7
    return str(int(round(days)))

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

def scrape_holdings_from_allocation_page(
    driver,
    profile_url: str,
    fund_code: str,
) -> List[Dict[str, Any]]:
    holdings: List[Dict[str, Any]] = []
    base_url = re.sub(r"/profile/?$", "", profile_url)
    port_url = base_url + "/port"
    log(f"[HOLDING] open port: {port_url}")
    try:
        driver.get(port_url)
        unlock_scroll(driver)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".portallocation-list, .emptyData"))
        )
        has_empty_tag = driver.find_elements(By.CSS_SELECTOR, ".emptyData")
        if has_empty_tag:
            return []

    except TimeoutException:
        log(f"  [HOLDING] timeout or no data element found -> skip holdings")
        return []
    except Exception as e:
        log(f"  [HOLDING] error opening port page: {e}")
        return []

    scraped_at = datetime.now().isoformat(timespec="seconds")
    as_of_raw = ""
    try:
        as_of_el = driver.find_element(
            By.XPATH,
            "//div[contains(@class,'mainDivtopHolding')]//span[contains(@class,'asofdate')]"
        )
        as_of_raw = _clean_txt(as_of_el.text or "")
    except Exception:
        pass

    try:
        rows = driver.find_elements(By.CSS_SELECTOR, ".portallocation-list")
    except Exception:
        rows = []

    for row in rows:
        try:
            name = ""
            weight = ""
            try:
                name_el = row.find_element(By.CSS_SELECTOR, ".name-text")
                name = _clean_txt(name_el.text or "")
            except Exception:
                pass
            try:
                weight_el = row.find_element(By.CSS_SELECTOR, ".ratio-text")
                pct_txt = _clean_txt(weight_el.text or "")
                m = re.search(r"[-+]?\d+(?:\.\d+)?", pct_txt.replace(",", ""))
                if m:
                    weight = m.group(0)
            except Exception:
                pass
            
            if name and weight:
                holdings.append({
                    "scraped_at": scraped_at,
                    "fund_code": fund_code,
                    "fund_url": profile_url,
                    "holding_name": name,
                    "weight_pct": weight,
                    "as_of_raw": as_of_raw,
                })

        except Exception as e:
            continue

    return holdings

def scrape_fee_page(driver, profile_url: str) -> Dict[str, str]:
    result = {
        "initial_purchase": "",
        "additional_purchase": "",
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
    }
    fees_url = profile_url.replace("/profile", "/fee")

    log(f"[FEE] Loading... {fees_url}")
    driver.get(fees_url)
    unlock_scroll(driver)
    wait_visible(driver, By.XPATH, "//*[contains(text(),'ค่าธรรมเนียม (Fees)')]", timeout=15)
    
    initial_raw = find_text_by_xpath(driver, "//*[@id='wmg.funddetailfee.text.initialPurchase-ffs']")
    additional_raw = find_text_by_xpath(driver, "//*[@id='wmg.funddetailfee.text.additionalPurchase-ffs']")
    front_max_raw = find_text_by_xpath(driver, "//*[@id='wmg.funddetailfee.text.frontEndFee-ffs']")
    front_actual_raw = find_text_by_xpath(driver, "//*[@id='wmg.funddetailfee.text.frontEndFee-actual']")
    back_max_raw = find_text_by_xpath(driver, "//*[@id='wmg.funddetailfee.text.backEndFee-ffs']")
    back_actual_raw = find_text_by_xpath(driver, "//*[@id='wmg.funddetailfee.text.backEndFee-actual']")
    switching_in_max_raw = find_text_by_xpath(driver, "//*[@id='wmg.funddetailfee.text.switchingInFee-ffs']")
    switching_in_actual_raw = find_text_by_xpath(driver, "//*[@id='wmg.funddetailfee.text.switchingInFee-actual']")
    switching_out_max_raw = find_text_by_xpath(driver, "//*[@id='wmg.funddetailfee.text.switchingOutFee-ffs']")
    switching_out_actual_raw = find_text_by_xpath(driver, "//*[@id='wmg.funddetailfee.text.switchingOutFee-actual']")
    Management_Fee_max_raw = find_text_by_xpath(driver, "//*[@id='wmg.funddetailfee.text.managementFee-ffs']")
    Management_Fee_actual_raw = find_text_by_xpath(driver, "//*[@id='wmg.funddetailfee.text.managementFee-actual']")
    Total_Expense_Ratio_max_raw = find_text_by_xpath(driver, "//*[@id='wmg.funddetailfee.text.totalExpenseRatio-ffs']")
    Total_Expense_Ratio_actual_raw = find_text_by_xpath(driver, "//*[@id='wmg.funddetailfee.text.totalExpenseRatioActual-ffs']")

    result["initial_purchase"] = parse_percent_str(initial_raw)
    result["additional_purchase"] = parse_percent_str(additional_raw)
    result["front_end_fee_max_percent"] = parse_percent_str(front_max_raw)
    result["front_end_fee_actual_percent"] = parse_percent_str(front_actual_raw)
    result["back_end_fee_max_percent"] = parse_percent_str(back_max_raw)
    result["back_end_fee_actual_percent"] = parse_percent_str(back_actual_raw)
    result["switching_in_fee_max_percent"] = parse_percent_str(switching_in_max_raw)
    result["switching_in_fee_actual_percent"] = parse_percent_str(switching_in_actual_raw)
    result["switching_out_fee_max_percent"] = parse_percent_str(switching_out_max_raw)
    result["switching_out_fee_actual_percent"] =parse_percent_str(switching_out_actual_raw)
    result["management_fee_max_percent"]  = parse_percent_str(Management_Fee_max_raw)
    result["management_fee_actual_percent"] =parse_percent_str(Management_Fee_actual_raw)
    result["total_expense_ratio_max_percent"] = parse_percent_str(Total_Expense_Ratio_max_raw)
    result["total_expense_ratio_actual_percent"] =parse_percent_str(Total_Expense_Ratio_actual_raw)

    return result
    

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
        "fx_hedging_web": "",
        "turnover_ratio_web": "",
        "factsheet_pdf_url": "",
        "factsheet_date": "",
        "fx_hedging": "",
        "turnover_ratio": "",
        "sharpe_ratio": "",
        "alpha": "",
        "max_drawdown": "",
        "recovering_period": "",
        "beta": "",
        "initial_purchase": "",
        "additional_purchase": "",
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
        try:
            driver.get(url)
        except TimeoutException as e:
            log(f"[soft timeout] driver.get() timeout for {url}: {e}")
            try:
                driver.execute_script("window.stop();")
            except Exception:
                pass

        unlock_scroll(driver)
        wait_visible(driver, By.XPATH, "//div[@class='fundName']", timeout=20)

    except TimeoutException as e:
        data["error"] = f"load_timeout: {e}"
        return data
    except WebDriverException as e:
        data["error"] = f"load_fail: {e}"
        return data
    except Exception as e:
        data["error"] = f"load_fail: {e}"
        return data

    try:
        nav_text = driver.find_element(By.CLASS_NAME, "nav").text.strip()
    except Exception:
        nav_text = ""
    
    data["nav_value"] = nav_text
    data["nav_date"] = parse_date_yyyymmdd(get_id_value_by_prefix(driver, "wmg.funddetailinfo.text.tnaclassDate."))

    data["full_name_th"] = find_text_by_xpath(driver, "//div[@class='fundName']/span[@class='categoryTH']")
    data["fund_code"] = find_text_by_xpath(driver, "//div[@class='fundName']/h1")

    data["bid_price_per_unit"] = get_id_value_by_prefix(driver, "wmg.funddetailinfo.text.bidPrice.")
    data["offer_price_per_unit"] = get_id_value_by_prefix(driver, "wmg.funddetailinfo.text.offerPrice.")
    data["aum_value"] = get_id_value_by_prefix(driver, "wmg.funddetailinfo.text.totalnetAsset.")

    data["risk_level"] = get_id_value_by_prefix(driver, "wmg.funddetailinfo.text.riskSpectrum.")
    data["category_th"] = get_id_value_by_prefix(driver, "wmg.funddetailinfo.text.categoryTH.")
    data["aimc_categories"] = get_id_value_by_prefix(driver, "wmg.funddetailinfo.text.aimcCategories.")
    data["is_dividend"] = get_id_value_by_prefix(driver, "wmg.funddetailinfo.text.isDividend.")
    data["inception_date"] = parse_date_yyyymmdd(get_id_value_by_prefix(driver, "wmg.funddetailinfo.text.inceptionDate."))

    raw_fx_hedging = find_text_by_xpath(driver, "//div[contains(@class, 'groupDetail') and .//div[contains(@class, 'label') and normalize-space(text()) = 'FX Hedging']]//div[contains(@class, 'value')]/span")
    raw_turnover_ratio = find_text_by_xpath(driver, "//div[contains(@class, 'groupDetail') and .//div[contains(@class, 'label') and normalize-space(text()) = 'Turnover Ratio']]//div[contains(@class, 'value')]/span")
    if "N/A" in raw_fx_hedging.upper():
        raw_fx_hedging = ""
    data["fx_hedging_web"] = raw_fx_hedging
    if "N/A" in raw_turnover_ratio.upper():
        raw_turnover_ratio = ""
    data["turnover_ratio_web"] = raw_turnover_ratio
    try:
        pdf_url = get_id_value_by_prefix(driver, "wmg.funddetailinfo.button.factSheetPath.")
    except Exception:
        pdf_url = ""
    data["factsheet_pdf_url"] = pdf_url

    fund_code_for_pdf = (data.get("fund_code") or "").strip()
    scraped_at = data["scraped_at"]

    codes_rows: List[Dict[str, Any]] = []
    data["_holdings"] = []
    data["_pdf_codes"] = []

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
                        #"is_primary": "1" if idx == 0 else "0",
                    })
                blooms = extract_all_bloomberg_codes_from_pdf_bytes(pdf_bytes)
                for idx, code in enumerate(blooms):
                    codes_rows.append({
                        "scraped_at": scraped_at,
                        "fund_code": fund_code_for_pdf,
                        "fund_url": url,
                        "code_type": "Bloomberg",
                        "code_value": code,
                        #"is_primary": "1" if idx == 0 else "0",
                    })
                hints = []
                if fund_code_for_pdf:
                    hints.append(fund_code_for_pdf)
                if data.get("full_name_th"):
                    hints.append(data["full_name_th"])
                data["beta"] = extract_beta_from_pdf_bytes(pdf_bytes, fund_hints=hints)
                data["sharpe_ratio"] = extract_sharpe_from_pdf_bytes(pdf_bytes)
                data["alpha"] = extract_alpha_from_pdf_bytes(pdf_bytes)
                data["max_drawdown"] = extract_max_drawdown_from_pdf_bytes(pdf_bytes)
                data["recovering_period"] = extract_recovering_period_from_pdf_bytes(pdf_bytes)
                data["factsheet_date"] = extract_factsheet_date_from_pdf_bytes(pdf_bytes)
                data["turnover_ratio"] = extract_turnover_from_pdf_bytes(pdf_bytes)
                data["fx_hedging"] = extract_fx_hedging_from_pdf_bytes(pdf_bytes)
                
        except Exception as e:
            data["beta"] = ""
            data["_pdf_codes"] = []
            data.setdefault("error", f"pdf_parse_fail: {e}")
    else:
        data["beta"] = ""

    data["_pdf_codes"] = codes_rows
    
    html_holdings = scrape_holdings_from_allocation_page(
        driver,
        url,
        fund_code_for_pdf or data.get("fund_code", "")
    )
    if html_holdings:
        data["_holdings"] = html_holdings
    fee_data = scrape_fee_page(driver, url)
    data.update(fee_data)

    return data

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
    "fx_hedging_web",
    "turnover_ratio_web",
    "factsheet_pdf_url",
    "factsheet_date",
    "fx_hedging",
    "turnover_ratio",
    "sharpe_ratio",
    "alpha",
    "max_drawdown",
    "recovering_period",
    "beta",
    "initial_purchase",
    "additional_purchase",
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
        log("No data to save to CSV")
        return
    filtered_rows: List[Dict[str, Any]] = []
    seen_codes: Set[str] = set()

    for r in rows:
        code = (r.get("fund_code") or "").strip()
        if not code:
            filtered_rows.append(r)
            continue
        if code in seen_codes:
            continue
        seen_codes.add(code)
        filtered_rows.append(r)

    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS_ORDER, extrasaction="ignore")
        w.writeheader()
        for r in filtered_rows:
            row_out = {
                col: (r.get(col, "") if r.get(col, "") is not None else "")
                for col in FIELDS_ORDER
            }
            w.writerow(row_out)

    ok = sum(1 for r in filtered_rows if not r.get("error"))
    log(f"save CSV done {csv_path} (success: {ok} / total: {len(filtered_rows)})")

def save_holdings_to_csv(rows: List[Dict[str, Any]], csv_path: str):
    if not rows:
        log("nothing to save csv")
        return
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS_ORDER_HOLDINGS, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            row_out = {col: (r.get(col, "") if r.get(col, "") is not None else "") for col in FIELDS_ORDER_HOLDINGS}
            w.writerow(row_out)
    log(f"save holding csv done-> {csv_path} (total: {len(rows)})")

def save_codes_to_csv(rows: List[Dict[str, Any]], csv_path: str):
    if not rows:
        log("No codes to save to CSV")
        return
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS_ORDER_CODES, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            row_out = {col: (r.get(col, "") if r.get(col, "") is not None else "") for col in FIELDS_ORDER_CODES}
            w.writerow(row_out)
    log(f"save codes csv done -> {csv_path} (total: {len(rows)})")
    
def save_failed_to_csv(rows: List[Dict[str, Any]], csv_path: str):
    if not rows:
        return

    failed_fields = ["scraped_at", "fund_code", "fund_url", "error"]

    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=failed_fields, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            row_out = {col: (r.get(col, "") if r.get(col, "") is not None else "") for col in failed_fields}
            w.writerow(row_out)

    log(f"save FAILED CSV {csv_path} (total: {len(rows)})")
    
def is_row_failed_or_incomplete(row: Dict[str, Any]) -> bool:
    if row.get("error"):
        return True

    critical_keys = ["fund_code", "full_name_th"]
    for k in critical_keys:
        v = row.get(k, "")
        if not str(v).strip():
            return True

    if not str(row.get("nav_value", "")).strip() and not str(row.get("aum_value", "")).strip():
        return True

    return False

def main():
    driver = None

    results_by_url: Dict[str, Dict[str, Any]] = {}
    holdings_by_fund: Dict[str, List[Dict[str, Any]]] = {}
    codes_by_fund: Dict[str, List[Dict[str, Any]]] = {}

    def process_url(url: str, idx: int, total: int):
        log(f"[{idx}/{total}] Scrape fund -> {url}")
        row = scrape_fund_profile_with_retry(driver, url)

        html_holdings = row.pop("_holdings", [])
        pdf_codes = row.pop("_pdf_codes", [])

        results_by_url[url] = row
        if html_holdings:
            holdings_by_fund[url] = html_holdings
        if pdf_codes:
            codes_by_fund[url] = pdf_codes

    try:
        log("start web")
        driver = make_driver(HEADLESS)
        driver.set_page_load_timeout(PAGELOAD_TIMEOUT)

        urls = get_all_fund_profile_urls(driver)
        if not urls:
            log("can't find link")
            return
        max_rounds = 3
        round_no = 1
        current_urls = urls[:]

        while current_urls and round_no <= max_rounds:
            log(f"===={round_no} / {max_rounds}")
            total = len(current_urls)

            for i, url in enumerate(current_urls, 1):
                process_url(url, i, total)
                if i < total:
                    polite_sleep()
            failed_next: List[str] = []
            for url in current_urls:
                row = results_by_url.get(url, {})
                if is_row_failed_or_incomplete(row):
                    failed_next.append(url)

            log(f"round {round_no} done (scraped: {len(current_urls)}, failed: {len(failed_next)})")
            round_no += 1
            current_urls = failed_next
        total_funds = len(results_by_url)
        total_holdings = sum(len(v) for v in holdings_by_fund.values())
        total_codes = sum(len(v) for v in codes_by_fund.values())
        failed_final = [u for u, r in results_by_url.items() if is_row_failed_or_incomplete(r)]

        log("========== SUMMARY ==========")
        log(f"total funds scraped: {total_funds}")
        log(f"total holdings: {total_holdings}")
        log(f"total codes: {total_codes}")
        log(f"still failed after {max_rounds} rounds: {len(failed_final)}")
        if failed_final:
            log("sample failed: " + ", ".join(failed_final[:10]))

    except KeyboardInterrupt:
        log("Stop")
    except Exception as e:
        print("FAILED")
        print(f"Error: {e}")
    finally:
        final_results: List[Dict[str, Any]] = list(results_by_url.values())

        final_holdings: List[Dict[str, Any]] = []
        for rows in holdings_by_fund.values():
            final_holdings.extend(rows)

        final_codes: List[Dict[str, Any]] = []
        for rows in codes_by_fund.values():
            final_codes.extend(rows)

        if final_results:
            save_to_csv(final_results, OUTPUT_CSV)
        if final_holdings:
            save_holdings_to_csv(final_holdings, OUTPUT_HOLDINGS_CSV)
        if final_codes:
            save_codes_to_csv(final_codes, OUTPUT_CODES_CSV)
        failed_rows: List[Dict[str, Any]] = []
        for row in final_results:
            if is_row_failed_or_incomplete(row):
                failed_rows.append(row)
        if failed_rows:
            save_failed_to_csv(failed_rows, OUTPUT_FAILED_CSV)
        else:
            log("nothing fail")

        log("close browser")
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

if __name__ == "__main__":
    main()

import pandas as pd
from pathlib import Path
import glob
import time
import re
from datetime import datetime
from prefect import task

# CONFIG
script_dir = Path(__file__).resolve().parent

# Input Folders
FN_RAW_DIR = script_dir/"finnomena/raw_data"
FN_NAV_DIR = script_dir/"finnomena/all_nav"
WM_RAW_DIR = script_dir/"wealthmagik/raw_data"

# Output Folders
MERGED_OUTPUT_DIR = script_dir/"merged_output"
MERGED_NAV_DIR = MERGED_OUTPUT_DIR/"merged_nav_all"

for d in [MERGED_OUTPUT_DIR, MERGED_NAV_DIR]:
    if not d.exists(): d.mkdir(parents=True, exist_ok=True)

def log(msg):
    timestamp = time.strftime('%H:%M:%S')
    print(f"[{timestamp}] {msg}")

def safe_read_csv(path):
    if path.exists() and path.stat().st_size > 0:
        try: return pd.read_csv(path)
        except: return pd.DataFrame()
    return pd.DataFrame()

def sanitize_filename(name):
    if not name: return "unknown"
    return re.sub(r'[<>:"/\\|?*]', '_', str(name)).strip()

def get_valid_fund_codes():
    path = FN_RAW_DIR/"finnomena_fund_list.csv"
    df = safe_read_csv(path)
    if not df.empty and 'fund_code' in df.columns:
        return set(df['fund_code'].astype(str).str.strip().tolist())
    return set()

def merge_fee():
    log("Merging Fee")
    df = safe_read_csv(FN_RAW_DIR/"finnomena_fees.csv")
    if not df.empty:
        output_path = MERGED_OUTPUT_DIR/"merged_fee.csv"
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        log(f"Saved merged_fee.csv ({len(df)} records)")

def merge_performance():
    log("Merging Performance")
    df = safe_read_csv(FN_RAW_DIR/"finnomena_performance.csv")
    if not df.empty:
        output_path = MERGED_OUTPUT_DIR/"merged_performance.csv"
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        log(f"Saved merged_performance.csv ({len(df)} records)")

def merge_allocations(valid_codes):
    log("Merging Allocations")
    fn_df = safe_read_csv(FN_RAW_DIR/"finnomena_allocations.csv")
    if not fn_df.empty: fn_df['source'] = 'finnomena'
    wm_df = safe_read_csv(WM_RAW_DIR/"wealthmagik_allocations.csv")
    if not wm_df.empty: wm_df['source'] = 'wealthmagik'
    merged_df = pd.concat([fn_df, wm_df], ignore_index=True)
    if not merged_df.empty:
        sector_mapping = {
            "บริการด้านการเงิน": "Financial Services",
            "อุตสาหกรรม": "Industrials",
            "บริการด้านการสื่อสาร": "Communication Services",
            "พลังงาน": "Energy",
            "อรรถประโยชน์": "Utilities",
            "สาธารณูปโภคพื้นฐาน": "Utilities",
            "อสังหาริมทรัพย์": "Real Estate",
            "การแพทย์": "Healthcare",
            "สินค้าฟุ่มเฟือย/ตามวัฏจักร": "Consumer Cyclical",
            "สินค้าอุปโภคบริโภคคงทน": "Consumer Cyclical",
            "วัสดุทั่วไป": "Basic Materials",
            "เทคโนโลยี": "Technology",
            "สินค้าจำเป็น": "Consumer Defensive",
            "สินค้าอุปโภคบริโภคที่จำเป็น": "Consumer Defensive"
        }
        asset_mapping = {
            "พันธบัตรรัฐบาล": "Government Bond",
            "เงินฝากธนาคาร P/N และ B/E": "Cash & Equivalents",
            "เงินฝากธนาคาร": "Cash",
            "สินทรัพย์อื่นๆ/หนี้สินอื่นๆ": "Others",
            "สินทรัพย์อื่นๆ": "Others",
            "หนี้สินอื่นๆ": "Others",
            "หุ้น": "Equity",
            "หุ้นสามัญ": "Equity",
            "หน่วยลงทุน": "Unit Trust",
            "ตราสารอนุพันธ์": "Derivatives",
            "หุ้นกู้": "Corporate Bond",
            "ทองคำแท่ง": "Gold",
            "ใบสำคัญแสดงสิทธิ": "Warrant",
            "ใบสำคัญแสดงสิทธิอนุพันธ์": "Derivative Warrants"
        }
        country_mapping = {
            "สหรัฐอเมริกา": "United States (Country)",
            "USA": "United States (Country)",
            "United States": "United States (Country)",
            "แคนาดา": "Canada (Country)", "Canada": "Canada (Country)",
            "เม็กซิโก": "Mexico (Country)", "Mexico": "Mexico (Country)",
            "บราซิล": "Brazil (Country)", "Brazil": "Brazil (Country)",
            "ชิลี": "Chile (Country)", "Chile": "Chile (Country)",
            "โคลอมเบีย": "Colombia (Country)",
            "เปรู": "Peru (Country)", "Peru": "Peru (Country)",
            "สาธารณรัฐประชาชนจีน": "China (Country)", "จีน": "China (Country)", "China": "China (Country)",
            "ญี่ปุ่น": "Japan (Country)", "Japan": "Japan (Country)",
            "ไต้หวัน": "Taiwan (Country)", "Taiwan": "Taiwan (Country)",
            "เกาหลีใต้": "South Korea (Country)", "South Korea": "South Korea (Country)", "Korea": "South Korea (Country)",
            "ฮ่องกง": "Hong Kong (Country)", "Hong Kong": "Hong Kong (Country)",
            "อินเดีย": "India (Country)", "India": "India (Country)",
            "ไทย": "Thailand (Country)", "Thailand": "Thailand (Country)",
            "เวียดนาม": "Vietnam (Country)", "Vietnam": "Vietnam (Country)",
            "อินโดนีเซีย": "Indonesia (Country)", "INDONESIA": "Indonesia (Country)", "Indonesia": "Indonesia (Country)",
            "สิงคโปร์": "Singapore (Country)", "Singapore": "Singapore (Country)",
            "มาเลเซีย": "Malaysia (Country)", "Malaysia": "Malaysia (Country)",
            "ฟิลิปปินส์": "Philippines (Country)", "Philippines": "Philippines (Country)",
            "ศรีลังกา": "Sri Lanka (Country)", "Sri Lanka": "Sri Lanka (Country)",
            "มาเก๊า": "Macau (Country)", "Macau": "Macau (Country)",
            "สหราชอาณาจักร": "United Kingdom (Country)", "United Kingdom": "United Kingdom (Country)", "Britain": "United Kingdom (Country)",
            "เยอรมนี": "Germany (Country)", "Germany": "Germany (Country)",
            "ฝรั่งเศส": "France (Country)", "France": "France (Country)",
            "สวิตเซอร์แลนด์": "Switzerland (Country)", "Switzerland": "Switzerland (Country)",
            "เนเธอร์แลนด์": "Netherlands (Country)", "Netherlands": "Netherlands (Country)",
            "ไอร์แลนด์": "Ireland (Country)", "Ireland": "Ireland (Country)",
            "สเปน": "Spain (Country)", "Spain": "Spain (Country)",
            "อิตาลี": "Italy (Country)", "Italy": "Italy (Country)",
            "สวีเดน": "Sweden (Country)", "Sweden": "Sweden (Country)",
            "เดนมาร์ก": "Denmark (Country)", "Denmark": "Denmark (Country)",
            "ฟินแลนด์": "Finland (Country)", "Finland": "Finland (Country)",
            "เบลเยียม": "Belgium (Country)", "Belgium": "Belgium (Country)",
            "ออสเตรีย": "Austria (Country)", "Austria": "Austria (Country)",
            "โปรตุเกส": "Portugal (Country)", "Portugal": "Portugal (Country)",
            "กรีซ": "Greece (Country)", "Greece": "Greece (Country)",
            "ตุรกี": "Turkey (Country)", "Turkey": "Turkey (Country)",
            "โรมาเนีย": "Romania (Country)", "Romania": "Romania (Country)",
            "ลิกเตนสไตน์": "Liechtenstein (Country)", "Liechtenstein": "Liechtenstein (Country)",
            "ลักเซมเบิร์ก": "Luxembourg (Country)", "Luxembourg": "Luxembourg (Country)",
            "สาธารณรัฐเช็ก": "Czech Republic (Country)", "CZECH REPUBLIC": "Czech Republic (Country)",
            "ซาอุดีอาระเบีย": "Saudi Arabia (Country)", "Saudi Arabia": "Saudi Arabia (Country)",
            "สหรัฐอาหรับเอมิเรดส์": "United Arab Emirates (Country)", "United Arab Emirates": "United Arab Emirates (Country)",
            "กาตาร์": "Qatar (Country)", "Qatar": "Qatar (Country)",
            "อิสราเอล": "Israel (Country)", "Israel": "Israel (Country)",
            "แอฟริกาใต้": "South Africa (Country)", "South Africa": "South Africa (Country)",
            "ออสเตรเลีย": "Australia (Country)", "Australia": "Australia (Country)",
            "หมู่เกาะเคย์แมน": "Cayman Islands (Country)", "Cayman Islands": "Cayman Islands (Country)",
            "เบอร์มิวดา": "Bermuda (Country)", "Bermuda": "Bermuda (Country)",
            "ยุโรป": "Europe (Region)", "Europe": "Europe (Region)",
            "Europe ex UK": "Europe ex UK (Region)",
            "Europe & Middle East ex UK": "Europe & Middle East ex UK (Region)",
            "Continental Europe": "Continental Europe (Region)",
            "Economic and Monetary Union": "Eurozone (Region)",
            "Asia ex Japan": "Asia ex Japan (Region)",
            "Pacific ex Japan": "Pacific ex Japan (Region)",
            "Pacific Basin": "Pacific Basin (Region)",
            "Indian Sub-Continent": "Indian Sub-Continent (Region)",
            "North America": "North America (Region)",
            "Latin America": "Latin America (Region)",
            "Emerging Markets": "Emerging Markets (Region)",
            "Middle East/Developed": "Middle East/Developed (Region)",
            "อื่นๆ": "Others (Other)", "Others": "Others (Other)", "Other": "Others (Other)",
            "Unclassified": "Unclassified (Other)"
        }
        if 'name' in merged_df.columns:
            merged_df['name'] = merged_df['name'].replace(asset_mapping)
            merged_df['name'] = merged_df['name'].replace(sector_mapping)
            country_mask = merged_df['type'] == 'country_alloc'
            merged_df.loc[country_mask, 'name'] = merged_df.loc[country_mask, 'name'].str.strip()
            merged_df.loc[country_mask, 'name'] = merged_df.loc[country_mask, 'name'].replace(country_mapping)
        before_count = len(merged_df)
        merged_df = merged_df[merged_df['fund_code'].astype(str).str.strip().isin(valid_codes)]
        after_count = len(merged_df)
        if before_count > after_count:
            log(f"Filtered out {before_count - after_count} rows (funds not in finnomena list)")
        output_path = MERGED_OUTPUT_DIR/"merged_allocations.csv"
        merged_df.to_csv(output_path, index=False, encoding="utf-8-sig")
        log(f"Saved merged_allocations.csv ({len(merged_df)} records)")

def merge_codes():
    log("Merging Codes/ISIN")
    df = safe_read_csv(FN_RAW_DIR/"finnomena_codes.csv")
    if not df.empty:
        output_path = MERGED_OUTPUT_DIR/"merged_codes.csv"
        df.to_csv(output_path, index=False, encoding="utf-8-sig")

def merge_nav():
    log("Merging NAVs")
    wm_bid_offer_file = WM_RAW_DIR/"wealthmagik_bid_offer.csv"
    wm_data_map = {}
    wm_df = safe_read_csv(wm_bid_offer_file)
    if not wm_df.empty:
        log(f"Loaded WealthMagik Bid/Offer data ({len(wm_df)} rows)")
        for _, row in wm_df.iterrows():
            code = str(row.get('fund_code', '')).strip()
            date_str = str(row.get('nav_date', '')).strip()
            bid = row.get('bid_price', '')
            offer = row.get('offer_price', '')
            if code and date_str:
                wm_data_map[(code, date_str)] = {'bid': bid, 'offer': offer}
    fin_files = list(FN_NAV_DIR.glob("*.csv"))
    total_files = len(fin_files)
    log(f"Found {total_files} historical NAV files from Finnomena")
    count = 0
    for f_path in fin_files:
        count += 1
        nav_df = safe_read_csv(f_path)
        if nav_df.empty: continue
        if 'fund_code' in nav_df.columns and not nav_df.empty:
            fund_code = str(nav_df.iloc[0]['fund_code']).strip()
        else:
            fund_code = f_path.stem
        nav_df['date_str'] = nav_df['date'].astype(str).str.strip()
        def get_bid(date_val):
            return wm_data_map.get((fund_code, date_val), {}).get('bid', '')
        def get_offer(date_val):
            return wm_data_map.get((fund_code, date_val), {}).get('offer', '')
        nav_df['bid'] = nav_df['date_str'].map(get_bid)
        nav_df['offer'] = nav_df['date_str'].map(get_offer)
        nav_df['source_nav'] = "finnomena"
        if 'fund_code' not in nav_df.columns:
            nav_df['fund_code'] = fund_code
        if 'date_str' in nav_df.columns: del nav_df['date_str']
        safe_name = sanitize_filename(fund_code)
        output_path = MERGED_NAV_DIR/f"merged_nav_{safe_name}.csv"
        nav_df.to_csv(output_path, index=False, encoding="utf-8-sig")
        if count % 300 == 0:
            print(f"Processed NAVs: {count}/{total_files}")
    log("NAV Merge Completed")

@task(name="merged_funds_file", log_prints=True)
def merged_file():
    log("Starting Merge Process")
    valid_codes = get_valid_fund_codes()
    log(f"Master Fund List loaded: {len(valid_codes)} funds")
    merge_fee()
    merge_codes()
    merge_allocations(valid_codes)
    merge_performance()
    merge_nav()
    log("All Merge Tasks Done")

if __name__ == "__main__":
    merged_file.fn()
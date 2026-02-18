from fastapi import FastAPI, HTTPException
import mysql.connector
from mysql.connector import Error
from typing import List
from pydantic import BaseModel

app = FastAPI()

db_config = {
    'host': 'localhost',
    'database': 'funds_API',
    'user': 'fund_master',
    'password': 'password',
    'charset': 'utf8mb4'
}

def get_db_connection():
    try:
        connection = mysql.connector.connect(**db_config)
        return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

class MultiSearchRequest(BaseModel):
    symbols: List[str]

@app.get("/")
def health_check():
    return {"status": "online", "message": "Fund API is ready"}

@app.get("/api/stocks/top")
def get_top_stocks(type: str = "FOREIGN"):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    query = """
        SELECT s.symbol, s.full_name, s.stock_type, s.country, sa.total_thai_fund_value 
        FROM stocks s
        JOIN stock_aggregates sa ON s.id = sa.stock_id
        WHERE s.stock_type = %s
        ORDER BY sa.total_thai_fund_value DESC
        LIMIT 10
    """
    cursor.execute(query, (type,))
    result = cursor.fetchall()
    conn.close()
    return result

@app.post("/api/search/funds")
def search_funds_by_assets(req: MultiSearchRequest):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    if not req.symbols:
        return []
    placeholders = ','.join(['%s'] * len(req.symbols))
    num_symbols = len(req.symbols)
    query = f"""
        SELECT f.name_th, f.code, f.return_1y,
               SUM(fh.holding_value_thb) as total_value, 
               MAX(fh.nav_thb) as nav, 
               (SUM(fh.holding_value_thb)/MAX(fh.nav_thb)*100) as pct_nav,
               MAX(fh.investment_method) as investment_method
        FROM fund_holdings fh
        JOIN funds f ON fh.fund_id = f.id
        JOIN stocks s ON fh.stock_id = s.id
        WHERE s.symbol IN ({placeholders})
        GROUP BY f.id 
        HAVING COUNT(DISTINCT s.symbol) = %s
        ORDER BY total_value DESC
    """
    params = tuple(req.symbols) + (num_symbols,)
    cursor.execute(query, params)
    res = cursor.fetchall()
    conn.close()
    return res

@app.get("/api/stocks/{symbol}")
def get_stock_detail(symbol: str):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT s.*, sa.* FROM stocks s JOIN stock_aggregates sa ON s.id = sa.stock_id WHERE s.symbol = %s", (symbol,))
    summary = cursor.fetchone()
    if not summary: raise HTTPException(404, "Stock not found")
    cursor.execute("""
        SELECT f.name_th, f.code, fh.investment_method, fh.holding_value_thb, fh.percent_nav, fh.ranking
        FROM fund_holdings fh JOIN funds f ON fh.fund_id = f.id
        JOIN stocks s ON fh.stock_id = s.id WHERE UPPER(s.symbol) = UPPER(%s) ORDER BY fh.ranking ASC
    """, (symbol,))
    holders = cursor.fetchall()
    conn.close()
    return {"summary": summary, "holders": holders}

@app.get("/api/funds/{code}")
def get_fund_detail(code: str):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT f.name_th, f.name_en, f.code, f.return_1y,
               MAX(fh.nav_thb) as total_nav_thb
        FROM funds f
        LEFT JOIN fund_holdings fh ON f.id = fh.fund_id
        WHERE f.code = %s
        GROUP BY f.id
    """, (code,))
    fund_info = cursor.fetchone()
    if not fund_info:
        raise HTTPException(status_code=404, detail="Fund not found")
    cursor.execute("""
        SELECT s.symbol, s.full_name, s.stock_type, 
               fh.percent_nav, fh.holding_value_thb, 
               fh.investment_method
        FROM fund_holdings fh 
        JOIN funds f ON fh.fund_id = f.id
        JOIN stocks s ON fh.stock_id = s.id 
        WHERE f.code = %s 
        ORDER BY fh.percent_nav DESC
    """, (code,))
    holdings = cursor.fetchall()
    conn.close()
    return {"fund_info": fund_info, "holdings": holdings}

@app.get("/api/search/suggestions")
def get_suggestions(q: str):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    query = """
        (SELECT symbol as id, full_name as name, 'STOCK' as type 
         FROM stocks 
         WHERE symbol LIKE %s OR full_name LIKE %s 
         LIMIT 5)
        UNION
        (SELECT code as id, name_th as name, 'FUND' as type 
         FROM funds 
         WHERE code LIKE %s OR name_th LIKE %s 
         LIMIT 5)
    """
    search_term = f"%{q}%"
    cursor.execute(query, (search_term, search_term, search_term, search_term))
    
    res = cursor.fetchall()
    conn.close()
    return res

@app.get("/api/dashboard/stats")
def get_dashboard_stats(type: str = "FOREIGN"):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    summary_query = """
        SELECT 
            SUM(fh.holding_value_thb) as total_value,
            AVG(f.return_1y) as avg_return
        FROM fund_holdings fh
        JOIN funds f ON fh.fund_id = f.id
        JOIN stocks s ON fh.stock_id = s.id
        WHERE s.stock_type = %s
    """
    cursor.execute(summary_query, (type,))
    summary = cursor.fetchone()
    sector_query = """
        SELECT s.sector as name, SUM(fh.holding_value_thb) as value
        FROM fund_holdings fh
        JOIN stocks s ON fh.stock_id = s.id
        WHERE s.stock_type = %s
        GROUP BY s.sector
        ORDER BY value DESC
    """
    cursor.execute(sector_query, (type,))
    sectors = cursor.fetchall()
    country_query = """
        SELECT s.country as name, SUM(fh.holding_value_thb) as value
        FROM fund_holdings fh
        JOIN stocks s ON fh.stock_id = s.id
        WHERE s.stock_type = %s
        GROUP BY s.country
        ORDER BY value DESC
    """
    cursor.execute(country_query, (type,))
    countries = cursor.fetchall()
    conn.close()
    total_val = summary['total_value'] if summary and summary['total_value'] else 1
    top_sector = sectors[0] if sectors else {"name": "N/A", "value": 0}
    top_sector_pct = (top_sector['value'] / total_val) * 100
    top_country = countries[0] if countries else {"name": "N/A", "value": 0}
    top_country_pct = (top_country['value'] / total_val) * 100
    return {
        "cards": {
            "total_value": summary['total_value'] if summary else 0,
            "avg_return": round(summary['avg_return'], 2) if summary and summary['avg_return'] else 0,
            "top_sector": {"name": top_sector['name'], "percent": round(top_sector_pct, 1)},
            "top_country": {"name": top_country['name'], "percent": round(top_country_pct, 1)}
        },
        "charts": {
            "sector_allocation": sectors,
            "country_allocation": countries
        }
    }
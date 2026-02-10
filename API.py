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
    'password': 'password' 
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
        SELECT s.symbol, s.full_name, s.stock_type, sa.total_thai_fund_value 
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
    placeholders = ','.join(['%s'] * len(req.symbols))
    num_symbols = len(req.symbols)
    query = f"""
        SELECT f.name_th, f.code, SUM(fh.holding_value_thb) as total_value, 
               MAX(fh.nav_thb) as nav, (SUM(fh.holding_value_thb)/MAX(fh.nav_thb)*100) as pct_nav
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

@app.get("/api/funds/{code}/holdings")
def get_fund_holdings(code: str):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    query = """
        SELECT s.symbol, s.full_name, s.stock_type, fh.percent_nav, fh.holding_value_thb
        FROM fund_holdings fh JOIN funds f ON fh.fund_id = f.id
        JOIN stocks s ON fh.stock_id = s.id WHERE f.code = %s ORDER BY fh.percent_nav DESC
    """
    cursor.execute(query, (code,))
    res = cursor.fetchall()
    conn.close()
    return res

@app.get("/api/search/suggestions")
def get_suggestions(q: str):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    query = "SELECT symbol, full_name FROM stocks WHERE symbol LIKE %s OR full_name LIKE %s LIMIT 5"
    cursor.execute(query, (f"%{{q}}%", f"%{{q}}%"))
    res = cursor.fetchall()
    conn.close()
    return res
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, List
import mysql.connector
from mysql.connector import Error

app = FastAPI(title="Funds Master API", description="API สำหรับระบบแกะรอยกองทุนทะลวงไส้")

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

class ThaiFundFilter(BaseModel):
    category: Optional[str] = None
    amc: Optional[str] = None
    risk_level: Optional[int] = None
    stock_symbol: Optional[str] = None

class FeederFundFilter(BaseModel):
    amc: Optional[str] = None
    stock_symbol: Optional[str] = None

@app.get("/")
def health_check():
    return {"status": "online", "message": "API"}

@app.get("/api/filters")
def get_filters():
    conn = get_db_connection()
    if not conn: raise HTTPException(500, "Database connection failed")
    cursor = conn.cursor(dictionary=True)
    
    cursor.execute("SELECT DISTINCT amc FROM funds WHERE amc IS NOT NULL ORDER BY amc")
    amcs = [row['amc'] for row in cursor.fetchall()]
    
    cursor.execute("SELECT DISTINCT category FROM funds WHERE category IS NOT NULL ORDER BY category")
    categories = [row['category'] for row in cursor.fetchall()]
    
    cursor.execute("SELECT DISTINCT risk_level FROM funds WHERE risk_level IS NOT NULL ORDER BY risk_level")
    risk_levels = [row['risk_level'] for row in cursor.fetchall()]
    
    conn.close()
    return {
        "amc": amcs,
        "category": categories,
        "risk_level": risk_levels
    }

@app.get("/api/search/suggestions")
def get_suggestions(q: str):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    query = """
        SELECT symbol, full_name, stock_type 
        FROM stocks 
        WHERE symbol LIKE %s OR full_name LIKE %s 
        LIMIT 10
    """
    search_term = f"%{q}%"
    cursor.execute(query, (search_term, search_term))
    res = cursor.fetchall()
    conn.close()
    return res

@app.post("/api/dashboard/thai-funds")
def get_thai_funds_dashboard(filters: ThaiFundFilter):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    where_clauses = ["1=1"]
    params = []
    
    if filters.category:
        where_clauses.append("f.category = %s")
        params.append(filters.category)
    if filters.amc:
        where_clauses.append("f.amc = %s")
        params.append(filters.amc)
    if filters.risk_level:
        where_clauses.append("f.risk_level = %s")
        params.append(filters.risk_level)
    if filters.stock_symbol:
        where_clauses.append("s.symbol = %s")
        params.append(filters.stock_symbol)
    where_sql = " AND ".join(where_clauses)
    count_query = f"""
        SELECT COUNT(DISTINCT f.id) as total_funds
        FROM funds f
        JOIN fund_direct_holdings fdh ON f.id = fdh.fund_id
        JOIN stocks s ON fdh.stock_id = s.id
        WHERE {where_sql}
    """
    cursor.execute(count_query, tuple(params))
    total_funds = cursor.fetchone()['total_funds']
    donut_query = f"""
        SELECT s.symbol, SUM(fdh.holding_value_thb) as total_value
        FROM funds f
        JOIN fund_direct_holdings fdh ON f.id = fdh.fund_id
        JOIN stocks s ON fdh.stock_id = s.id
        WHERE {where_sql}
        GROUP BY s.symbol
        ORDER BY total_value DESC
        LIMIT 5
    """
    cursor.execute(donut_query, tuple(params))
    top_holdings = cursor.fetchall()
    table_query = f"""
        SELECT f.code as symbol, f.name_th, fdh.percent_nav as weight, 
               fdh.holding_value_thb as value_thb, fdh.nav_thb
        FROM funds f
        JOIN fund_direct_holdings fdh ON f.id = fdh.fund_id
        JOIN stocks s ON fdh.stock_id = s.id
        WHERE {where_sql}
        ORDER BY fdh.holding_value_thb DESC
        LIMIT 100
    """
    cursor.execute(table_query, tuple(params))
    funds_table = cursor.fetchall()
    
    conn.close()
    
    return {
        "kpi": {"total_funds": total_funds},
        "charts": {"top_holdings": top_holdings},
        "table": funds_table
    }
@app.post("/api/dashboard/feeder-funds")
def get_feeder_funds_dashboard(filters: FeederFundFilter):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    where_clauses = ["1=1"]
    params = []
    if filters.amc:
        where_clauses.append("f.amc = %s")
        params.append(filters.amc)
    if filters.stock_symbol:
        where_clauses.append("s.symbol LIKE %s")
        params.append(f"%{filters.stock_symbol}%")
    where_sql = " AND ".join(where_clauses)
    count_query = f"""
        SELECT COUNT(DISTINCT f.id) as total_funds
        FROM funds f
        JOIN fund_master_holdings fmh ON f.id = fmh.fund_id
        JOIN master_funds mf ON fmh.master_fund_id = mf.id
        JOIN master_fund_holdings mfh ON mf.id = mfh.master_fund_id
        JOIN stocks s ON mfh.stock_id = s.id
        WHERE {where_sql}
    """
    cursor.execute(count_query, tuple(params))
    total_funds = cursor.fetchone()['total_funds']
    bar_query = f"""
        SELECT f.code as fund_code, 
               MAX((fmh.percent_nav * mfh.percent_weight) / 100) as effective_weight
        FROM funds f
        JOIN fund_master_holdings fmh ON f.id = fmh.fund_id
        JOIN master_funds mf ON fmh.master_fund_id = mf.id
        JOIN master_fund_holdings mfh ON mf.id = mfh.master_fund_id
        JOIN stocks s ON mfh.stock_id = s.id
        WHERE {where_sql}
        GROUP BY f.id
        ORDER BY effective_weight DESC
        LIMIT 10
    """
    cursor.execute(bar_query, tuple(params))
    weight_bar_chart = cursor.fetchall()
    donut_query = f"""
        SELECT s.symbol, SUM((fmh.holding_value_thb * mfh.percent_weight) / 100) as est_value_thb
        FROM funds f
        JOIN fund_master_holdings fmh ON f.id = fmh.fund_id
        JOIN master_funds mf ON fmh.master_fund_id = mf.id
        JOIN master_fund_holdings mfh ON mf.id = mfh.master_fund_id
        JOIN stocks s ON mfh.stock_id = s.id
        WHERE {where_sql}
        GROUP BY s.symbol
        ORDER BY est_value_thb DESC
        LIMIT 10
    """
    cursor.execute(donut_query, tuple(params))
    asset_donut_chart = cursor.fetchall()
    table_query = f"""
        SELECT mf.name_en as feeder_fund_name, 
               SUM(fmh.holding_value_thb) as total_thai_value_thb,
               COUNT(DISTINCT f.id) as thai_funds_count
        FROM funds f
        JOIN fund_master_holdings fmh ON f.id = fmh.fund_id
        JOIN master_funds mf ON fmh.master_fund_id = mf.id
        JOIN master_fund_holdings mfh ON mf.id = mfh.master_fund_id
        JOIN stocks s ON mfh.stock_id = s.id
        WHERE {where_sql}
        GROUP BY mf.id
        ORDER BY total_thai_value_thb DESC
        LIMIT 100
    """
    cursor.execute(table_query, tuple(params))
    feeder_table = cursor.fetchall()
    
    conn.close()
    
    return {
        "kpi": {"total_funds": total_funds},
        "charts": {
            "weight_bar_chart": weight_bar_chart,
            "asset_donut_chart": asset_donut_chart
        },
        "table": feeder_table
    }
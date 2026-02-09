from fastapi import FastAPI, HTTPException
import mysql.connector
from mysql.connector import Error

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

@app.get("/")
def read_root():
    return {"message": "API ready"}

@app.get("/api/top-holdings")
def get_top_holdings():
    conn = get_db_connection()
    if conn is None:
        return {"error": "can't connect to database"}
    cursor = conn.cursor(dictionary=True)
    sql = """
    SELECT 
        s.symbol, 
        s.full_name, 
        s.stock_type, 
        sa.total_exposure_value
    FROM stock_aggregates sa
    JOIN stocks s ON sa.stock_id = s.id
    ORDER BY sa.total_exposure_value DESC
    LIMIT 10
    """
    cursor.execute(sql)
    results = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return results

@app.get("/api/funds-holding/{stock_symbol}")
def get_funds_holding_stock(stock_symbol: str):
    conn = get_db_connection()
    if conn is None:
        return {"error": "can't connect to database"}
    cursor = conn.cursor(dictionary=True)
    sql = """
    SELECT 
        f.name_th AS fund_name,
        fh.investment_method,
        fh.holding_value_thb,
        fh.nav_thb,
        fh.percent_nav,
        fh.ranking
    FROM fund_holdings fh
    JOIN funds f ON fh.fund_id = f.id
    JOIN stocks s ON fh.stock_id = s.id
    WHERE s.symbol = %s
    ORDER BY fh.holding_value_thb DESC
    """
    
    cursor.execute(sql, (stock_symbol,)) 
    results = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return results

# เพิ่ม Endpoint ตัวนี้เข้าไปครับ (อันเก่าเก็บไว้ด้วยนะ)
@app.get("/api/stock-summary/{symbol}")
def get_stock_summary(symbol: str):
    conn = get_db_connection()
    if conn is None:
        return {"error": "can't connect to database"}
    cursor = conn.cursor(dictionary=True)
    
    # ดึงข้อมูลตัวเลขสรุปทั้งหมดของหุ้นตัวนั้น เพื่อไปโชว์ใน Popup หรือ Header
    sql = """
    SELECT 
        s.symbol, s.full_name, s.sector,
        sa.total_exposure_value, sa.portfolio_weight, 
        sa.exposure_type, sa.total_funds_holding,
        sa.total_thai_fund_value, sa.global_fund_value
    FROM stocks s
    JOIN stock_aggregates sa ON s.id = sa.stock_id
    WHERE s.symbol = %s
    """
    
    cursor.execute(sql, (symbol,))
    result = cursor.fetchone() # เอาแค่ตัวเดียว เพราะเราเจาะจงหุ้นตัวนั้น
    
    cursor.close()
    conn.close()
    
    if not result:
        raise HTTPException(status_code=404, detail="Stock not found")
        
    return result
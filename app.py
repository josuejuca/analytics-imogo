from fastapi import FastAPI, Request, Query, HTTPException
from pydantic import BaseModel
from datetime import datetime, timedelta
from typing import List, Optional
import sqlite3
import pytz
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# Configuração do CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Conectar e configurar banco de dados
def get_db():
    conn = sqlite3.connect("analytics.db")
    conn.row_factory = sqlite3.Row  # Para retornar dicionários
    cursor = conn.cursor()
    
    # Criar tabela e índices
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS access_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT,
            page TEXT,
            ip TEXT,
            browser TEXT,
            timestamp TEXT
        )
    """)
    # Criar índices para consultas rápidas
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_id ON access_logs (user_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_page ON access_logs (page)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON access_logs (timestamp)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_ip ON access_logs (ip)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_browser ON access_logs (browser)")
    
    conn.commit()
    return conn, cursor

class AccessLog(BaseModel):
    user_id: str
    page: str

@app.post("/log_access")
def log_access(data: AccessLog, request: Request):
    conn, cursor = get_db()
    try:
        ip = request.client.host
        user_agent = request.headers.get("User-Agent", "Desconhecido")
        fuso_br = pytz.timezone("America/Sao_Paulo")
        timestamp_br = datetime.now(fuso_br).strftime("%Y-%m-%d %H:%M:%S")

        cursor.execute("""
            INSERT INTO access_logs (user_id, page, ip, browser, timestamp)
            VALUES (?, ?, ?, ?, ?)
        """, (data.user_id, data.page, ip, user_agent, timestamp_br))

        conn.commit()
        return {"success": True, "message": "Acesso registrado!"}
    finally:
        conn.close()

# Rotas GET melhoradas
@app.get("/access_logs", summary="Lista todos os logs com paginação")
def get_access_logs(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    sort: str = Query("desc", regex="^(asc|desc)$")
):
    conn, cursor = get_db()
    try:
        offset = (page - 1) * page_size
        order = "DESC" if sort == "desc" else "ASC"
        
        cursor.execute(
            f"SELECT * FROM access_logs ORDER BY timestamp {order} LIMIT ? OFFSET ?",
            (page_size, offset)
        )
        logs = [dict(row) for row in cursor.fetchall()]
        return {"data": logs, "page": page, "page_size": page_size}
    finally:
        conn.close()

@app.get("/access_logs/user/{user_id}", summary="Filtra logs por usuário")
def get_logs_by_user(user_id: str):
    conn, cursor = get_db()
    try:
        cursor.execute("""
            SELECT * FROM access_logs 
            WHERE user_id = ? 
            ORDER BY timestamp DESC
        """, (user_id,))
        logs = [dict(row) for row in cursor.fetchall()]
        return {"data": logs}
    finally:
        conn.close()

@app.get("/access_logs/page/{page}", summary="Filtra logs por página")
def get_logs_by_page(page: str):
    conn, cursor = get_db()
    try:
        cursor.execute("""
            SELECT * FROM access_logs 
            WHERE page = ? 
            ORDER BY timestamp DESC
        """, (page,))
        logs = [dict(row) for row in cursor.fetchall()]
        return {"data": logs}
    finally:
        conn.close()

@app.get("/access_logs/date_range", summary="Filtra logs por intervalo de datas")
def get_logs_by_date_range(
    start_date: str = Query(..., description="Formato: YYYY-MM-DD"),
    end_date: str = Query(..., description="Formato: YYYY-MM-DD")
):
    conn, cursor = get_db()
    try:
        cursor.execute("""
            SELECT * FROM access_logs 
            WHERE date(timestamp) BETWEEN ? AND ?
            ORDER BY timestamp DESC
        """, (start_date, end_date))
        logs = [dict(row) for row in cursor.fetchall()]
        return {"data": logs}
    except ValueError:
        raise HTTPException(400, "Formato de data inválido. Use YYYY-MM-DD")
    finally:
        conn.close()

@app.get("/access_logs/ip/{ip}", summary="Filtra logs por endereço IP")
def get_logs_by_ip(ip: str):
    conn, cursor = get_db()
    try:
        cursor.execute("""
            SELECT * FROM access_logs 
            WHERE ip = ? 
            ORDER BY timestamp DESC
        """, (ip,))
        logs = [dict(row) for row in cursor.fetchall()]
        return {"data": logs}
    finally:
        conn.close()

@app.get("/access_logs/browser/{browser}", summary="Filtra logs por navegador")
def get_logs_by_browser(browser: str):
    conn, cursor = get_db()
    try:
        cursor.execute("""
            SELECT * FROM access_logs 
            WHERE browser LIKE ? 
            ORDER BY timestamp DESC
        """, (f"%{browser}%",))
        logs = [dict(row) for row in cursor.fetchall()]
        return {"data": logs}
    finally:
        conn.close()

# Rotas de estatísticas
@app.get("/stats/count", summary="Contagem total de acessos")
def get_total_access():
    conn, cursor = get_db()
    try:
        cursor.execute("SELECT COUNT(*) as total FROM access_logs")
        return {"total": cursor.fetchone()["total"]}
    finally:
        conn.close()

@app.get("/stats/count/user/{user_id}", summary="Contagem de acessos por usuário")
def count_by_user(user_id: str):
    conn, cursor = get_db()
    try:
        cursor.execute("""
            SELECT COUNT(*) as count 
            FROM access_logs 
            WHERE user_id = ?
        """, (user_id,))
        return {"user_id": user_id, "count": cursor.fetchone()["count"]}
    finally:
        conn.close()

@app.get("/stats/count/page/{page}", summary="Contagem de acessos por página")
def count_by_page(page: str):
    conn, cursor = get_db()
    try:
        cursor.execute("""
            SELECT COUNT(*) as count 
            FROM access_logs 
            WHERE page = ?
        """, (page,))
        return {"page": page, "count": cursor.fetchone()["count"]}
    finally:
        conn.close()

@app.get("/stats/summary", summary="Resumo estatístico")
def get_summary():
    conn, cursor = get_db()
    try:
        # Acessos por dia
        cursor.execute("""
            SELECT date(timestamp) as date, COUNT(*) as count 
            FROM access_logs 
            GROUP BY date 
            ORDER BY date DESC
        """)
        daily = [dict(row) for row in cursor.fetchall()]
        
        # Top páginas
        cursor.execute("""
            SELECT page, COUNT(*) as count 
            FROM access_logs 
            GROUP BY page 
            ORDER BY count DESC 
            LIMIT 10
        """)
        top_pages = [dict(row) for row in cursor.fetchall()]
        
        # Top navegadores
        cursor.execute("""
            SELECT browser, COUNT(*) as count 
            FROM access_logs 
            GROUP BY browser 
            ORDER BY count DESC 
            LIMIT 5
        """)
        top_browsers = [dict(row) for row in cursor.fetchall()]
        
        return {
            "daily_access": daily,
            "top_pages": top_pages,
            "top_browsers": top_browsers
        }
    finally:
        conn.close()

@app.get("/stats/suspicious_ips", summary="Detecta IPs com alta atividade")
def get_suspicious_ips(
    threshold: int = Query(100, ge=10),
    hours: int = Query(24, ge=1)
):
    conn, cursor = get_db()
    try:
        start_time = datetime.now() - timedelta(hours=hours)
        start_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
        
        cursor.execute("""
            SELECT ip, COUNT(*) as count 
            FROM access_logs 
            WHERE timestamp >= ? 
            GROUP BY ip 
            HAVING count >= ? 
            ORDER BY count DESC
        """, (start_str, threshold))
        
        results = [dict(row) for row in cursor.fetchall()]
        return {
            "threshold": threshold,
            "time_window_hours": hours,
            "suspicious_ips": results
        }
    finally:
        conn.close()

@app.get("/stats/daily_summary", summary="Resumo diário de acessos únicos")
def get_daily_summary(days: int = Query(7, ge=1, description="Número de dias para analisar")):
    conn, cursor = get_db()
    try:
        # Calcular data de início
        fuso_br = pytz.timezone("America/Sao_Paulo")
        end_date = datetime.now(fuso_br)
        start_date = end_date - timedelta(days=days)
        
        # Query para agrupar acessos únicos
        cursor.execute("""
            SELECT 
                date(timestamp) as date,
                COUNT(*) as total_accesses,
                COUNT(DISTINCT 
                    CASE 
                        WHEN user_id = 'anon' THEN ip || '|' || browser 
                        ELSE user_id 
                    END
                ) as unique_users
            FROM access_logs
            WHERE date(timestamp) BETWEEN ? AND ?
            GROUP BY date(timestamp)
            ORDER BY date(timestamp) DESC
        """, (start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")))
        
        results = []
        for row in cursor.fetchall():
            results.append({
                "date": row["date"],
                "total_accesses": row["total_accesses"],
                "unique_users": row["unique_users"]
            })
            
        return {
            "days_analyzed": days,
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "daily_summary": results
        }
    finally:
        conn.close()
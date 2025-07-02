from fastapi import FastAPI, Request, Query, HTTPException
from pydantic import BaseModel
from datetime import datetime, timedelta
from typing import List, Optional, Dict
import sqlite3
import pytz
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import io
from collections import defaultdict

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

@app.get("/access_logs/all", summary="Retorna todos os logs sem filtro")
def get_all_logs(limit: int = Query(1000, ge=1, le=10000)):
    conn, cursor = get_db()
    try:
        cursor.execute("""
            SELECT * FROM access_logs 
            ORDER BY timestamp DESC 
            LIMIT ?
        """, (limit,))
        logs = [dict(row) for row in cursor.fetchall()]
        return {"total": len(logs), "data": logs}
    finally:
        conn.close()

@app.get("/backup/sqlite", summary="Gera e retorna o backup SQL do banco de dados", tags=["Backup"])
def backup_sqlite():
    conn = sqlite3.connect("analytics.db")
    try:
        # Gera o dump SQL
        dump_buffer = io.StringIO()
        for line in conn.iterdump():
            dump_buffer.write(f"{line}\n")
        dump_buffer.seek(0)

        # Retorna como arquivo para download
        return StreamingResponse(
            iter([dump_buffer.getvalue()]),
            media_type="application/sql",
            headers={"Content-Disposition": "attachment; filename=backup_analytics.sql"}
        )
    finally:
        conn.close()

@app.get("/stats/hourly_access", summary="Acessos por hora do dia (0-23)")
def get_hourly_access():
    conn, cursor = get_db()
    try:
        cursor.execute("""
            SELECT strftime('%H', timestamp) as hour, COUNT(*) as count
            FROM access_logs
            GROUP BY hour
            ORDER BY hour
        """)
        return {"hourly_access": [dict(row) for row in cursor.fetchall()]}
    finally:
        conn.close()

@app.get("/stats/last_access_per_user", summary="Último acesso de cada usuário")
def get_last_access_per_user(limit: int = Query(100, ge=1, le=1000)):
    conn, cursor = get_db()
    try:
        cursor.execute("""
            SELECT user_id, MAX(timestamp) as last_access
            FROM access_logs
            GROUP BY user_id
            ORDER BY last_access DESC
            LIMIT ?
        """, (limit,))
        return {"last_access_per_user": [dict(row) for row in cursor.fetchall()]}
    finally:
        conn.close()

# novas rotas 

@app.get("/access_logs/month_year", summary="Filtra logs por mês e ano (Ex: julho de 2025)", tags=["Acessos por Mês/Ano"])
def get_logs_by_month_year(
    mes: int = Query(..., ge=1, le=12, description="Mês (1 a 12)"),
    ano: int = Query(..., ge=2000, le=2100, description="Ano (ex: 2025)"),
    limit: int = Query(1000, ge=1, le=10000, description="Limite de resultados")
):
    conn, cursor = get_db()
    try:
        # Formata o mês com zero à esquerda se for necessário (ex: '07')
        mes_str = f"{mes:02d}"
        ano_str = str(ano)

        cursor.execute("""
            SELECT * FROM access_logs
            WHERE strftime('%m', timestamp) = ? AND strftime('%Y', timestamp) = ?
            ORDER BY timestamp DESC
            LIMIT ?
        """, (mes_str, ano_str, limit))

        logs = [dict(row) for row in cursor.fetchall()]
        return {
            "mes": mes,
            "ano": ano,
            "total": len(logs),
            "data": logs
        }
    finally:
        conn.close()

@app.get("/stats/pages_by_month_year", summary="Contagem de acessos e usuários únicos por página para um mês/ano com acumulados" , tags=["Acessos por Mês/Ano"])
def get_page_counts_and_uniques_by_month_year(
    mes: int = Query(..., ge=1, le=12, description="Mês (1 a 12)"),
    ano: int = Query(..., ge=2000, le=2100, description="Ano (ex: 2025)")
):
    conn, cursor = get_db()
    try:
        # Definir intervalo de datas do mês
        start_date = f"{ano}-{mes:02d}-01"
        if mes == 12:
            end_date = f"{ano+1}-01-01"
        else:
            end_date = f"{ano}-{mes+1:02d}-01"

        # ---------- Contagem de acessos por página no mês ----------
        cursor.execute("""
            SELECT page, COUNT(*) as count_in_month
            FROM access_logs
            WHERE timestamp >= ? AND timestamp < ?
            GROUP BY page
        """, (start_date, end_date))
        month_counts = {row["page"]: row["count_in_month"] for row in cursor.fetchall()}

        # ---------- Usuários únicos por página no mês ----------
        cursor.execute("""
            SELECT page, COUNT(DISTINCT user_id) as unique_users_in_month
            FROM access_logs
            WHERE timestamp >= ? AND timestamp < ?
            GROUP BY page
        """, (start_date, end_date))
        month_uniques = {row["page"]: row["unique_users_in_month"] for row in cursor.fetchall()}

        # ---------- Contagem acumulada até o fim do mês ----------
        cursor.execute("""
            SELECT page, COUNT(*) as count_until_month
            FROM access_logs
            WHERE timestamp < ?
            GROUP BY page
        """, (end_date,))
        until_month_counts = {row["page"]: row["count_until_month"] for row in cursor.fetchall()}

        # ---------- Usuários únicos acumulados até o mês ----------
        cursor.execute("""
            SELECT page, COUNT(DISTINCT user_id) as unique_users_until_month
            FROM access_logs
            WHERE timestamp < ?
            GROUP BY page
        """, (end_date,))
        until_month_uniques = {row["page"]: row["unique_users_until_month"] for row in cursor.fetchall()}

        # ---------- Contagem total geral ----------
        cursor.execute("""
            SELECT page, COUNT(*) as count_total
            FROM access_logs
            GROUP BY page
        """)
        total_counts = {row["page"]: row["count_total"] for row in cursor.fetchall()}

        # ---------- Usuários únicos total geral ----------
        cursor.execute("""
            SELECT page, COUNT(DISTINCT user_id) as unique_users_total
            FROM access_logs
            GROUP BY page
        """)
        total_uniques = {row["page"]: row["unique_users_total"] for row in cursor.fetchall()}

        # ---------- Montar resposta final ----------
        all_pages = set(month_counts) | set(month_uniques) | set(until_month_counts) | set(until_month_uniques) | set(total_counts) | set(total_uniques)
        result = []
        for page in sorted(all_pages):
            result.append({
                "page": page,
                "count_in_month": month_counts.get(page, 0),
                "unique_users_in_month": month_uniques.get(page, 0),
                "count_until_month": until_month_counts.get(page, 0),
                "unique_users_until_month": until_month_uniques.get(page, 0),
                "count_total": total_counts.get(page, 0),
                "unique_users_total": total_uniques.get(page, 0)
            })

        return {
            "mes": mes,
            "ano": ano,
            "pages": result
        }

    finally:
        conn.close()

@app.get("/stats/recurrence_by_page", summary="Recorrência mensal de acessos por página (frequência de usuários)", tags=["Acessos por Mês/Ano"])
def get_recurrence_by_page(
    mes: int = Query(..., ge=1, le=12, description="Mês (1 a 12)"),
    ano: int = Query(..., ge=2000, le=2100, description="Ano (ex: 2025)")
):
    conn, cursor = get_db()
    try:
        # Definindo intervalo do mês
        start_date = f"{ano}-{mes:02d}-01"
        if mes == 12:
            end_date = f"{ano+1}-01-01"
        else:
            end_date = f"{ano}-{mes+1:02d}-01"

        # Primeiro: pegar quantas vezes cada usuário acessou cada página no mês
        cursor.execute("""
            SELECT page, user_id, COUNT(*) as access_count
            FROM access_logs
            WHERE timestamp >= ? AND timestamp < ?
            GROUP BY page, user_id
        """, (start_date, end_date))
        rows = cursor.fetchall()

        # Estrutura: { page: { user_id: quantidade_de_acessos } }
        page_user_counts = {}
        for row in rows:
            page = row["page"]
            user_id = row["user_id"]
            count = row["access_count"]
            if page not in page_user_counts:
                page_user_counts[page] = []
            page_user_counts[page].append(count)

        # Agora calcular frequência por quantidade de acessos
        result = []
        for page, user_counts in page_user_counts.items():
            freq = {
                "1x": 0,
                "2x": 0,
                "3x": 0,
                "4x": 0,
                "5x_or_more": 0
            }
            max_access = 0
            for count in user_counts:
                max_access = max(max_access, count)
                if count == 1:
                    freq["1x"] += 1
                elif count == 2:
                    freq["2x"] += 1
                elif count == 3:
                    freq["3x"] += 1
                elif count == 4:
                    freq["4x"] += 1
                else:
                    freq["5x_or_more"] += 1

            result.append({
                "page": page,
                "month": mes,
                "year": ano,
                "user_access_frequency": freq,
                "max_accesses_by_single_user": max_access
            })

        return {
            "mes": mes,
            "ano": ano,
            "pages": result
        }

    finally:
        conn.close()

@app.get("/access_logs/by_page_and_month_year", summary="Retorna todos os logs de uma página específica em um mês/ano", tags=["Acessos por Mês/Ano"])
def get_logs_by_page_and_month_year(
    page: str = Query(..., description="Nome exato da página"),
    mes: int = Query(..., ge=1, le=12, description="Mês (1 a 12)"),
    ano: int = Query(..., ge=2000, le=2100, description="Ano (ex: 2025)"),
    limit: int = Query(10000, ge=1, le=50000, description="Limite máximo de registros retornados")
):
    conn, cursor = get_db()
    try:
        # Definir intervalo de datas do mês
        start_date = f"{ano}-{mes:02d}-01"
        if mes == 12:
            end_date = f"{ano+1}-01-01"
        else:
            end_date = f"{ano}-{mes+1:02d}-01"

        cursor.execute("""
            SELECT * FROM access_logs
            WHERE page = ?
            AND timestamp >= ?
            AND timestamp < ?
            ORDER BY timestamp DESC
            LIMIT ?
        """, (page, start_date, end_date, limit))

        logs = [dict(row) for row in cursor.fetchall()]

        return {
            "page": page,
            "mes": mes,
            "ano": ano,
            "total": len(logs),
            "data": logs
        }

    finally:
        conn.close()
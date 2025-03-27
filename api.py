from fastapi import FastAPI, Request
from pydantic import BaseModel
from datetime import datetime
from typing import List
import sqlite3
import pytz
from fastapi.middleware.cors import CORSMiddleware
app = FastAPI()

# Configuração do CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Libera acesso para qualquer origem (pode restringir depois)
    allow_credentials=True,
    allow_methods=["*"],  # Permite todos os métodos HTTP
    allow_headers=["*"],  # Permite todos os headers
)

# Conectar ao banco de dados SQLite
def get_db():
    conn = sqlite3.connect("analytics.db")
    cursor = conn.cursor()
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
    conn.commit()
    return conn, cursor

# Modelo de dados para receber o acesso
class AccessLog(BaseModel):
    user_id: str
    page: str

# 📌 1️⃣ Registrar um acesso com IP e Navegador
@app.post("/log_access")
def log_access(data: AccessLog, request: Request):
    conn, cursor = get_db()

    # Capturar IP do usuário
    ip = request.client.host

    # Capturar o User-Agent (navegador)
    user_agent = request.headers.get("User-Agent", "Desconhecido")

    # Capturar data e hora em formato BR
    fuso_br = pytz.timezone("America/Sao_Paulo")
    timestamp_br = datetime.now(fuso_br).strftime("%Y-%m-%d %H:%M:%S")

    cursor.execute("""
        INSERT INTO access_logs (user_id, page, ip, browser, timestamp)
        VALUES (?, ?, ?, ?, ?)
    """, (data.user_id, data.page, ip, user_agent, timestamp_br))

    conn.commit()
    conn.close()
    return {"message": "Acesso registrado!"}

# 📌 2️⃣ Listar acessos com IP e Navegador
@app.get("/access_logs", response_model=List[dict])
def get_access_logs():
    conn, cursor = get_db()
    cursor.execute("SELECT user_id, page, ip, browser, timestamp FROM access_logs ORDER BY timestamp DESC")
    logs = [
        {"user_id": row[0], "page": row[1], "ip": row[2], "browser": row[3], "timestamp": row[4]}
        for row in cursor.fetchall()
    ]
    conn.close()
    return logs


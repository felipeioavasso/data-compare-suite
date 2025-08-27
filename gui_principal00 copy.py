# gui_conexoes.py
import os
import time
import threading
import tkinter as tk
from tkinter import messagebox

import psycopg2
import pyodbc
import pandas as pd
from dotenv import load_dotenv

from datetime import datetime

from extracao_notas import extrair_aws, extrair_fabric, normalizar_numericos

load_dotenv()

conn_pg = None
conn_fabric = None

# ---------- helpers ----------
def log(msg: str):
    log_text.configure(state=tk.NORMAL)
    log_text.insert(tk.END, msg.rstrip() + "\n")
    log_text.see(tk.END)
    log_text.configure(state=tk.DISABLED)

def safe_disable(w): 
    try: w.config(state=tk.DISABLED)
    except: pass

def safe_enable(w): 
    try: w.config(state=tk.NORMAL)
    except: pass

def require_env(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f"Variável de ambiente '{key}' ausente no .env")
    return val

def parse_status(s: str):
    return [int(x.strip()) for x in s.split(",") if x.strip()]

# ---------- conexões ----------
def conectar_postgres_async(): threading.Thread(target=conectar_postgres, daemon=True).start()
def conectar_fabric_async():   threading.Thread(target=conectar_fabric,  daemon=True).start()
def ping_postgres_async():     threading.Thread(target=ping_postgres,    daemon=True).start()
def ping_fabric_async():       threading.Thread(target=ping_fabric,      daemon=True).start()

def conectar_postgres():
    global conn_pg
    safe_disable(btn_con_pg); safe_disable(btn_descon_pg); safe_disable(btn_pg_ping)
    status_pg.config(text="⏳ Conectando ao PostgreSQL...", fg="orange"); log("PostgreSQL: conectando...")
    try:
        host = require_env("PG_HOST")
        port = os.getenv("PG_PORT", "5432")
        db   = require_env("PG_DB")
        user = require_env("PG_USER")
        pwd  = require_env("PG_PASSWORD")
        conn_pg = psycopg2.connect(
            host=host, port=port, database=db, user=user, password=pwd,
            connect_timeout=10, sslmode=os.getenv("PG_SSLMODE","require")
        )
        status_pg.config(text="✅ PostgreSQL conectado", fg="green")
        log("PostgreSQL: conexão estabelecida.")
        safe_disable(btn_con_pg); safe_enable(btn_descon_pg); safe_enable(btn_pg_ping)
    except Exception as e:
        conn_pg = None
        status_pg.config(text=f"❌ PostgreSQL: {e}", fg="red")
        log(f"PostgreSQL ERRO: {e}")
        safe_enable(btn_con_pg); safe_disable(btn_descon_pg); safe_disable(btn_pg_ping)

def desconectar_postgres():
    global conn_pg
    try:
        if conn_pg: conn_pg.close()
        status_pg.config(text="🔌 PostgreSQL desconectado", fg="gray")
        log("PostgreSQL: desconectado.")
    except Exception as e:
        messagebox.showerror("Erro", f"Falha ao desconectar PostgreSQL: {e}")
        log(f"PostgreSQL ERRO ao desconectar: {e}")
    finally:
        conn_pg = None
        safe_enable(btn_con_pg); safe_disable(btn_descon_pg); safe_disable(btn_pg_ping)

def conectar_fabric():
    global conn_fabric
    safe_disable(btn_con_fab); safe_disable(btn_descon_fab); safe_disable(btn_fab_ping)
    status_fabric.config(text="⏳ Conectando ao Fabric...", fg="orange"); log("Fabric: conectando...")
    try:
        driver = os.getenv("FABRIC_ODBC_DRIVER", "ODBC Driver 18 for SQL Server")
        server = require_env("FABRIC_SERVER")
        db     = require_env("FABRIC_DB")
        user   = require_env("FABRIC_USER")
        pwd    = require_env("FABRIC_PASSWORD")
        auth   = os.getenv("FABRIC_AUTH", "ActiveDirectoryPassword")
        conn_str = (
            f"DRIVER={{{driver}}};SERVER={server};DATABASE={db};"
            f"UID={user};PWD={pwd};Authentication={auth};"
            "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=15;"
        )
        conn_fabric = pyodbc.connect(conn_str)
        status_fabric.config(text="✅ Fabric conectado", fg="green")
        log("Fabric: conexão estabelecida.")
        safe_disable(btn_con_fab); safe_enable(btn_descon_fab); safe_enable(btn_fab_ping)
    except Exception as e:
        conn_fabric = None
        status_fabric.config(text=f"❌ Fabric: {e}", fg="red")
        log(f"Fabric ERRO: {e}")
        safe_enable(btn_con_fab); safe_disable(btn_descon_fab); safe_disable(btn_fab_ping)

def desconectar_fabric():
    global conn_fabric
    try:
        if conn_fabric: conn_fabric.close()
        status_fabric.config(text="🔌 Fabric desconectado", fg="gray")
        log("Fabric: desconectado.")
    except Exception as e:
        messagebox.showerror("Erro", f"Falha ao desconectar Fabric: {e}")
        log(f"Fabric ERRO ao desconectar: {e}")
    finally:
        conn_fabric = None
        safe_enable(btn_con_fab); safe_disable(btn_descon_fab); safe_disable(btn_fab_ping)

# ---------- pings ----------
def ping_postgres():
    if not conn_pg: return log("PostgreSQL: não conectado.")
    safe_disable(btn_pg_ping)
    try:
        t0 = time.perf_counter()
        with conn_pg.cursor() as cur:
            cur.execute("SELECT 1")
            val = cur.fetchone()[0]
        dt = (time.perf_counter() - t0) * 1000
        log(f"PostgreSQL PING ok (SELECT 1 = {val}) — {dt:.1f} ms")
    except Exception as e:
        log(f"PostgreSQL PING ERRO: {e}")
    finally:
        safe_enable(btn_pg_ping)

def ping_fabric():
    if not conn_fabric: return log("Fabric: não conectado.")
    safe_disable(btn_fab_ping)
    try:
        t0 = time.perf_counter()
        df = pd.read_sql("SELECT 1 AS ok", conn_fabric)
        dt = (time.perf_counter() - t0) * 1000
        log(f"Fabric PING ok (SELECT 1 = {int(df.iloc[0,0])}) — {dt:.1f} ms")
    except Exception as e:
        log(f"Fabric PING ERRO: {e}")
    finally:
        safe_enable(btn_fab_ping)

# ---------- extração ----------
def extrair_aws_async():    threading.Thread(target=_extrair_aws,    daemon=True).start()
def extrair_fabric_async(): threading.Thread(target=_extrair_fabric, daemon=True).start()
def extrair_ambos_async():  threading.Thread(target=_extrair_ambos,  daemon=True).start()

def _extrair_aws():
    if not conn_pg: return log("⚠️ Conecte no PostgreSQL antes de extrair AWS.")
    _toggle_extract_buttons(False)
    try:
        dt = entry_data.get().strip()
        status = parse_status(entry_status.get().strip())
        log(f"🔎 Extraindo AWS: dt_inicio={dt} status={status}")
        df = extrair_aws(conn_pg, dt, status)
        df = normalizar_numericos(df, casas=4)
        _salvar(df, "aws", dt)
    except Exception as e:
        log(f"❌ Extração AWS ERRO: {e}")
    finally:
        _toggle_extract_buttons(True)

def _extrair_fabric():
    if not conn_fabric: return log("⚠️ Conecte no Fabric antes de extrair Fabric.")
    _toggle_extract_buttons(False)
    try:
        dt = entry_data.get().strip()
        status = parse_status(entry_status.get().strip())
        log(f"🔎 Extraindo Fabric: dt_inicio={dt} status={status}")
        df = extrair_fabric(conn_fabric, dt, status)
        df = normalizar_numericos(df, casas=4)
        _salvar(df, "fabric", dt)
    except Exception as e:
        log(f"❌ Extração Fabric ERRO: {e}")
    finally:
        _toggle_extract_buttons(True)

def _extrair_ambos():
    if not conn_pg or not conn_fabric:
        return log("⚠️ Conecte nos dois bancos antes de 'Extrair Ambos'.")
    _toggle_extract_buttons(False)
    try:
        _extrair_aws()
        _extrair_fabric()
    finally:
        _toggle_extract_buttons(True)

def _salvar(df: pd.DataFrame, prefix: str, dt_inicio: str):
    os.makedirs("out", exist_ok=True)

    # pega timestamp no formato YYYYMMDD_HHMMSS
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    # monta nomes de arquivo incluindo dt_inicio e timestamp
    csv = f"out/{prefix}_notas_{dt_inicio}_{timestamp}.csv"
    pq  = f"out/{prefix}_notas_{dt_inicio}_{timestamp}.parquet"

    # salva CSV com separador ';'
    df.to_csv(csv, index=False, sep=";")
    try:
        df.to_parquet(pq, index=False)
        log(f"✅ {prefix.upper()} extraído: {len(df)} linhas | CSV: {csv} | Parquet: {pq}")
    except Exception:
        log(f"✅ {prefix.upper()} extraído: {len(df)} linhas | CSV: {csv} (Parquet não salvo: instale pyarrow ou fastparquet)")

def _toggle_extract_buttons(enable: bool):
    (safe_enable if enable else safe_disable)(btn_ext_aws)
    (safe_enable if enable else safe_disable)(btn_ext_fab)
    (safe_enable if enable else safe_disable)(btn_ext_both)

# ---------- UI ----------
root = tk.Tk()
root.title("Conexões | PostgreSQL & Microsoft Fabric")
root.geometry("820x560")
root.configure(bg="#f7f7f7")

# PostgreSQL frame
frame_pg = tk.LabelFrame(root, text="PostgreSQL (AWS)", padx=10, pady=10, bg="#f7f7f7")
frame_pg.pack(fill="x", padx=10, pady=8)
btn_con_pg     = tk.Button(frame_pg, text="Conectar", bg="#2e7d32", fg="white", width=16, command=conectar_postgres_async)
btn_descon_pg  = tk.Button(frame_pg, text="Desconectar", bg="#757575", fg="white", width=16, command=desconectar_postgres, state=tk.DISABLED)
btn_pg_ping    = tk.Button(frame_pg, text="Testar Query (SELECT 1)", bg="#455a64", fg="white", width=22, command=ping_postgres, state=tk.DISABLED)
status_pg      = tk.Label(frame_pg, text="🔌 Desconectado", fg="gray", bg="#f7f7f7")
btn_con_pg.grid(row=0, column=0, padx=6, pady=6)
btn_descon_pg.grid(row=0, column=1, padx=6, pady=6)
btn_pg_ping.grid(row=0, column=2, padx=6, pady=6)
status_pg.grid(row=0, column=3, padx=10, pady=6, sticky="w")

# Fabric frame
frame_fab = tk.LabelFrame(root, text="Microsoft Fabric (SQL)", padx=10, pady=10, bg="#f7f7f7")
frame_fab.pack(fill="x", padx=10, pady=8)
btn_con_fab    = tk.Button(frame_fab, text="Conectar", bg="#1565c0", fg="white", width=16, command=conectar_fabric_async)
btn_descon_fab = tk.Button(frame_fab, text="Desconectar", bg="#757575", fg="white", width=16, command=desconectar_fabric, state=tk.DISABLED)
btn_fab_ping   = tk.Button(frame_fab, text="Testar Query (SELECT 1)", bg="#455a64", fg="white", width=22, command=ping_fabric, state=tk.DISABLED)
status_fabric  = tk.Label(frame_fab, text="🔌 Desconectado", fg="gray", bg="#f7f7f7")
btn_con_fab.grid(row=0, column=0, padx=6, pady=6)
btn_descon_fab.grid(row=0, column=1, padx=6, pady=6)
btn_fab_ping.grid(row=0, column=2, padx=6, pady=6)
status_fabric.grid(row=0, column=3, padx=10, pady=6, sticky="w")

# Parâmetros de extração
frame_params = tk.LabelFrame(root, text="Parâmetros de Extração", padx=10, pady=10, bg="#f7f7f7")
frame_params.pack(fill="x", padx=10, pady=8)
tk.Label(frame_params, text="Data inicial (YYYY-MM-DD):", bg="#f7f7f7").grid(row=0, column=0, sticky="w")
entry_data = tk.Entry(frame_params, width=20)
entry_data.grid(row=0, column=1, padx=6)
entry_data.insert(0, os.getenv("DT_INICIO", "2025-08-01"))

tk.Label(frame_params, text="Status (ex.: 1,3):", bg="#f7f7f7").grid(row=0, column=2, sticky="w", padx=(16,0))
entry_status = tk.Entry(frame_params, width=20)
entry_status.grid(row=0, column=3, padx=6)
entry_status.insert(0, os.getenv("STATUS_LISTA", "1"))

# Botões de extração
frame_extract = tk.LabelFrame(root, text="Ações de Extração", padx=10, pady=10, bg="#f7f7f7")
frame_extract.pack(fill="x", padx=10, pady=8)
btn_ext_aws  = tk.Button(frame_extract, text="Extrair AWS",    bg="#4e342e", fg="white", width=16, command=extrair_aws_async)
btn_ext_fab  = tk.Button(frame_extract, text="Extrair Fabric", bg="#00695c", fg="white", width=16, command=extrair_fabric_async)
btn_ext_both = tk.Button(frame_extract, text="Extrair Ambos",  bg="#7b1fa2", fg="white", width=16, command=extrair_ambos_async)
btn_ext_aws.grid(row=0, column=0, padx=6, pady=6)
btn_ext_fab.grid(row=0, column=1, padx=6, pady=6)
btn_ext_both.grid(row=0, column=2, padx=6, pady=6)

# Log box
log_frame = tk.LabelFrame(root, text="Log", padx=6, pady=6, bg="#f7f7f7")
log_frame.pack(fill="both", expand=True, padx=10, pady=8)
log_text = tk.Text(log_frame, height=12, width=100, state=tk.DISABLED)
log_text.pack(fill="both", expand=True)

footer = tk.Label(root, text="Dica: configure .env (PG_*/FABRIC_*) e os SQLs em /sql antes da extração.", bg="#f7f7f7", fg="#555")
footer.pack(pady=4)

root.mainloop()

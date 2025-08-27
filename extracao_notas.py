# extracao_notas.py
import os
import pandas as pd
from decimal import Decimal, ROUND_HALF_UP

SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")
METRICAS = ["vol","fat","fatliq","fatdol","fatbon","cc","cp","ci","cf","frete"]

def _ler_sql(nome_arquivo: str) -> str:
    path = os.path.join(SQL_DIR, nome_arquivo)
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def extrair_aws(conn_pg, dt_inicio: str, status_lista):
    """
    Lê sql/aws.sql (Postgres), com parâmetros:
      tempo_id >= %(dt_inicio)s
      status_pedido_id = ANY(%(status_lista)s)
    """
    sql = _ler_sql("aws.sql")
    df = pd.read_sql_query(sql, conn_pg, params={
        "dt_inicio": dt_inicio,
        "status_lista": status_lista
    })
    return _padronizar_cols(df)

def extrair_fabric(conn_fabric, dt_inicio: str, status_lista):
    """
    Lê sql/fabric.sql (Fabric/SQL Server). O arquivo deve conter o comentário:
      -- {{STATUS_FILTER}}
    que será substituído por: and status_pedido_id in (?,?,...)
    O parâmetro da data é o primeiro '?'
    """
    base_sql = _ler_sql("fabric.sql")
    params = [dt_inicio]
    status_clause = ""
    if status_lista:
        qmarks = ",".join(["?"] * len(status_lista))
        status_clause = f" and status_pedido_id in ({qmarks})"
        params.extend(status_lista)
    sql = base_sql.replace("-- {{STATUS_FILTER}}", status_clause)
    df = pd.read_sql(sql, conn_fabric, params=params)
    return _padronizar_cols(df)

def _padronizar_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    renames = {
        "volume_fisico_realizado":"vol",
        "faturamento_bruto_realizado":"fat",
        "faturamento_liquido_realizado":"fatliq",
        "faturamento_dolar":"fatdol",
        "faturamento_bruto_bonificado":"fatbon",
        "custo_comercializacao":"cc",
        "custo_producao_realizado":"cp",
        "custo_materiais_realizado":"ci",
        "custo_financeiro":"cf",
        "valor_frete":"frete",
    }
    for k,v in renames.items():
        if k in df.columns and v not in df.columns:
            df = df.rename(columns={k:v})
    if "nota_fiscal_id" in df.columns:
        df["nota_fiscal_id"] = pd.to_numeric(df["nota_fiscal_id"], errors="coerce").astype("Int64")
    for c in METRICAS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    cols = ["nota_fiscal_id"] + [c for c in METRICAS if c in df.columns]
    return df[cols].sort_values("nota_fiscal_id").reset_index(drop=True)

def normalizar_numericos(df: pd.DataFrame, casas: int = 4) -> pd.DataFrame:
    """
    Normaliza números para mesma escala (default 4 casas) com Decimal
    para evitar diferenças de ponto flutuante entre bancos.
    """
    df = df.copy()
    quant = Decimal("1") if casas == 0 else Decimal("0." + "0"*casas)
    for c in METRICAS:
        if c in df.columns:
            df[c] = df[c].apply(
                lambda x: float(Decimal(str(x if pd.notna(x) else 0)).quantize(quant, rounding=ROUND_HALF_UP))
            )
    return df

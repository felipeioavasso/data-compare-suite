# app.py — Comparador de Notas (Streamlit) v3
import io, os, glob
import time
import pandas as pd
import streamlit as st

METRICAS = ["vol","fat","fatliq","fatdol","fatbon","cc","cp","ci","cf","frete"]

st.set_page_config(page_title="Comparação de Notas", layout="wide")
st.title("🧾 Comparação de Notas — AWS × Fabric")

def stop_streamlit():
    """Encerra o processo do Streamlit de forma controlada."""
    # Mostra um aviso rápido antes de sair
    st.toast("🛑 Servidor sendo encerrado…")
    time.sleep(0.3)  # pequeno delay para a notificação aparecer
    os._exit(0)      # mata o processo do Streamlit imediatamente

# ---------- helpers ----------
def _latest_file(pattern: str):
    files = glob.glob(pattern)
    return max(files, key=os.path.getmtime) if files else None

def _read_csv_auto(file_or_bytes) -> pd.DataFrame:
    if isinstance(file_or_bytes, (str, os.PathLike)):
        try:
            return pd.read_csv(file_or_bytes, sep=";")
        except Exception:
            return pd.read_csv(file_or_bytes)
    else:
        try:
            return pd.read_csv(file_or_bytes, sep=";")
        except Exception:
            file_or_bytes.seek(0)
            return pd.read_csv(file_or_bytes)

def _normalizar(df: pd.DataFrame, casas: int = 4) -> pd.DataFrame:
    df = df.copy()
    if "nota_fiscal_id" in df.columns:
        df["nota_fiscal_id"] = pd.to_numeric(df["nota_fiscal_id"], errors="coerce").astype("Int64")
    for c in METRICAS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0).round(casas)
    cols = ["nota_fiscal_id"] + [c for c in METRICAS if c in df.columns]
    return df[cols].sort_values("nota_fiscal_id").reset_index(drop=True)

def _to_bytes_csv(df: pd.DataFrame, sep=";"):
    buf = io.StringIO()
    df.to_csv(buf, index=False, sep=sep)
    return buf.getvalue().encode()

# ---------- sidebar: carregar dados ----------
st.sidebar.header("Carregamento dos Dados")
auto_pick = st.sidebar.checkbox("Usar arquivos mais recentes em /out", value=True)
aws_df = fab_df = None

if auto_pick:
    aws_path = _latest_file("out/aws_notas_*.csv") or _latest_file("out/aws_notas.csv")
    fab_path = _latest_file("out/fabric_notas_*.csv") or _latest_file("out/fabric_notas.csv")
    st.sidebar.write("AWS:", aws_path or "—")
    st.sidebar.write("Fabric:", fab_path or "—")
    if aws_path: aws_df = _read_csv_auto(aws_path)
    if fab_path: fab_df = _read_csv_auto(fab_path)
else:
    aws_up = st.sidebar.file_uploader("CSV AWS", type=["csv"])
    fab_up = st.sidebar.file_uploader("CSV Fabric", type=["csv"])
    if aws_up: aws_df = _read_csv_auto(aws_up)
    if fab_up: fab_df = _read_csv_auto(fab_up)

if aws_df is None or fab_df is None:
    st.info("Carregue os dois conjuntos (AWS e Fabric) pela barra lateral ou deixe o app localizar os mais recentes em **/out**.")
    st.stop()

# ---------- parâmetros ----------
st.sidebar.header("Parâmetros")
casas = st.sidebar.number_input("Casas decimais (normalização)", min_value=0, max_value=10, value=4, step=1)
atol  = st.sidebar.number_input("Tolerância absoluta", min_value=0.0, value=0.01, step=0.01, format="%.2f")
somente_div = st.sidebar.checkbox("Mostrar apenas linhas divergentes", value=True)

# ---------- botão encerrar ----------
st.sidebar.divider()
if "confirm_stop" not in st.session_state:
    st.session_state.confirm_stop = False

if not st.session_state.confirm_stop:
    if st.sidebar.button("🛑 Encerrar servidor", use_container_width=True):
        st.session_state.confirm_stop = True
else:
    st.sidebar.warning("Confirma encerrar o servidor?")
    col1, col2 = st.sidebar.columns(2)
    if col1.button("✅ Sim", use_container_width=True):
        stop_streamlit()
    if col2.button("❌ Não", use_container_width=True):
        st.session_state.confirm_stop = False





# ---------- normalização ----------
aws_n = _normalizar(aws_df, casas)
fab_n = _normalizar(fab_df, casas)

# ---------- preparar relação AWS x Fabric ----------
aws_ren = aws_n.rename(columns={"nota_fiscal_id":"nota_fiscal_id_aws"})
fab_ren = fab_n.rename(columns={"nota_fiscal_id":"nota_fiscal_id_fabric"})

# outer join (preserva IDs dos dois lados)
merged = aws_ren.merge(
    fab_ren,
    left_on="nota_fiscal_id_aws",
    right_on="nota_fiscal_id_fabric",
    how="outer",
    suffixes=("_aws","_fabric")
)

# ---------- diferenças (aws - fabric) ----------
diff_cols = []
for m in METRICAS:
    a = f"{m}_aws"; b = f"{m}_fabric"
    if a not in merged.columns: merged[a] = 0.0
    if b not in merged.columns: merged[b] = 0.0
    merged[f"diff_{m}"] = merged[a].fillna(0) - merged[b].fillna(0)
    diff_cols.append(f"diff_{m}")

merged["diverge"] = (merged[diff_cols].abs() > atol).any(axis=1)

diff_table = merged[["nota_fiscal_id_aws","nota_fiscal_id_fabric"] + diff_cols + ["diverge"]].copy()
if somente_div:
    diff_table = diff_table[diff_table["diverge"]]

# ---------- IDs que existem só em um lado ----------
so_fabric = merged[merged["nota_fiscal_id_aws"].isna()][["nota_fiscal_id_fabric"]].dropna().astype("Int64")
so_aws    = merged[merged["nota_fiscal_id_fabric"].isna()][["nota_fiscal_id_aws"]].dropna().astype("Int64")
so_fabric = so_fabric.rename(columns={"nota_fiscal_id_fabric":"nota_fiscal_id"})
so_aws    = so_aws.rename(columns={"nota_fiscal_id_aws":"nota_fiscal_id"})

# ---------- abas ----------
tab_diff, tab_fabric, tab_aws = st.tabs(["🔎 Diferenças", "📘 Fabric (dados)", "📗 AWS (dados)"])

with tab_diff:
    st.subheader("Diferenças (AWS − Fabric)")
    st.caption(f"Tolerância: {atol:.2f} | Linhas exibidas: {len(diff_table)}")
    #st.dataframe(diff_table, use_container_width=True)
    # destaque de linhas com diferenças grandes (>= 1)
    THRESHOLD = 1.0

    def highlight_big_diffs(row):
        has_big = any(
            abs(row[c]) >= THRESHOLD
            for c in row.index
            if str(c).startswith("diff_") and pd.notna(row[c])
        )
        style = 'background-color: #fff3b0; color: black' if has_big else ''
        return [style for _ in row]

    styled = diff_table.style.apply(highlight_big_diffs, axis=1)
    st.dataframe(styled, use_container_width=True)

    st.download_button("⬇️ Baixar diferenças (CSV ;)", _to_bytes_csv(diff_table), "diferencas_aws_fabric.csv", "text/csv")

    st.divider()
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Somente no **Fabric**")
        st.caption(f"Total: {len(so_fabric)}")
        st.dataframe(so_fabric, use_container_width=True, height=260)
        st.download_button("⬇️ Baixar (só no Fabric) CSV", _to_bytes_csv(so_fabric), "so_no_fabric.csv", "text/csv")

    with col2:
        st.markdown("### Somente na **AWS**")
        st.caption(f"Total: {len(so_aws)}")
        st.dataframe(so_aws, use_container_width=True, height=260)
        st.download_button("⬇️ Baixar (só na AWS) CSV", _to_bytes_csv(so_aws), "so_na_aws.csv", "text/csv")

with tab_fabric:
    st.subheader("Fabric (dados normalizados)")
    st.dataframe(fab_n, use_container_width=True)
    st.download_button("⬇️ Baixar Fabric (CSV ;)", _to_bytes_csv(fab_n), "fabric_dados.csv", "text/csv")

with tab_aws:
    st.subheader("AWS (dados normalizados)")
    st.dataframe(aws_n, use_container_width=True)
    st.download_button("⬇️ Baixar AWS (CSV ;)", _to_bytes_csv(aws_n), "aws_dados.csv", "text/csv")

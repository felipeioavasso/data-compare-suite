✨ Autor
✨ Desenvolvido por Felipe Ioavasso Vieira dos Santos
✨



# NF Compare (AWS x Fabric)

Ferramenta para **extração, comparação e análise de notas fiscais** entre dois ambientes distintos:
- **AWS (PostgreSQL)**
- **Microsoft Fabric (SQL Server)**

O projeto inclui:
- **Interface Tkinter** (`gui_conexoes.py`) para gerenciar conexões, extrações e iniciar/parar o Streamlit.
- **Aplicação Streamlit** (`app.py`) para análise visual das diferenças entre bases.
- **SQLs parametrizados** em `sql/` para facilitar a extração.

---

## 📂 Estrutura do projeto

extracao_notas_fiscais/
├── app.py                              # Dashboard comparativo (Streamlit)
├── extracao_notas.py                   # Funções de extração e normalização
├── gui_conexoes.py                     # GUI (Tkinter) para conexões, extração e controle do Streamlit
├── sql/                                # Scripts SQL para AWS e Fabric
├── out/                                # Saída de arquivos CSV/Parquet (ignorada no git)
├── requirements.txt                    # Dependências do projeto
└── .env                                # Variáveis de ambiente (não versionado)


---

## ⚙️ Pré-requisitos

- Python 3.9+  
- Banco de dados acessíveis (AWS PostgreSQL e Fabric SQL Server)  
- [ODBC Driver 18 for SQL Server](https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server)

---

## 📦 Instalação

Clone o repositório:

```bash
git clone https://github.com/seu-usuario/nf-compare-aws-fabric.git
cd nf-compare-aws-fabric

Crie o ambiente virtual e instale dependências:
python -m venv .venv
source .venv/bin/activate   # Linux/Mac
.venv\Scripts\activate      # Windows

pip install -r requirements.txt



🔑 Configuração
Crie um arquivo .env na raiz com suas credenciais:
# PostgreSQL (AWS)
PG_HOST=seu-host.aws.com
PG_PORT=5432
PG_DB=postgres
PG_USER=usuario
PG_PASSWORD=senha

# Microsoft Fabric
FABRIC_SERVER=seu-endpoint.sql.fabric.microsoft.com
FABRIC_DB=nome_do_banco
FABRIC_USER=usuario
FABRIC_PASSWORD=senha

# Data inicial e status padrão
DT_INICIO=2025-08-01
STATUS_LISTA=1,3

# Porta do Streamlit (opcional)
STREAMLIT_PORT=8501


▶️ Uso
Execute:
python gui_conexoes.py

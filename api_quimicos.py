"""
API de Cotações Químicas — SQL Server
Requer: pip install flask pyodbc pandas
Execução: python api_quimicos.py
Endpoint: http://localhost:8051/api/precos
"""

import os
import pyodbc
import pandas as pd
from flask import Flask, jsonify

# ─── CONFIGURAÇÃO ────────────────────────────────────────────────────────────

SQL_SERVER   = "CORPORATIVOMTCS"
SQL_DATABASE = "corporativomtcs"
SQL_TABLE    = "dbo.historico_precos_quimicos"
SQL_DRIVER   = "ODBC Driver 17 for SQL Server"
PORT         = 8051

CONN_STRING = (
    f"DRIVER={{{SQL_DRIVER}}};"
    f"SERVER={SQL_SERVER};"
    f"DATABASE={SQL_DATABASE};"
    "Trusted_Connection=yes;"
)

CORES_GRUPOS = {
    "Ácidos":                   "#06b6d4",
    "Álcoois":                  "#8b5cf6",
    "Outros Químicos":          "#f59e0b",
    "Polímeros/PEGs":           "#10b981",
    "Tensoativos/Polisorbatos": "#f43f5e",
    "Edulcorantes/Outros":      "#fb923c",
}
CORES_FALLBACK = ["#00ff41", "#ff4444", "#00aaff", "#ffaa00", "#cc44ff", "#00ccaa", "#ff7700", "#ff44aa"]

# ─── APP ─────────────────────────────────────────────────────────────────────

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

app = Flask(__name__)


@app.route("/")
def index():
    html_path = os.path.join(BASE_DIR, "dashboard_quimicos.html")
    with open(html_path, "r", encoding="utf-8") as f:
        return f.read(), 200, {"Content-Type": "text/html; charset=utf-8"}


def _resposta(data, status=200):
    resp = jsonify(data)
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.status_code = status
    return resp


@app.route("/api/precos")
def api_precos():
    try:
        conn = pyodbc.connect(CONN_STRING, timeout=10)
        df   = pd.read_sql(f"SELECT * FROM {SQL_TABLE} ORDER BY Data", conn)
        conn.close()
    except Exception as e:
        return _resposta({"erro": True, "msg": str(e)}, 500)

    df["Data"] = pd.to_datetime(df["Data"])
    datas      = sorted(df["Data"].dt.strftime("%Y-%m-%d").unique().tolist())

    color_idx = 0
    grupos    = {}

    for grp in sorted(df["Grupo"].unique()):
        color = CORES_GRUPOS.get(grp, CORES_FALLBACK[color_idx % len(CORES_FALLBACK)])
        if grp not in CORES_GRUPOS:
            color_idx += 1

        df_grp   = df[df["Grupo"] == grp]
        produtos = {}

        for item in sorted(df_grp["Item"].unique()):
            df_item    = df_grp[df_grp["Item"] == item]
            date_price = dict(zip(df_item["Data"].dt.strftime("%Y-%m-%d"), df_item["Preco"]))

            serie  = []
            ultimo = None
            for d in datas:
                if d in date_price:
                    ultimo = float(date_price[d])
                serie.append(ultimo if ultimo is not None else 0.0)

            produtos[item] = serie

        grupos[grp] = {"color": color, "produtos": produtos}

    return _resposta({"dates": datas, "grupos": grupos})


@app.route("/health")
def health():
    return _resposta({"status": "ok", "server": SQL_SERVER, "database": SQL_DATABASE})


# ─── ENTRY POINT ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"API rodando em http://localhost:{PORT}/api/precos")
    app.run(host="0.0.0.0", port=PORT, debug=False)

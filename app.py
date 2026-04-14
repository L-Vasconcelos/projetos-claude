"""
Dashboard de Cotações Químicas
Requer: pip install dash plotly pandas pyodbc
Execução: python app.py
Acesso: http://localhost:8050
"""

import pyodbc
import pandas as pd
from dash import Dash, dcc, html, Input, Output, callback_context
import plotly.graph_objects as go
from datetime import datetime

# ─── CONFIGURAÇÃO ────────────────────────────────────────────────────────────

# Fonte de dados: SQL Server (Windows Authentication)
SQL_SERVER     = "CORPORATIVOMTCS"
SQL_DATABASE   = "corporativomtcs"
SQL_TABLE      = "dbo.historico_precos_quimicos"
SQL_DRIVER     = "ODBC Driver 17 for SQL Server"

CONN_STRING = (
    f"DRIVER={{{SQL_DRIVER}}};"
    f"SERVER={SQL_SERVER};"
    f"DATABASE={SQL_DATABASE};"
    "Trusted_Connection=yes;"
)

# Polling: verifica mudança no banco a cada N segundos
POLL_INTERVAL_MS = 30_000  # 30 segundos

# ─── PALETA ──────────────────────────────────────────────────────────────────

CORES = {
    "bg":        "#0a0e1a",
    "card":      "#111827",
    "card2":     "#1a2235",
    "border":    "#1e2d45",
    "text":      "#e2e8f0",
    "muted":     "#64748b",
    "accent":    "#06b6d4",
    "up":        "#10b981",
    "down":      "#f43f5e",
    "neutral":   "#94a3b8",
    "grupos": {
        "Ácidos":                  "#06b6d4",
        "Álcoois":                 "#8b5cf6",
        "Outros Químicos":         "#f59e0b",
        "Polímeros/PEGs":          "#10b981",
        "Tensoativos/Polisorbatos": "#f43f5e",
        "Edulcorantes/Outros":     "#fb923c",
    },
}

# ─── DADOS ───────────────────────────────────────────────────────────────────

_cache_contagem = None


def carregar_dados() -> tuple[pd.DataFrame, str, bool]:
    """
    Retorna (df, fonte_str, mudou).
    Lê direto do SQL Server via Windows Authentication.
    """
    global _cache_contagem

    try:
        conn = pyodbc.connect(CONN_STRING, timeout=10)
    except Exception as e:
        return pd.DataFrame(), f"Erro de conexão: {e}", False

    try:
        df = pd.read_sql(f"SELECT * FROM {SQL_TABLE} ORDER BY Data", conn)
    except Exception as e:
        conn.close()
        return pd.DataFrame(), f"Erro na query: {e}", False

    conn.close()

    df["Data"] = pd.to_datetime(df["Data"])
    nova_contagem = len(df)
    mudou = nova_contagem != _cache_contagem
    _cache_contagem = nova_contagem

    return df, f"SQL Server · {SQL_SERVER}/{SQL_DATABASE}", mudou


# ─── LÓGICA DE NEGÓCIO ───────────────────────────────────────────────────────

def calcular_variacao(df: pd.DataFrame) -> pd.DataFrame:
    """Para cada item, retorna preço atual, anterior, variação % e variação R$."""
    if df.empty:
        return df

    datas = sorted(df["Data"].unique())
    data_atual = datas[-1]
    data_ant = datas[-2] if len(datas) > 1 else None

    atual = df[df["Data"] == data_atual][["Item", "Preco", "Unidade", "Grupo", "Fonte"]].copy()
    atual.columns = ["Item", "Preco_Atual", "Unidade", "Grupo", "Fonte"]

    if data_ant is not None:
        ant = df[df["Data"] == data_ant][["Item", "Preco"]].copy()
        ant.columns = ["Item", "Preco_Ant"]
        result = atual.merge(ant, on="Item", how="left")
    else:
        result = atual.copy()
        result["Preco_Ant"] = None

    result["Var_Pct"] = (
        (result["Preco_Atual"] - result["Preco_Ant"]) / result["Preco_Ant"] * 100
    ).round(2)
    result["Var_Abs"] = (result["Preco_Atual"] - result["Preco_Ant"]).round(2)
    result["Data_Atual"] = data_atual
    result["Data_Ant"] = data_ant
    return result.sort_values(["Grupo", "Item"])


def preco_por_kg(preco_mt: float, unidade: str) -> tuple[float, str]:
    """Converte MT → kg (÷1000). Drum 300kg permanece como está."""
    if unidade == "MT":
        return round(preco_mt / 1000, 4), "kg"
    return preco_mt, unidade


# ─── COMPONENTES ─────────────────────────────────────────────────────────────

def card_produto(row: dict) -> html.Div:
    preco, unid = preco_por_kg(row["Preco_Atual"], row["Unidade"])
    cor_grupo = CORES["grupos"].get(row["Grupo"], CORES["accent"])
    var = row["Var_Pct"]

    if pd.isna(var):
        var_cor = CORES["neutral"]
        var_txt = "—"
        seta = ""
    elif var > 0:
        var_cor = CORES["up"]
        var_txt = f"+{var:.2f}%"
        seta = "▲"
    elif var < 0:
        var_cor = CORES["down"]
        var_txt = f"{var:.2f}%"
        seta = "▼"
    else:
        var_cor = CORES["neutral"]
        var_txt = "0.00%"
        seta = "—"

    return html.Div(
        style={
            "background": CORES["card"],
            "border": f"1px solid {CORES['border']}",
            "borderLeft": f"3px solid {cor_grupo}",
            "borderRadius": "8px",
            "padding": "12px 14px",
            "display": "flex",
            "flexDirection": "column",
            "gap": "6px",
            "minWidth": "0",
        },
        children=[
            html.Div(
                row["Item"],
                style={
                    "fontSize": "11px",
                    "fontWeight": "600",
                    "color": CORES["text"],
                    "letterSpacing": "0.04em",
                    "overflow": "hidden",
                    "textOverflow": "ellipsis",
                    "whiteSpace": "nowrap",
                },
            ),
            html.Div(
                style={"display": "flex", "justifyContent": "space-between", "alignItems": "baseline"},
                children=[
                    html.Span(
                        f"R$ {preco:,.4f}/{unid}",
                        style={"fontSize": "15px", "fontWeight": "700", "color": CORES["text"]},
                    ),
                    html.Span(
                        f"{seta} {var_txt}",
                        style={"fontSize": "13px", "fontWeight": "700", "color": var_cor},
                    ),
                ],
            ),
            html.Div(
                row["Fonte"] if not pd.isna(row.get("Fonte", "")) else "",
                style={"fontSize": "9px", "color": CORES["muted"], "overflow": "hidden", "textOverflow": "ellipsis", "whiteSpace": "nowrap"},
            ),
        ],
    )


def grafico_grupo(df: pd.DataFrame, grupo: str) -> dcc.Graph:
    itens = df[df["Grupo"] == grupo]["Item"].unique()

    fig = go.Figure()

    for item in sorted(itens):
        serie = df[df["Item"] == item].sort_values("Data")
        if serie.empty:
            continue
        precos_kg = [preco_por_kg(p, u)[0] for p, u in zip(serie["Preco"], serie["Unidade"])]
        fig.add_trace(
            go.Scatter(
                x=serie["Data"],
                y=precos_kg,
                mode="lines+markers",
                name=item,
                line=dict(width=2),
                marker=dict(size=5),
                hovertemplate=(
                    "<b>%{fullData.name}</b><br>"
                    "Data: %{x|%d/%m/%Y}<br>"
                    "R$ %{y:,.4f}<extra></extra>"
                ),
            )
        )

    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="'JetBrains Mono', monospace", size=10, color=CORES["muted"]),
        margin=dict(l=8, r=8, t=8, b=8),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
            font=dict(size=9),
            bgcolor="rgba(0,0,0,0)",
        ),
        xaxis=dict(
            gridcolor=CORES["border"],
            linecolor=CORES["border"],
            tickformat="%d/%m",
            tickfont=dict(size=9),
        ),
        yaxis=dict(
            gridcolor=CORES["border"],
            linecolor=CORES["border"],
            tickprefix="R$ ",
            tickfont=dict(size=9),
        ),
        hovermode="x unified",
        height=220,
    )

    return dcc.Graph(
        figure=fig,
        config={"displayModeBar": False},
        style={"width": "100%"},
    )


# ─── LAYOUT ──────────────────────────────────────────────────────────────────

app = Dash(__name__, title="Citações Químicas")
app.layout = html.Div(
    id="root",
    style={
        "background": CORES["bg"],
        "minHeight": "100vh",
        "fontFamily": "'JetBrains Mono', 'Fira Code', monospace",
        "color": CORES["text"],
        "padding": "0",
        "margin": "0",
    },
    children=[
        dcc.Interval(id="intervalo", interval=POLL_INTERVAL_MS, n_intervals=0),
        dcc.Store(id="store-dados"),

        # HEADER
        html.Div(
            style={
                "background": CORES["card"],
                "borderBottom": f"1px solid {CORES['border']}",
                "padding": "14px 24px",
                "display": "flex",
                "justifyContent": "space-between",
                "alignItems": "center",
                "position": "sticky",
                "top": "0",
                "zIndex": "100",
            },
            children=[
                html.Div(
                    children=[
                        html.Span("⬡ ", style={"color": CORES["accent"], "fontSize": "18px"}),
                        html.Span(
                            "CITAÇÕES QUÍMICAS",
                            style={"fontWeight": "800", "fontSize": "15px", "letterSpacing": "0.12em"},
                        ),
                        html.Span(
                            " · Importados",
                            style={"color": CORES["muted"], "fontSize": "12px", "marginLeft": "6px"},
                        ),
                    ]
                ),
                html.Div(
                    id="header-status",
                    style={"fontSize": "11px", "color": CORES["muted"]},
                ),
            ],
        ),

        # CORPO PRINCIPAL
        html.Div(
            id="corpo",
            style={"padding": "20px 24px", "maxWidth": "1600px", "margin": "0 auto"},
        ),
    ],
)


# ─── CALLBACKS ───────────────────────────────────────────────────────────────

@app.callback(
    Output("store-dados", "data"),
    Output("header-status", "children"),
    Input("intervalo", "n_intervals"),
)
def atualizar_store(n):
    df, fonte, _ = carregar_dados()
    if df.empty:
        # Retorna sentinel de erro para garantir que renderizar seja disparado
        return '{"erro": true, "msg": "' + fonte.replace('"', "'") + '"}', f"❌ {fonte}"

    datas = sorted(df["Data"].unique())
    data_ref = pd.to_datetime(datas[-1]).strftime("%d/%m/%Y")
    agora = datetime.now().strftime("%H:%M:%S")
    status = f"Ref: {data_ref}  ·  Fonte: {fonte}  ·  Sync: {agora}"

    return df.to_json(date_format="iso", orient="records"), status


@app.callback(
    Output("corpo", "children"),
    Input("store-dados", "data"),
)
def renderizar(json_data):
    if json_data is None:
        return html.Div("Aguardando dados...", style={"color": CORES["muted"], "padding": "40px"})

    # Verifica sentinel de erro
    import json
    try:
        check = json.loads(json_data)
        if isinstance(check, dict) and check.get("erro"):
            return html.Div(
                check.get("msg", "Erro ao carregar dados"),
                style={"color": CORES["down"], "padding": "40px"},
            )
    except (ValueError, KeyError):
        pass

    df = pd.read_json(json_data, orient="records")
    df["Data"] = pd.to_datetime(df["Data"])

    var_df = calcular_variacao(df)
    grupos = sorted(df["Grupo"].unique())

    # ── KPIs do topo ──────────────────────────────────────────────────────
    n_alta = int((var_df["Var_Pct"] > 0).sum())
    n_baixa = int((var_df["Var_Pct"] < 0).sum())
    n_estavel = int((var_df["Var_Pct"] == 0).sum())

    top_alta = var_df.nlargest(1, "Var_Pct").iloc[0] if n_alta else None
    top_baixa = var_df.nsmallest(1, "Var_Pct").iloc[0] if n_baixa else None

    def kpi(label, valor, cor):
        return html.Div(
            style={
                "background": CORES["card"],
                "border": f"1px solid {CORES['border']}",
                "borderRadius": "8px",
                "padding": "12px 18px",
                "display": "flex",
                "flexDirection": "column",
                "gap": "2px",
                "minWidth": "130px",
            },
            children=[
                html.Div(label, style={"fontSize": "9px", "color": CORES["muted"], "letterSpacing": "0.1em", "textTransform": "uppercase"}),
                html.Div(str(valor), style={"fontSize": "28px", "fontWeight": "800", "color": cor}),
            ],
        )

    def destaque(label, row, cor):
        if row is None:
            return html.Div()
        preco, unid = preco_por_kg(row["Preco_Atual"], row["Unidade"])
        return html.Div(
            style={
                "background": CORES["card"],
                "border": f"1px solid {CORES['border']}",
                "borderLeft": f"3px solid {cor}",
                "borderRadius": "8px",
                "padding": "12px 18px",
                "flex": "1",
            },
            children=[
                html.Div(label, style={"fontSize": "9px", "color": CORES["muted"], "letterSpacing": "0.1em", "textTransform": "uppercase"}),
                html.Div(row["Item"], style={"fontSize": "13px", "fontWeight": "700", "color": CORES["text"], "marginTop": "4px"}),
                html.Div(
                    f"R$ {preco:,.4f}/{unid}  ·  {row['Var_Pct']:+.2f}%",
                    style={"fontSize": "12px", "color": cor, "marginTop": "2px"},
                ),
            ],
        )

    barra_kpi = html.Div(
        style={"display": "flex", "gap": "12px", "marginBottom": "20px", "flexWrap": "wrap", "alignItems": "stretch"},
        children=[
            kpi("PRODUTOS", len(var_df), CORES["text"]),
            kpi("EM ALTA", n_alta, CORES["up"]),
            kpi("EM BAIXA", n_baixa, CORES["down"]),
            kpi("ESTÁVEIS", n_estavel, CORES["neutral"]),
            destaque("▲ MAIOR ALTA", top_alta, CORES["up"]),
            destaque("▼ MAIOR BAIXA", top_baixa, CORES["down"]),
        ],
    )

    # ── Seções por grupo ───────────────────────────────────────────────────
    secoes = []
    for grupo in grupos:
        cor_grupo = CORES["grupos"].get(grupo, CORES["accent"])
        itens_grupo = var_df[var_df["Grupo"] == grupo]

        cards = html.Div(
            style={
                "display": "grid",
                "gridTemplateColumns": "repeat(auto-fill, minmax(220px, 1fr))",
                "gap": "10px",
                "marginBottom": "16px",
            },
            children=[card_produto(row) for _, row in itens_grupo.iterrows()],
        )

        grafico = grafico_grupo(df, grupo)

        secao = html.Div(
            style={
                "background": CORES["card2"],
                "border": f"1px solid {CORES['border']}",
                "borderTop": f"2px solid {cor_grupo}",
                "borderRadius": "8px",
                "padding": "16px",
                "marginBottom": "16px",
            },
            children=[
                html.Div(
                    style={"display": "flex", "alignItems": "center", "marginBottom": "14px", "gap": "10px"},
                    children=[
                        html.Span("●", style={"color": cor_grupo, "fontSize": "16px"}),
                        html.Span(
                            grupo.upper(),
                            style={"fontWeight": "700", "fontSize": "12px", "letterSpacing": "0.1em", "color": CORES["text"]},
                        ),
                        html.Span(
                            f"{len(itens_grupo)} itens",
                            style={"fontSize": "10px", "color": CORES["muted"]},
                        ),
                    ],
                ),
                cards,
                html.Div(
                    html.Details(
                        children=[
                            html.Summary(
                                "📈 Histórico de evolução",
                                style={"cursor": "pointer", "fontSize": "11px", "color": CORES["muted"], "marginBottom": "8px"},
                            ),
                            grafico,
                        ],
                        style={"borderTop": f"1px solid {CORES['border']}", "paddingTop": "10px"},
                    )
                ),
            ],
        )
        secoes.append(secao)

    return html.Div([barra_kpi] + secoes)


# ─── ENTRY POINT ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=8050)

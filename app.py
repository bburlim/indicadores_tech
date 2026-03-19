"""
Indicadores de Eficiência de Tecnologia — Streamlit App
Lê CSV exportado do Jira e exibe o dashboard interativo com filtros.
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import datetime

from dashboard import (
    load_csv,
    get_months,
    label_mes,
    throughput_mensal,
    abertura_mensal,
    backlog_por_mes,
    percentil85_mensal,
    percentil85,
    desvio_padrao_mensal,
    vazao_qualificada_mensal,
    vazao_por_equipe_mensal,
    flow_efficiency_mensal,
    retrabalho_mensal,
    saude_backlog_mensal,
    tempo_por_status_total,
    infer_status_names,
    ACTIVE_STATUS_IDS,
    DONE_STATUS_IDS,
    COLORS,
    TEMPLATE,
    BENCH_RETRABALHO,
    BENCH_FLUXO,
    PESO_DEFEITO,
    PESO_HISTORIA_ATE_1_DIA,
    PESO_HISTORIA_1_3_DIAS,
    PESO_HISTORIA_4_10_DIAS,
    PESO_HISTORIA_11_MAIS_DIAS,
)

# ─────────────────────────────────────────────
# CONFIG DA PÁGINA
# ─────────────────────────────────────────────

st.set_page_config(
    page_title="Indicadores de Eficiência de Tecnologia",
    page_icon="📊",
    layout="wide",
)

st.markdown("""
<style>
  .block-container { padding-top: 1.5rem; }
  .metric-card {
    background: #5B5FCF; color: white; border-radius: 10px;
    padding: 16px 20px; text-align: center;
  }
  .metric-card .val { font-size: 2rem; font-weight: bold; }
  .metric-card .lbl { font-size: 0.8rem; margin-top: 2px; opacity: .85; }
  .section-divider { border-top: 3px solid #5B5FCF; margin: 2rem 0 1rem; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

HEIGHT = 380

def kpi(label: str, value: str, delta: str = "", color: str = "#5B5FCF"):
    st.markdown(f"""
    <div class="metric-card" style="background:{color}">
      <div class="val">{value}</div>
      <div class="lbl">{label}</div>
      {'<div style="font-size:.75rem;opacity:.7">'+delta+'</div>' if delta else ''}
    </div>""", unsafe_allow_html=True)


def bar_chart(title, x_labels, series: dict, stacked=False, text_vals=True):
    fig = go.Figure()
    palette = list(COLORS.values())
    for i, (name, vals) in enumerate(series.items()):
        color = {"Defeito": COLORS["defeito"], "História": COLORS["historia"],
                 "Itens Abertos": COLORS["abertura"], "Itens Entregues": COLORS["entregue"],
                 "Lead Time P85": COLORS["lead_time"], "Cycle Time P85": COLORS["cycle_time"],
                 "Não Atendido": COLORS["sla_nao"]}.get(name, palette[i % len(palette)])
        trace = go.Bar(
            x=x_labels, y=vals, name=name, marker_color=color,
            text=[f"{v:.1f}" if isinstance(v, float) else str(v) for v in vals] if text_vals else None,
            textposition="outside" if not stacked else "inside",
        )
        fig.add_trace(trace)
    fig.update_layout(
        title=title,
        barmode="stack" if stacked else "group",
        template=TEMPLATE,
        height=HEIGHT,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(t=60, b=40, l=40, r=20),
    )
    return fig


def line_chart(title, x_labels, series: dict, bench_val=None, bench_label=""):
    fig = go.Figure()
    palette = [COLORS["retrabalho"], COLORS["fluxo"], COLORS["abertura"]]
    for i, (name, vals) in enumerate(series.items()):
        fig.add_scatter(
            x=x_labels, y=vals, mode="lines+markers+text",
            text=[f"{v:.1f}" for v in vals],
            textposition="top center",
            fill="tozeroy" if i == 0 else None,
            line=dict(color=palette[i % len(palette)]),
            name=name,
        )
    if bench_val is not None:
        fig.add_hline(
            y=bench_val,
            line=dict(color=COLORS["bench_line"], dash="dash", width=1.5),
            annotation_text=bench_label,
        )
    fig.update_layout(
        title=title, template=TEMPLATE, height=HEIGHT,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(t=60, b=40, l=40, r=20),
    )
    return fig


def pivot_table(pivot: pd.DataFrame, meses: list[str]):
    cols = [m for m in meses if m in pivot.columns]
    if pivot.empty or not cols:
        return None
    df_show = pivot[cols].copy()
    df_show.columns = [label_mes(m) for m in cols]
    df_show.index.name = "Equipe"
    return df_show.reset_index()


# ─────────────────────────────────────────────
# FILTROS & CARREGAMENTO
# ─────────────────────────────────────────────

import os
# import tempfile
# from sharepoint import secrets_configured, get_secrets, load_from_sharepoint
from jira_api import (
    jira_secrets_configured, get_jira_secrets, load_from_jira, test_connection, debug_jql
)

with st.sidebar:
    st.image("https://cdn-icons-png.flaticon.com/512/2920/2920244.png", width=48)
    st.title("Indicadores Tech")
    st.divider()

    # ── Fonte de dados ───────────────────────────────
    jira_ok = jira_secrets_configured()
    # sp_ok   = secrets_configured()
    df_full = None
    fonte   = None

    # 1. Jira API
    if jira_ok:
        st.success("🟠 Jira API configurada")
        col_r, col_i = st.columns([2, 1])
        with col_r:
            if st.button("🔄 Atualizar dados", use_container_width=True, key="refresh_jira"):
                load_from_jira.clear()
                st.rerun()
        with col_i:
            st.caption("Cache: 1h")

        try:
            with st.spinner("Buscando issues do Jira..."):
                secrets = get_jira_secrets()
                df_full = load_from_jira(**secrets)
            if df_full.empty:
                st.warning("⚠️ API do Jira conectada, mas nenhum issue retornado. Verifique o JQL nas secrets.")
                if st.button("🔍 Debug: testar JQL agora"):
                    info = debug_jql(
                        secrets["jira_url"], secrets["email"],
                        secrets["api_token"], secrets["jql"],
                    )
                    st.json(info)
                df_full = None
            else:
                fonte = "jira"
                st.caption(f"📋 {len(df_full)} issues · Jira API")
                with st.expander("🔍 Debug: campos descobertos"):
                    from jira_api import discover_fields
                    import requests as _req
                    from requests.auth import HTTPBasicAuth as _BA
                    fm = discover_fields(secrets["jira_url"], secrets["email"], secrets["api_token"])
                    st.write("**Campos mapeados:**", fm)
                    tis_ok = df_full["time_in_status"].notna() & (df_full["time_in_status"] != "")
                    st.write(f"**time_in_status preenchido:** {tis_ok.sum()} / {len(df_full)}")
                    st.write(f"**cycle_time não-nulo:** {df_full['cycle_time'].notna().sum()} / {len(df_full)}")
                    st.write(f"**equipe preenchida:** {(df_full['equipe'] != '').sum()} / {len(df_full)}")
                    if tis_ok.sum() > 0:
                        st.write("**Exemplo TIS:**", df_full.loc[tis_ok, "time_in_status"].iloc[0][:120])
                    st.write("**Todos os campos custom (para achar team):**")
                    _r = _req.get(f"{secrets['jira_url']}/rest/api/3/field",
                                  auth=_BA(secrets["email"], secrets["api_token"]),
                                  headers={"Accept": "application/json"}, timeout=15)
                    if _r.ok:
                        _custom = sorted([f["name"] for f in _r.json() if f.get("custom")],
                                         key=str.lower)
                        st.write(_custom)
        except Exception as e:
            st.error(f"Erro na API do Jira:\n{e}")
            df_full = None

    # 2. SharePoint (fallback via CSV) — desativado temporariamente
    # if df_full is None and sp_ok:
    #     try:
    #         sp = get_secrets()
    #         csv_bytes = load_from_sharepoint(**sp)
    #         with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
    #             tmp.write(csv_bytes.read())
    #             tmp_path = tmp.name
    #         df_full = load_csv(tmp_path)
    #         os.unlink(tmp_path)
    #         fonte = "sharepoint"
    #     except Exception as e:
    #         st.error(f"Erro ao acessar SharePoint:\n{e}")

    # 3. Upload manual de CSV — desativado temporariamente
    # st.divider()
    # uploaded = st.file_uploader("📂 Upload manual do CSV", type="csv")
    # if uploaded:
    #     with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
    #         tmp.write(uploaded.read())
    #         tmp_path = tmp.name
    #     df_full = load_csv(tmp_path)
    #     os.unlink(tmp_path)
    #     fonte = "upload"

    # 4. CSV local (desenvolvimento) — desativado temporariamente
    # if df_full is None:
    #     default_csv = os.path.join(os.path.dirname(__file__), "data", "jira.csv")
    #     if os.path.exists(default_csv):
    #         df_full = load_csv(default_csv)
    #         fonte = "local"

    if not jira_ok:
        st.info("Configure as credenciais do Jira nas secrets para carregar os dados.")

    if df_full is None:
        st.warning("Nenhuma fonte de dados disponível.")
        st.stop()

    st.divider()
    st.subheader("Filtros")

    # Período
    all_months = get_months(df_full)
    if not all_months:
        st.error("Nenhum dado de data encontrado.")
        st.stop()

    month_options = all_months
    month_labels  = [label_mes(m) + " " + m[:4] for m in month_options]

    col_ini, col_fim = st.columns(2)
    with col_ini:
        idx_ini = st.selectbox("De", range(len(month_labels)),
                                format_func=lambda i: month_labels[i], index=0)
    with col_fim:
        idx_fim = st.selectbox("Até", range(len(month_labels)),
                                format_func=lambda i: month_labels[i], index=len(month_labels)-1)

    mes_ini = month_options[idx_ini]
    mes_fim = month_options[idx_fim]

    # Equipe
    equipes = sorted(df_full["equipe"].dropna().unique().tolist())
    equipes = [e for e in equipes if e.strip()]
    equipe_sel = st.multiselect("Equipe", equipes, default=equipes) if equipes else equipes

    # Tipo
    tipo_sel = st.multiselect(
        "Tipo de Item",
        ["Defeito", "História", "Outro"],
        default=["Defeito", "História"],
    )

    st.divider()
    st.caption("Gerado com Python + Streamlit")

# ─────────────────────────────────────────────
# APLICAR FILTROS
# ─────────────────────────────────────────────

df = df_full.copy()

# Filtro de período (por criação E resolução dentro do intervalo)
dt_ini = datetime.strptime(mes_ini, "%Y-%m")
dt_fim_raw = datetime.strptime(mes_fim, "%Y-%m")
if dt_fim_raw.month == 12:
    dt_fim = dt_fim_raw.replace(year=dt_fim_raw.year + 1, month=1, day=1)
else:
    dt_fim = dt_fim_raw.replace(month=dt_fim_raw.month + 1, day=1)

# Mantém itens criados dentro do período OU com resolução dentro do período
mask_periodo = (
    (df["criado"].notna() & (df["criado"] >= dt_ini) & (df["criado"] < dt_fim)) |
    (df["resolvido"].notna() & (df["resolvido"] >= dt_ini) & (df["resolvido"] < dt_fim))
)
df = df[mask_periodo]

# Filtro de equipe
if equipe_sel and equipes:
    df = df[df["equipe"].isin(equipe_sel) | df["equipe"].isna() | (df["equipe"] == "")]

# Filtro de tipo
if tipo_sel:
    df = df[df["tipo_class"].isin(tipo_sel)]

if df.empty:
    st.warning("Nenhum item encontrado com os filtros selecionados.")
    st.stop()

meses = [m for m in all_months if mes_ini <= m <= mes_fim]
labels = [label_mes(m) for m in meses]

# ─────────────────────────────────────────────
# CABEÇALHO
# ─────────────────────────────────────────────

st.markdown("""
<div style="background:#5B5FCF;color:white;text-align:center;
            padding:16px;border-radius:8px;font-size:1.4rem;font-weight:bold;margin-bottom:1.5rem">
  📊 Indicadores de Eficiência de Tecnologia
</div>
""", unsafe_allow_html=True)

tab_prod, tab_qual, tab_vel = st.tabs([
    "📦 Produtividade", "🐛 Qualidade", "⏱ Velocidade"
])

# ═══════════════════════════════════════════════
# ABA PRODUTIVIDADE
# ═══════════════════════════════════════════════

with tab_prod:
    st.subheader("Estamos conseguindo entregar valor para o cliente?")

    # KPIs
    tp_total = throughput_mensal(df)
    backlog_atual = len(df[~df["concluido"]])
    entregas_lista = [tp_total.get(m, 0) for m in meses]
    media_entregas = np.mean([e for e in entregas_lista if e > 0]) if any(entregas_lista) else 0
    burndown = backlog_atual / media_entregas if media_entregas > 0 else 0

    c1, c2, c3 = st.columns(3)
    with c1: kpi("Backlog", str(backlog_atual))
    with c2: kpi("Média de Entregas / mês", f"{media_entregas:.1f}")
    with c3: kpi("Burn-Down Time", f"{burndown:.1f} meses")

    st.caption(
        "**Backlog:** itens criados ainda não concluídos  |  "
        "**Burn-Down Time:** backlog ÷ média de entregas mensais"
    )

    st.divider()

    # Abertura x Throughput
    ab_total = abertura_mensal(df)
    c1, c2 = st.columns(2)
    with c1:
        st.plotly_chart(
            bar_chart(
                "Abertura x Throughput",
                labels,
                {"Itens Abertos": [ab_total.get(m, 0) for m in meses],
                 "Itens Entregues": [tp_total.get(m, 0) for m in meses]},
            ),
            use_container_width=True,
        )
    with c2:
        tp_def = throughput_mensal(df, "Defeito")
        tp_his = throughput_mensal(df, "História")
        st.plotly_chart(
            bar_chart(
                "Throughput — Defeito x História",
                labels,
                {"Defeito": [tp_def.get(m, 0) for m in meses],
                 "História": [tp_his.get(m, 0) for m in meses]},
                stacked=True,
            ),
            use_container_width=True,
        )

    # Backlog
    bl_def = backlog_por_mes(df, "Defeito")
    bl_his = backlog_por_mes(df, "História")
    st.plotly_chart(
        bar_chart(
            "Backlog Acumulado — Defeito x História",
            labels,
            {"Defeito": [bl_def.get(m, 0) for m in meses],
             "História": [bl_his.get(m, 0) for m in meses]},
            stacked=True,
        ),
        use_container_width=True,
    )

    st.divider()
    st.subheader("Vazão Qualificada")
    st.caption(
        f"Pesos: Defeito = **{PESO_DEFEITO}pt** | "
        f"História ≤1d = **{PESO_HISTORIA_ATE_1_DIA}pt** | "
        f"1–3d = **{PESO_HISTORIA_1_3_DIAS}pt** | "
        f"4–10d = **{PESO_HISTORIA_4_10_DIAS}pt** | "
        f"≥11d = **{PESO_HISTORIA_11_MAIS_DIAS}pt**"
    )

    vq_def = vazao_qualificada_mensal(df, "Defeito")
    vq_his = vazao_qualificada_mensal(df, "História")

    c1, c2 = st.columns(2)
    with c1:
        st.plotly_chart(
            bar_chart(
                "Vazão Qualificada por Tipo",
                labels,
                {"Defeito": [vq_def.get(m, 0) for m in meses],
                 "História": [vq_his.get(m, 0) for m in meses]},
                stacked=True,
            ),
            use_container_width=True,
        )
    with c2:
        vq_equipe_pivot = vazao_por_equipe_mensal(df)
        tbl = pivot_table(vq_equipe_pivot, meses)
        if tbl is not None:
            st.markdown("**Vazão Qualificada por Equipe**")
            st.dataframe(tbl, hide_index=True, use_container_width=True)
        else:
            st.info("Dados de equipe não disponíveis (campo Team Name vazio no CSV).")


# ═══════════════════════════════════════════════
# ABA QUALIDADE
# ═══════════════════════════════════════════════

with tab_qual:
    st.subheader("Esse valor está sendo entregue com qualidade?")

    # KPIs
    retrabalho = retrabalho_mensal(df)
    saude = saude_backlog_mensal(df)
    ret_atual = np.mean(list(retrabalho.values())) * 100 if retrabalho else 0
    saude_atual = saude.get(meses[-1], 1.0) * 100 if meses else 0

    c1, c2 = st.columns(2)
    with c1:
        color = "#c0392b" if ret_atual > 20 else "#27ae60"
        kpi("Percentual de Retrabalho (média)", f"{ret_atual:.1f}%",
            "⚠️ Acima do benchmark 20%" if ret_atual > 20 else "✅ Dentro do benchmark", color)
    with c2:
        kpi("Saúde do Backlog (último mês)", f"{saude_atual:.0f}%")

    st.divider()

    # Abertura x Defeitos Entregues
    ab_def = abertura_mensal(df, "Defeito")
    tp_def = throughput_mensal(df, "Defeito")

    c1, c2 = st.columns(2)
    with c1:
        st.plotly_chart(
            bar_chart(
                "Abertura x Defeitos Entregues",
                labels,
                {"Itens Abertos": [ab_def.get(m, 0) for m in meses],
                 "Itens Entregues": [tp_def.get(m, 0) for m in meses]},
            ),
            use_container_width=True,
        )
    with c2:
        # Backlog defeitos por origem
        bl_cli, bl_int = {}, {}
        for ym in meses:
            dt_f = datetime.strptime(ym, "%Y-%m")
            dt_f = dt_f.replace(month=dt_f.month % 12 + 1) if dt_f.month < 12 \
                   else dt_f.replace(year=dt_f.year + 1, month=1)
            sub = df[
                (df["tipo_class"] == "Defeito") &
                (df["criado"].notna()) & (df["criado"] < dt_f) &
                (df["resolvido"].isna() | (df["resolvido"] >= dt_f))
            ]
            bl_cli[ym] = len(sub[sub["origem"] == "Cliente"])
            bl_int[ym] = len(sub[sub["origem"] == "Interno"])

        st.plotly_chart(
            bar_chart(
                "Backlog Defeitos por Origem",
                labels,
                {"Cliente": [bl_cli.get(m, 0) for m in meses],
                 "Interno": [bl_int.get(m, 0) for m in meses]},
                stacked=True,
            ),
            use_container_width=True,
        )

    # Saúde Backlog
    fig_saude = go.Figure(go.Bar(
        x=labels,
        y=[saude.get(m, 0) * 100 for m in meses],
        marker_color=COLORS["saude"],
        text=[f"{saude.get(m,0)*100:.0f}%" for m in meses],
        textposition="inside",
    ))
    fig_saude.update_layout(title="Saúde do Backlog (%)", template="plotly_white",
                             height=HEIGHT, yaxis_range=[0, 110],
                             margin=dict(t=60, b=40))
    st.plotly_chart(fig_saude, use_container_width=True)

    st.divider()
    st.subheader("Taxa de Retrabalho")
    st.caption(
        "Proporção do Touch Time gasto em defeitos vs histórias. "
        f"Benchmark de alta performance (Accelerate): abaixo de **{BENCH_RETRABALHO*100:.0f}%**."
    )

    st.plotly_chart(
        line_chart(
            f"Taxa de Retrabalho (Bench {BENCH_RETRABALHO*100:.0f}%)",
            labels,
            {"Retrabalho %": [retrabalho.get(m, 0) * 100 for m in meses]},
            bench_val=BENCH_RETRABALHO * 100,
            bench_label=f"Bench {BENCH_RETRABALHO*100:.0f}%",
        ),
        use_container_width=True,
    )

    # Taxa de retrabalho por equipe (tabela)
    equipes_df = df[df["equipe"].notna() & (df["equipe"] != "")]
    if not equipes_df.empty:
        ret_rows = []
        for eq in sorted(equipes_df["equipe"].unique()):
            row = {"Equipe": eq}
            for m in meses:
                sub_eq = df[df["equipe"] == eq]
                r = retrabalho_mensal(sub_eq)
                row[label_mes(m)] = f"{r.get(m, 0)*100:.1f}%"
            ret_rows.append(row)
        if ret_rows:
            st.markdown("**Taxa de Retrabalho por Equipe**")
            st.dataframe(pd.DataFrame(ret_rows), hide_index=True, use_container_width=True)


# ═══════════════════════════════════════════════
# ABA VELOCIDADE
# ═══════════════════════════════════════════════

with tab_vel:
    st.subheader("Qual a cadência de entrega do time?")

    lt85  = percentil85_mensal(df, "lead_time")
    ct85  = percentil85_mensal(df, "cycle_time")
    fe    = flow_efficiency_mensal(df)

    lt85_geral = percentil85(list(lt85.values()))
    ct85_geral = percentil85(list(ct85.values()))
    fe_media   = np.mean(list(fe.values())) * 100 if fe else 0

    c1, c2, c3 = st.columns(3)
    with c1: kpi("Lead Time P85 (dias)", f"{lt85_geral:.1f}")
    with c2: kpi("Cycle Time P85 (dias)", f"{ct85_geral:.1f}")
    with c3:
        color = "#27ae60" if fe_media >= BENCH_FLUXO * 100 else "#e67e22"
        kpi("Eficiência de Fluxo", f"{fe_media:.1f}%", color=color)

    st.caption(
        "**Lead Time:** criação → entrega (dias corridos)  |  "
        "**Cycle Time:** início do desenvolvimento → entrega  |  "
        "**P85:** 85% dos itens entregues dentro desse prazo"
    )
    if not ct85:
        st.info(
            "ℹ️ Cycle Time não calculado: configure `ACTIVE_STATUS_IDS` e `DONE_STATUS_IDS` "
            "no `dashboard.py` com os IDs do seu fluxo Jira, ou use o campo `Actual start`."
        )

    st.divider()

    # Lead Time vs Cycle Time P85
    c1, c2 = st.columns(2)
    with c1:
        st.plotly_chart(
            bar_chart(
                "Lead Time vs Cycle Time (P85%)",
                labels,
                {"Lead Time P85": [lt85.get(m, 0) for m in meses],
                 "Cycle Time P85": [ct85.get(m, 0) for m in meses]},
            ),
            use_container_width=True,
        )
    with c2:
        lt85d = percentil85_mensal(df, "lead_time", "Defeito")
        lt85h = percentil85_mensal(df, "lead_time", "História")
        st.plotly_chart(
            bar_chart(
                "Lead Time P85% por Tipo",
                labels,
                {"Defeito": [lt85d.get(m, 0) for m in meses],
                 "História": [lt85h.get(m, 0) for m in meses]},
            ),
            use_container_width=True,
        )

    # Cycle Time por tipo
    c1, c2 = st.columns(2)
    with c1:
        ct85d = percentil85_mensal(df, "cycle_time", "Defeito")
        ct85h = percentil85_mensal(df, "cycle_time", "História")
        st.plotly_chart(
            bar_chart(
                "Cycle Time P85% por Tipo",
                labels,
                {"Defeito": [ct85d.get(m, 0) for m in meses],
                 "História": [ct85h.get(m, 0) for m in meses]},
            ),
            use_container_width=True,
        )
    with c2:
        # Lead Time por equipe
        if not df[df["equipe"].notna() & (df["equipe"] != "")].empty:
            lt_eq_pivot = (
                df[df["concluido"] & df["lead_time"].notna() & df["equipe"].notna()]
                .groupby(["equipe", "mes_resolvido"])["lead_time"]
                .apply(percentil85)
                .unstack(fill_value=0)
            )
            tbl = pivot_table(lt_eq_pivot, meses)
            if tbl is not None:
                st.markdown("**Lead Time P85 por Equipe (dias)**")
                st.dataframe(tbl, hide_index=True, use_container_width=True)

    st.divider()
    st.subheader("Desvio Padrão")
    st.caption(
        "**Desvio Baixo (positivo):** entregas consistentes, fluxo previsível.  "
        "**Desvio Alto (negativo):** grande variabilidade nos tempos de entrega."
    )

    dp_ltd = desvio_padrao_mensal(df, "lead_time", "Defeito")
    dp_lth = desvio_padrao_mensal(df, "lead_time", "História")
    dp_ctd = desvio_padrao_mensal(df, "cycle_time", "Defeito")
    dp_cth = desvio_padrao_mensal(df, "cycle_time", "História")

    c1, c2 = st.columns(2)
    with c1:
        st.plotly_chart(
            bar_chart(
                "Desvio Padrão — Lead Time",
                labels,
                {"Defeito": [dp_ltd.get(m, 0) for m in meses],
                 "História": [dp_lth.get(m, 0) for m in meses]},
            ),
            use_container_width=True,
        )
    with c2:
        st.plotly_chart(
            bar_chart(
                "Desvio Padrão — Cycle Time",
                labels,
                {"Defeito": [dp_ctd.get(m, 0) for m in meses],
                 "História": [dp_cth.get(m, 0) for m in meses]},
            ),
            use_container_width=True,
        )

    st.divider()
    st.subheader("Eficiência de Fluxo")
    st.caption(
        f"Compara o tempo ativo de trabalho (Touch Time) com o tempo total (Lead Time). "
        f"Eficiência acima de **{BENCH_FLUXO*100:.0f}%** é considerada ideal."
    )

    st.plotly_chart(
        line_chart(
            f"Eficiência de Fluxo — Ideal {BENCH_FLUXO*100:.0f}%",
            labels,
            {"Eficiência %": [fe.get(m, 0) * 100 for m in meses]},
            bench_val=BENCH_FLUXO * 100,
            bench_label=f"Ideal {BENCH_FLUXO*100:.0f}%",
        ),
        use_container_width=True,
    )

    # Eficiência de fluxo por equipe (tabela)
    if not df[df["equipe"].notna() & (df["equipe"] != "")].empty:
        fe_rows = []
        for eq in sorted(df["equipe"].dropna().unique()):
            if not eq.strip():
                continue
            sub_eq = df[df["equipe"] == eq]
            fe_eq = flow_efficiency_mensal(sub_eq)
            row = {"Equipe": eq}
            for m in meses:
                row[label_mes(m)] = f"{fe_eq.get(m, 0)*100:.1f}%"
            fe_rows.append(row)
        if fe_rows:
            st.markdown("**Eficiência de Fluxo por Equipe**")
            st.dataframe(pd.DataFrame(fe_rows), hide_index=True, use_container_width=True)

    st.divider()
    st.subheader("Tempo por Status")
    st.caption("Total de horas (úteis aprox.) que os itens passaram em cada status do fluxo.")

    tps = tempo_por_status_total(df)
    status_names = infer_status_names(df)
    if tps:
        sorted_tps = sorted(tps.items(), key=lambda x: x[1], reverse=True)
        sid_labels_chart = [status_names.get(k, f"ID {k}") for k, _ in sorted_tps]
        sid_vals   = [v for _, v in sorted_tps]
        bar_colors = []
        for k, _ in sorted_tps:
            if k in DONE_STATUS_IDS:
                bar_colors.append("#4CAF50")
            elif k in ACTIVE_STATUS_IDS:
                bar_colors.append(COLORS["historia"])
            else:
                bar_colors.append(COLORS["abertura"])

        fig_tps = go.Figure(go.Bar(
            x=sid_labels_chart, y=sid_vals,
            marker_color=bar_colors,
            text=[f"{v:.0f}h" for v in sid_vals],
            textposition="outside",
        ))
        fig_tps.update_layout(
            title="Tempo por Status (horas) — 🟢 done | 🔵 ativo | 🟠 espera",
            template="plotly_white", height=HEIGHT,
            margin=dict(t=60, b=60),
        )
        st.plotly_chart(fig_tps, use_container_width=True)

    st.divider()
    st.subheader("Relação de Itens Entregues")
    entregues = df[df["concluido"]].sort_values("resolvido", ascending=False)
    if not entregues.empty:
        show_cols = {
            "key": "Código",
            "resumo": "Título",
            "equipe": "Equipe",
            "tipo_class": "Tipo",
            "resolvido": "Dt. Resolução",
            "cycle_time": "Cycle Time (d)",
            "lead_time": "Lead Time (d)",
            "vazao_qual": "Vazão Qual.",
        }
        df_show = entregues[[c for c in show_cols if c in entregues.columns]].copy()
        df_show = df_show.rename(columns=show_cols)
        if "Dt. Resolução" in df_show.columns:
            df_show["Dt. Resolução"] = df_show["Dt. Resolução"].dt.strftime("%d/%m/%Y")
        for col in ["Cycle Time (d)", "Lead Time (d)", "Vazão Qual."]:
            if col in df_show.columns:
                df_show[col] = df_show[col].apply(
                    lambda x: f"{x:.2f}" if pd.notna(x) else "–"
                )
        st.dataframe(df_show, use_container_width=True, hide_index=True)

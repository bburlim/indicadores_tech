"""
Indicadores de Eficiência de Tecnologia — Streamlit App
Lê CSV exportado do Jira e exibe o dashboard interativo com filtros.
"""

import streamlit as st
import streamlit.components.v1 as components
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
    jira_secrets_configured, get_jira_secrets, load_from_jira, test_connection, debug_jql,
    discover_fields, fetch_parent_issues,
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
                discover_fields.clear()
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
                    st.write("**Tipos de issue (tipo → tipo_class):**",
                             df_full.groupby("tipo")["tipo_class"].first().to_dict())
                    _ri = _req.get(
                        f"{secrets['jira_url']}/rest/api/3/project/ERM/issuetypes",
                        auth=_BA(secrets["email"], secrets["api_token"]),
                        headers={"Accept": "application/json"}, timeout=15,
                    )
                    if _ri.ok:
                        st.write("**Tipos disponíveis no projeto:**",
                                 [t["name"] for t in _ri.json()])
                    tis_ok = df_full["time_in_status"].notna() & (df_full["time_in_status"] != "")
                    st.write(f"**time_in_status preenchido:** {tis_ok.sum()} / {len(df_full)}")
                    st.write(f"**cycle_time não-nulo:** {df_full['cycle_time'].notna().sum()} / {len(df_full)}")
                    st.write(f"**equipe preenchida:** {(df_full['equipe'] != '').sum()} / {len(df_full)}")
                    if tis_ok.sum() > 0:
                        st.write("**Exemplo TIS:**", df_full.loc[tis_ok, "time_in_status"].iloc[0][:120])
                    st.write("**Status do projeto (ID → Nome):**")
                    _rs = _req.get(
                        f"{secrets['jira_url']}/rest/api/3/project/ERM/statuses",
                        auth=_BA(secrets["email"], secrets["api_token"]),
                        headers={"Accept": "application/json"}, timeout=15,
                    )
                    if _rs.ok:
                        _seen = {}
                        for _it in _rs.json():
                            for _st in _it.get("statuses", []):
                                _seen[_st["id"]] = _st["name"]
                        st.write({k: v for k, v in sorted(_seen.items(), key=lambda x: x[1])})
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

    # Responsável
    if "responsavel" in df_full.columns:
        pessoas = sorted(df_full["responsavel"].dropna().unique().tolist())
        pessoas = [p for p in pessoas if p.strip()]
        pessoa_sel = st.multiselect("Responsável", pessoas, default=pessoas) if pessoas else pessoas
    else:
        pessoa_sel = []

    # Tipo
    tipo_sel = st.multiselect(
        "Tipo de Item",
        ["Defeito", "História", "Subtarefa", "Outro"],
        default=["Defeito", "História", "Subtarefa"],
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

# Filtro de responsável — inclui sem responsável só quando todos estão selecionados
if pessoa_sel and "responsavel" in df.columns:
    todas_pessoas = sorted(df_full["responsavel"].dropna().unique().tolist())
    todas_pessoas = [p for p in todas_pessoas if p.strip()]
    if set(pessoa_sel) == set(todas_pessoas):
        # seleção completa: inclui issues sem responsável atribuído
        df = df[df["responsavel"].isin(pessoa_sel) | df["responsavel"].isna() | (df["responsavel"] == "")]
    else:
        # seleção parcial: mostra apenas os selecionados
        df = df[df["responsavel"].isin(pessoa_sel)]

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
  📊 Indicadores — Tecnologia &amp; Produto
</div>
""", unsafe_allow_html=True)

tab_tech, tab_produto = st.tabs(["💻 Tecnologia", "🎯 Produto"])

with tab_tech:
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
        elif "responsavel" in df.columns:
            sub_resp = df[df["concluido"] & df["responsavel"].notna() & (df["responsavel"] != "")]
            if not sub_resp.empty:
                pivot_resp = sub_resp.pivot_table(
                    index="responsavel", columns="mes_resolvido",
                    values="vazao_qual", aggfunc="sum", fill_value=0,
                )
                tbl_resp = pivot_table(pivot_resp, meses)
                if tbl_resp is not None:
                    tbl_resp = tbl_resp.rename(columns={"responsavel": "Responsável"})
                    st.markdown("**Vazão Qualificada por Responsável**")
                    st.dataframe(tbl_resp, hide_index=True, use_container_width=True)

    # ── Vazão Qualificada — Subtarefas ───────────────────────────────
    df_sub = df[df["tipo_class"] == "Subtarefa"]
    if not df_sub.empty:
        st.divider()
        st.subheader("Vazão Qualificada — Subtarefas")
        st.caption(
            f"Mesmos pesos de História: ≤1d = **{PESO_HISTORIA_ATE_1_DIA}pt** | "
            f"1–3d = **{PESO_HISTORIA_1_3_DIAS}pt** | "
            f"4–10d = **{PESO_HISTORIA_4_10_DIAS}pt** | "
            f"≥11d = **{PESO_HISTORIA_11_MAIS_DIAS}pt**"
        )

        vq_def_s  = vazao_qualificada_mensal(df_sub, "Defeito")
        vq_sub    = vazao_qualificada_mensal(df_sub, "Subtarefa")

        c1, c2 = st.columns(2)
        with c1:
            st.plotly_chart(
                bar_chart(
                    "Vazão Qualificada — Subtarefas",
                    labels,
                    {"Subtarefa": [vq_sub.get(m, 0) for m in meses]},
                    stacked=True,
                ),
                use_container_width=True,
            )
        with c2:
            vq_equipe_sub = vazao_por_equipe_mensal(df_sub)
            tbl_s = pivot_table(vq_equipe_sub, meses)
            if tbl_s is not None:
                st.markdown("**Vazão Qualificada Subtarefas por Equipe**")
                st.dataframe(tbl_s, hide_index=True, use_container_width=True)
            elif "responsavel" in df_sub.columns:
                sub_r = df_sub[df_sub["concluido"] & df_sub["responsavel"].notna() & (df_sub["responsavel"] != "")]
                if not sub_r.empty:
                    piv_r = sub_r.pivot_table(
                        index="responsavel", columns="mes_resolvido",
                        values="vazao_qual", aggfunc="sum", fill_value=0,
                    )
                    tbl_r = pivot_table(piv_r, meses)
                    if tbl_r is not None:
                        tbl_r = tbl_r.rename(columns={"responsavel": "Responsável"})
                        st.markdown("**Vazão Qualificada Subtarefas por Responsável**")
                        st.dataframe(tbl_r, hide_index=True, use_container_width=True)


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
            "responsavel": "Responsável",
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


# ═══════════════════════════════════════════════
# ABA PRODUTO
# ═══════════════════════════════════════════════

with tab_produto:
    st.subheader("% Completude dos Épicos")
    st.caption("Proporção de itens filhos concluídos em relação ao total de itens de cada épico.")

    if "parent_key" not in df_full.columns:
        st.info("Nenhum épico encontrado nos dados retornados do Jira.")
    else:
        # Agrupa os filhos pelo épico pai (parent_key preenchido e parent_type = Epic/Épico)
        filhos = df_full[
            df_full["parent_key"].notna() & (df_full["parent_key"] != "") &
            df_full["parent_type"].str.lower().str.contains("epic|épico", na=False)
        ] if "parent_type" in df_full.columns else df_full[
            df_full["parent_key"].notna() & (df_full["parent_key"] != "")
        ]

        # Monta título do épico: usa parent_summary se disponível
        epic_titles = {}
        if "parent_summary" in df_full.columns:
            for key, grp in filhos.groupby("parent_key"):
                summary = grp["parent_summary"].iloc[0]
                epic_titles[key] = summary if summary else key

        rows_comp = []
        for epic_key, grp in filhos.groupby("parent_key"):
            total = len(grp)
            done  = int(grp["concluido"].sum())
            pct   = round(done / total * 100, 1)
            titulo = epic_titles.get(epic_key, epic_key)
            if len(titulo) > 55:
                titulo = titulo[:52] + "..."
            rows_comp.append({
                "key":          epic_key,
                "Épico":        titulo,
                "Total":        total,
                "Concluídos":   done,
                "Pendentes":    total - done,
                "% Completude": pct,
            })

        if not rows_comp:
            st.info("Nenhum item filho vinculado a épicos encontrado.")
        else:
            df_comp = pd.DataFrame(rows_comp).sort_values("% Completude", ascending=True)

            # KPIs
            total_epicos = len(df_comp)
            epicos_completos = int((df_comp["% Completude"] == 100).sum())
            completude_media = round(df_comp["% Completude"].mean(), 1)

            c1, c2, c3 = st.columns(3)
            with c1: kpi("Total de Épicos", str(total_epicos))
            with c2: kpi("Épicos 100% concluídos", str(epicos_completos), color="#27ae60")
            with c3: kpi("Completude Média", f"{completude_media}%")

            st.divider()

            # Gráfico horizontal
            bar_colors = []
            for pct in df_comp["% Completude"]:
                if pct >= 75:
                    bar_colors.append("#27ae60")
                elif pct >= 40:
                    bar_colors.append("#e67e22")
                else:
                    bar_colors.append("#c0392b")

            fig_ep = go.Figure(go.Bar(
                x=df_comp["% Completude"],
                y=df_comp["Épico"],
                orientation="h",
                marker_color=bar_colors,
                text=[f"{p:.0f}%" for p in df_comp["% Completude"]],
                textposition="outside",
                customdata=df_comp[["Concluídos", "Total"]].values,
                hovertemplate="%{y}<br>%{customdata[0]} de %{customdata[1]} concluídos<extra></extra>",
            ))
            fig_ep.update_layout(
                title="% Completude por Épico",
                template=TEMPLATE,
                height=max(300, len(df_comp) * 40 + 100),
                xaxis=dict(range=[0, 115], ticksuffix="%"),
                margin=dict(t=60, b=40, l=20, r=60),
            )
            st.plotly_chart(fig_ep, use_container_width=True)

            st.divider()
            st.markdown("**Detalhe por Épico**")
            df_tbl = df_comp[["key", "Épico", "Total", "Concluídos", "Pendentes", "% Completude"]].copy()
            df_tbl = df_tbl.rename(columns={"key": "Código"}).sort_values("% Completude", ascending=False)
            st.dataframe(df_tbl, hide_index=True, use_container_width=True)

    # ── Visão por Objetivo ───────────────────────────────────────────
    st.divider()
    st.subheader("Visão por Objetivo")
    st.caption("Épicos agrupados pelo objetivo pai, com progresso calculado a partir dos itens filhos.")

    if "parent_key" not in df_full.columns or not jira_secrets_configured():
        st.info("Dados insuficientes para montar a visão por objetivo.")
    else:
        epic_keys = tuple(sorted(
            df_full.loc[df_full["parent_key"].notna() & (df_full["parent_key"] != ""), "parent_key"].unique()
        ))

        if not epic_keys:
            st.info("Nenhum épico vinculado encontrado nos dados.")
        else:
            _sec = get_jira_secrets()
            with st.spinner("Buscando objetivos..."):
                df_epics = fetch_parent_issues(
                    jira_url=_sec["jira_url"],
                    email=_sec["email"],
                    api_token=_sec["api_token"],
                    issue_keys=epic_keys,
                )

            if df_epics.empty or "parent_key" not in df_epics.columns:
                st.info("Objetivos não encontrados. Verifique se os épicos possuem um item pai no Jira.")
            else:
                # ── Dados por épico (done + in_progress + todo) ──────────────
                filhos_por_epico: dict = {}
                for epic_key, grp in df_full.groupby("parent_key"):
                    if not epic_key:
                        continue
                    done_n = int(grp["concluido"].sum())
                    prog_n = int(
                        grp["status_cat"]
                        .str.contains("Em andamento|In Progress", case=False, na=False)
                        .sum()
                    )
                    filhos_por_epico[epic_key] = {
                        "total":       len(grp),
                        "done":        done_n,
                        "in_progress": prog_n,
                    }

                # ── Agrupa épicos por objetivo ────────────────────────────────
                obj_map: dict = {}
                for _, er in df_epics.iterrows():
                    obj_key     = er.get("parent_key") or "Sem Objetivo"
                    obj_summary = er.get("parent_summary") or obj_key
                    obj_map.setdefault(obj_key, {"summary": obj_summary, "epics": []})

                    ep = filhos_por_epico.get(er["key"], {"total": 0, "done": 0, "in_progress": 0})
                    obj_map[obj_key]["epics"].append({
                        "key":         er["key"],
                        "summary":     er.get("summary", er["key"]),
                        "total":       ep["total"],
                        "done":        ep["done"],
                        "in_progress": ep["in_progress"],
                    })

                # ── KPIs ──────────────────────────────────────────────────────
                total_obj    = len(obj_map)
                epics_linked = sum(len(v["epics"]) for v in obj_map.values())
                c1, c2 = st.columns(2)
                with c1: kpi("Total de Objetivos", str(total_obj))
                with c2: kpi("Épicos Vinculados",  str(epics_linked))

                st.markdown("<br>", unsafe_allow_html=True)

                # ── Helpers ───────────────────────────────────────────────────
                PT_MON = ["jan","fev","mar","abr","mai","jun",
                          "jul","ago","set","out","nov","dez"]

                def _fmt_date(dt) -> str:
                    if dt is None or (isinstance(dt, float) and np.isnan(dt)):
                        return "—"
                    try:
                        return f"{dt.day:02d} de {PT_MON[dt.month-1]}. de {dt.year}"
                    except Exception:
                        return "—"

                def _prog_bar_html(done: int, in_prog: int, total: int) -> str:
                    if total == 0:
                        return '<div class="pb-wrap"></div>'
                    d = done    / total * 100
                    p = in_prog / total * 100
                    return (
                        f'<div class="pb-wrap">'
                        f'<div class="pb-done" style="width:{d:.1f}%"></div>'
                        f'<div class="pb-prog" style="width:{p:.1f}%"></div>'
                        f'</div>'
                    )

                def _status_dot(status_cat: str) -> str:
                    if "conclu" in status_cat.lower() or "done" in status_cat.lower():
                        return '<span style="color:#5aac44;font-size:11px;">● Concluído</span>'
                    if "andamento" in status_cat.lower() or "progress" in status_cat.lower():
                        return '<span style="color:#0052cc;font-size:11px;">● Em andamento</span>'
                    return '<span style="color:#97a0af;font-size:11px;">○ Pendente</span>'

                # Stories do df_full agrupadas por epic_key
                stories_por_epico: dict = {}
                story_mask = (
                    df_full["parent_key"].notna() & (df_full["parent_key"] != "") &
                    df_full["parent_type"].str.lower().str.contains("epic|épico", na=False)
                ) if "parent_type" in df_full.columns else (
                    df_full["parent_key"].notna() & (df_full["parent_key"] != "")
                )
                for epic_key, grp in df_full[story_mask].groupby("parent_key"):
                    grp2 = grp.copy()
                    # Fallback: usa criado quando actual_start está vazio
                    grp2["start_date_display"] = grp2["actual_start"].fillna(grp2["criado"])
                    stories_por_epico[epic_key] = grp2[
                        ["key", "resumo", "status_cat", "start_date_display", "due_date"]
                    ].to_dict("records")

                # ── Tabela de épicos com datas (epic_key → row de df_epics) ──
                epic_dates: dict = {}
                for _, er in df_epics.iterrows():
                    epic_dates[er["key"]] = {
                        "start_date": er.get("start_date"),
                        "due_date":   er.get("due_date"),
                    }

                jira_base_url = _sec["jira_url"]

                # ── CSS ───────────────────────────────────────────────────────
                CSS = """
                <style>
                .rt { width:100%; border-collapse:collapse;
                      font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
                      font-size:13px; }
                .rt th { text-align:left; padding:8px 14px; border-bottom:2px solid #dfe1e6;
                         color:#5e6c84; font-weight:600; font-size:11px; text-transform:uppercase;
                         letter-spacing:.04em; white-space:nowrap; }
                .rt td { padding:8px 14px; border-bottom:1px solid #f0f0f0; vertical-align:middle;
                         white-space:nowrap; }
                .rt tr:last-child td { border-bottom:none; }
                .row-obj  td { background:#f4f5f7; font-weight:600; }
                .row-epic td { background:#ffffff; }
                .row-epic td:nth-child(2) { padding-left:36px; }
                .row-story td { background:#fafbfc; }
                .row-story td:nth-child(2) { padding-left:64px; }
                .pb-wrap { display:flex; height:8px; border-radius:4px; overflow:hidden;
                           width:140px; background:#dfe1e6; }
                .pb-done { background:#5aac44; flex-shrink:0; }
                .pb-prog { background:#0052cc; flex-shrink:0; }
                .ik  { color:#0052cc; font-weight:600; font-size:12px; text-decoration:none; }
                .ik:hover { text-decoration:underline; }
                .bk-icon { display:inline-block; width:14px; height:14px;
                           background:#00B8A3; border-radius:2px; margin-right:6px;
                           vertical-align:middle; position:relative; }
                .bk-icon::after { content:''; position:absolute; bottom:2px; left:2px; right:2px;
                                  height:3px; background:white; border-radius:1px; }
                .lt-icon { display:inline-block; color:#6554C0; margin-right:6px;
                           font-size:13px; vertical-align:middle; }
                .st-icon { display:inline-block; color:#97a0af; margin-right:6px;
                           font-size:12px; vertical-align:middle; }
                .cnt  { color:#5e6c84; font-size:12px; }
                .dt   { color:#5e6c84; font-size:12px; }
                .pri  { color:#F79233; font-size:15px; font-weight:900; vertical-align:middle; }
                .num  { color:#97a0af; font-size:12px; }
                .arrow { font-size:10px; color:#5e6c84; margin-right:5px;
                         display:inline-block; width:10px; user-select:none; }
                </style>
                """

                # ── Linhas da tabela ──────────────────────────────────────────
                rows_html  = ""
                row_num    = 1
                total_rows = 0
                epic_counter = 0

                for obj_key, obj_data in sorted(obj_map.items()):
                    epics    = obj_data["epics"]
                    obj_tot  = sum(e["total"]      for e in epics)
                    obj_done = sum(e["done"]        for e in epics)
                    obj_prog = sum(e["in_progress"] for e in epics)
                    obj_todo = obj_tot - obj_done - obj_prog
                    grp_id   = f"grp{row_num}"

                    # Datas do objetivo: mín início / máx limite dos épicos
                    ep_starts = [epic_dates.get(e["key"], {}).get("start_date") for e in epics]
                    ep_dues   = [epic_dates.get(e["key"], {}).get("due_date")   for e in epics]
                    ep_starts = [d for d in ep_starts if d is not None]
                    ep_dues   = [d for d in ep_dues   if d is not None]
                    obj_start = min(ep_starts) if ep_starts else None
                    obj_due   = max(ep_dues)   if ep_dues   else None

                    obj_title = obj_data["summary"]
                    if len(obj_title) > 60:
                        obj_title = obj_title[:57] + "…"

                    rows_html += f"""
                    <tr class="row-obj" onclick="toggleObj('{grp_id}')" style="cursor:pointer">
                      <td class="num">{row_num}</td>
                      <td>
                        <span id="arrow-{grp_id}" class="arrow">▼</span>
                        <span class="bk-icon"></span>
                        <a class="ik" href="{jira_base_url}/browse/{obj_key}" target="_blank">{obj_key}</a>
                        &nbsp; {obj_title}
                      </td>
                      <td>{_prog_bar_html(obj_done, obj_prog, obj_tot)}</td>
                      <td class="cnt" style="color:#5aac44">✓ {obj_done}</td>
                      <td class="cnt" style="color:#0052cc">◑ {obj_prog}</td>
                      <td class="cnt">○ {obj_todo}</td>
                      <td><span class="pri">≡</span></td>
                      <td class="dt">{_fmt_date(obj_start)}</td>
                      <td class="dt">{_fmt_date(obj_due)}</td>
                    </tr>"""
                    row_num   += 1
                    total_rows += 1

                    for epic in sorted(epics, key=lambda x: x["done"]/x["total"] if x["total"] else 0, reverse=True):
                        epic_counter += 1
                        ep_id    = f"ep{epic_counter}"
                        ep_title = epic["summary"]
                        if len(ep_title) > 60:
                            ep_title = ep_title[:57] + "…"
                        ep_todo  = epic["total"] - epic["done"] - epic["in_progress"]
                        ep_dates = epic_dates.get(epic["key"], {})
                        ep_stories = stories_por_epico.get(epic["key"], [])
                        has_stories = len(ep_stories) > 0

                        ep_arrow = f'<span id="arrow-{ep_id}" class="arrow">{"▶" if has_stories else " "}</span>'
                        ep_click = f'onclick="toggleEpic(event,\'{ep_id}\')"' if has_stories else ""
                        ep_cursor = "cursor:pointer" if has_stories else ""

                        rows_html += f"""
                        <tr class="row-epic {grp_id}" {ep_click} style="{ep_cursor}">
                          <td></td>
                          <td>
                            {ep_arrow}
                            <span class="lt-icon">⚡</span>
                            <a class="ik" href="{jira_base_url}/browse/{epic["key"]}" target="_blank">{epic["key"]}</a>
                            &nbsp; {ep_title}
                          </td>
                          <td>{_prog_bar_html(epic["done"], epic["in_progress"], epic["total"])}</td>
                          <td class="cnt" style="color:#5aac44">✓ {epic["done"]}</td>
                          <td class="cnt" style="color:#0052cc">◑ {epic["in_progress"]}</td>
                          <td class="cnt">○ {ep_todo}</td>
                          <td><span class="pri">≡</span></td>
                          <td class="dt">{_fmt_date(ep_dates.get("start_date"))}</td>
                          <td class="dt">{_fmt_date(ep_dates.get("due_date"))}</td>
                        </tr>"""
                        total_rows += 1

                        for story in ep_stories:
                            st_title = str(story.get("resumo", ""))
                            if len(st_title) > 60:
                                st_title = st_title[:57] + "…"
                            st_cat = str(story.get("status_cat", ""))
                            rows_html += f"""
                            <tr class="row-story {grp_id} {ep_id}" style="display:none">
                              <td></td>
                              <td>
                                <span class="st-icon">▸</span>
                                <a class="ik" href="{jira_base_url}/browse/{story["key"]}" target="_blank">{story["key"]}</a>
                                &nbsp; {st_title}
                              </td>
                              <td>{_status_dot(st_cat)}</td>
                              <td colspan="3"></td>
                              <td><span class="pri">≡</span></td>
                              <td class="dt">{_fmt_date(story.get("start_date_display"))}</td>
                              <td class="dt">{_fmt_date(story.get("due_date"))}</td>
                            </tr>"""
                            total_rows += 1

                # ── JavaScript ────────────────────────────────────────────────
                JS = """
                <script>
                function toggleObj(grpId) {
                  const epics = document.querySelectorAll('tr.row-epic.' + grpId);
                  const arrow = document.getElementById('arrow-' + grpId);
                  const hide  = epics.length > 0 && epics[0].style.display !== 'none';
                  epics.forEach(function(r) {
                    r.style.display = hide ? 'none' : '';
                    if (hide) {
                      // colapsa histórias dos épicos desse objetivo
                      r.classList.forEach(function(cls) {
                        if (cls.startsWith('ep')) {
                          document.querySelectorAll('tr.row-story.' + cls)
                            .forEach(function(s) { s.style.display = 'none'; });
                          var ea = document.getElementById('arrow-' + cls);
                          if (ea) ea.textContent = '▶';
                        }
                      });
                    }
                  });
                  if (arrow) arrow.textContent = hide ? '▶' : '▼';
                }
                function toggleEpic(event, epId) {
                  event.stopPropagation();
                  const stories = document.querySelectorAll('tr.row-story.' + epId);
                  const arrow   = document.getElementById('arrow-' + epId);
                  const hide    = stories.length > 0 && stories[0].style.display !== 'none';
                  stories.forEach(function(r) { r.style.display = hide ? 'none' : ''; });
                  if (arrow) arrow.textContent = hide ? '▶' : '▼';
                }
                </script>
                """

                table_html = f"""
                {CSS}
                <style>
                  .rt-scroll {{ overflow-x: auto; -webkit-overflow-scrolling: touch; }}
                  .rt {{ min-width: 900px; }}
                </style>
                <div class="rt-scroll">
                <table class="rt">
                  <thead>
                    <tr>
                      <th>#</th>
                      <th>Ticket</th>
                      <th>Progresso</th>
                      <th>Concluídos</th>
                      <th>Em andamento</th>
                      <th>Pendentes</th>
                      <th>Prioridade</th>
                      <th>Data de Início</th>
                      <th>Data Limite</th>
                    </tr>
                  </thead>
                  <tbody>{rows_html}</tbody>
                </table>
                </div>
                {JS}
                """

                height_px = 55 + total_rows * 44
                components.html(table_html, height=height_px, scrolling=False)

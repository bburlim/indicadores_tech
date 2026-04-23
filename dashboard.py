"""
Dashboard de Indicadores de Eficiência de Tecnologia
Baseado em exportação do Jira (CSV)

Indicadores gerados:
  Produtividade: Backlog, Média de Entregas, Burn-Down Time,
                 Throughput, Vazão Qualificada
  Qualidade:     Retrabalho, Defeitos, Saúde do Backlog, SLA
  Velocidade:    Lead Time, Cycle Time, Eficiência de Fluxo,
                 Tempo por Status
"""

from __future__ import annotations

import csv
import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

# ─────────────────────────────────────────────
# CONFIGURAÇÃO
# ─────────────────────────────────────────────

CSV_PATH = os.path.join(os.path.dirname(__file__), "data", "jira.csv")

# Status IDs que representam etapas "ativas" (Cycle Time / Touch Time)
# Inferidos automaticamente a partir das sequências do [CHART] Time in Status.
# Use --dump-status-ids para listar todos os IDs e --map-status-ids para ver
# a inferência automática. Ajuste conforme necessário para o seu projeto.
#
# Mapeamento projeto ERM (Emiteai):
#   3       → Em andamento           (ATIVO)
#   10180   → Testando               (ATIVO)
#   10285   → Ajustando defeito      (ATIVO)
#   10179   → Ag. Testes             (espera)
#   10213   → Ag. Code Review        (espera)
#   10284   → Ag. Ajuste de defeito  (espera)
#   10318   → Impedimento Dev        (espera/bloqueado)
#   10319   → Impedimento Testes     (espera/bloqueado)
#   10039   → Tarefas pendentes      (backlog)
#   10006   → Concluído              (done)
#   10352   → Released               (done)
ACTIVE_STATUS_IDS: Set[str] = {"3", "10180", "10285"}
DONE_STATUS_IDS: Set[str] = {"10006", "10352"}

# Tipos de item que representam Defeito / História / Subtarefa
DEFEITO_TYPES   = {"Bug"}
HISTORIA_TYPES  = {"História", "Historia", "Story"}
SUBTAREFA_TYPES = {"Subtarefa", "Sub-task", "Subtask"}

# Origem de defeitos: palavras-chave no resumo/labels para "Cliente"
ORIGEM_CLIENTE_KEYWORDS = ["cliente", "customer", "externo"]

# Mapeamento de equipe → headcount mensal (contábil e líquido)
# Formato: {"Equipe": {"2025-01": {"contabil": 5, "liquido": 5.2}, ...}}
HEADCOUNT: Dict = {}

# Pesos para Vazão Qualificada
PESO_DEFEITO = 0.5
PESO_HISTORIA_ATE_1_DIA = 0.0
PESO_HISTORIA_1_3_DIAS = 0.5
PESO_HISTORIA_4_10_DIAS = 1.0
PESO_HISTORIA_11_MAIS_DIAS = 2.0

# Benchmark Retrabalho (linha tracejada no gráfico)
BENCH_RETRABALHO = 0.20

# Benchmark Eficiência de Fluxo
BENCH_FLUXO = 0.40

# ─────────────────────────────────────────────
# PARSE DO CSV
# ─────────────────────────────────────────────

PT_MONTHS = {
    "jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6,
    "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12,
}

def parse_jira_date(s: str) -> Optional[datetime]:
    """Parse datas no formato do Jira em PT-BR: '17/mar/26 12:13 PM'"""
    if not s or not s.strip():
        return None
    s = s.strip()
    # Tenta DD/MMM/YY HH:MM AM/PM
    m = re.match(r"(\d{1,2})/(\w{3})/(\d{2,4})\s+(\d{1,2}):(\d{2})\s*(AM|PM)?", s, re.IGNORECASE)
    if m:
        day = int(m.group(1))
        mon = PT_MONTHS.get(m.group(2).lower(), 0)
        year = int(m.group(3))
        if year < 100:
            year += 2000
        hour = int(m.group(4))
        minute = int(m.group(5))
        ampm = (m.group(6) or "").upper()
        if ampm == "PM" and hour != 12:
            hour += 12
        if ampm == "AM" and hour == 12:
            hour = 0
        try:
            return datetime(year, mon, day, hour, minute)
        except ValueError:
            pass
    # Fallback pandas
    try:
        return pd.to_datetime(s, dayfirst=True).to_pydatetime()
    except Exception:
        return None


def parse_time_in_status(tis: str) -> Dict[str, int]:
    """
    Parseia o campo '[CHART] Time in Status'.
    Formato: 'ID_*:*_count_*:*_ms_*|*_ID_*:*_count_*:*_ms...'
    Retorna dict {status_id: total_ms}
    """
    result: dict[str, int] = {}
    if not tis or tis in ("{}", ""):
        return result
    for entry in tis.split("_*|*_"):
        parts = entry.split("_*:*_")
        if len(parts) >= 3:
            sid = parts[0].strip()
            try:
                ms = int(parts[2].strip())
            except ValueError:
                ms = 0
            result[sid] = result.get(sid, 0) + ms
    return result


def load_csv(path: str) -> pd.DataFrame:
    with open(path, encoding="utf-8") as f:
        reader = csv.reader(f)
        headers = next(reader)
        rows = list(reader)

    df = pd.DataFrame(rows, columns=headers)

    # Renomear colunas para nomes curtos
    rename = {
        "Chave da item": "key",
        "Tipo de item": "tipo",
        "Status": "status",
        "Prioridade": "prioridade",
        "Criado": "criado_str",
        "Resolvido": "resolvido_str",
        "Atualizado(a)":  "atualizado_str",
        "Categoria do status": "status_cat",
        "Categoria do status alterada": "status_cat_changed",
        "Team Name": "equipe",
        "Campo personalizado ([CHART] Time in Status)": "time_in_status",
        "Campo personalizado (Actual start)": "actual_start_str",
        "Campo personalizado (Actual end)": "actual_end_str",
        "Campo personalizado (Categoria de trabalho)": "categoria_trabalho",
        "Campo personalizado (Categoria)": "categoria",
        "Resumo": "resumo",
        "Categorias": "labels",
        "Sprint": "sprint",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    # Parse datas
    for col_str, col_dt in [("criado_str", "criado"), ("resolvido_str", "resolvido"),
                              ("actual_start_str", "actual_start"), ("actual_end_str", "actual_end"),
                              ("atualizado_str", "atualizado")]:
        if col_str in df.columns:
            df[col_dt] = df[col_str].apply(parse_jira_date)

    # Classificação Defeito / História / Outro
    def classifica_tipo(row):
        t = str(row.get("tipo", "")).strip()
        ct = str(row.get("categoria_trabalho", "")).strip()
        if ct:
            if "bug" in ct.lower() or "defeito" in ct.lower():
                return "Defeito"
            if "hist" in ct.lower() or "story" in ct.lower():
                return "História"
        if t in DEFEITO_TYPES:
            return "Defeito"
        if t in HISTORIA_TYPES:
            return "História"
        if t in SUBTAREFA_TYPES:
            return "Subtarefa"
        return "Outro"

    df["tipo_class"] = df.apply(classifica_tipo, axis=1)

    # Concluído?
    df["concluido"] = df["status_cat"].str.strip().isin(["Itens concluídos", "Done", "Concluído"])

    # Lead Time (dias corridos)
    def lead_time(row):
        if pd.notna(row.get("criado")) and pd.notna(row.get("resolvido")):
            return (row["resolvido"] - row["criado"]).total_seconds() / 86400
        return np.nan

    df["lead_time"] = df.apply(lead_time, axis=1)

    # Time in Status — deve ser criado ANTES do Cycle Time
    df["time_in_status_parsed"] = (
        df["time_in_status"].apply(parse_time_in_status)
        if "time_in_status" in df.columns
        else [{}] * len(df)
    )

    # Cycle Time: tempo desde o início do desenvolvimento até a entrega.
    #
    # IMPORTANTE: o campo [CHART] Time in Status acumula horas APÓS a resolução
    # (o item continua no status "done" até a próxima exportação). Por isso, o
    # tempo em status DONE não reflete o ciclo de trabalho.
    #
    # Fórmula:
    #   workflow_ms = total_ms - done_ms   (tempo no fluxo, excluindo pós-resolução)
    #   CT = LT × (active_ms / workflow_ms)
    #      → proporção do fluxo gasto em trabalho ativo, escalada ao LT calendário
    #
    # Prioridade:
    #   1. actual_start → resolvido  (mais preciso, quando disponível)
    #   2. LT × (active_ms / workflow_ms)  quando active_ms > 0 e workflow_ms > 0
    #   3. NaN — sem dados suficientes (nunca usa LT como fallback)

    def cycle_time(row):
        # 1. actual_start disponível
        if pd.notna(row.get("actual_start")) and pd.notna(row.get("resolvido")):
            return (row["resolvido"] - row["actual_start"]).total_seconds() / 86400

        tis = row.get("time_in_status_parsed", {})
        total_ms = sum(tis.values())
        lt = row.get("lead_time", np.nan)
        if not tis or total_ms == 0 or pd.isna(lt) or lt == 0:
            return np.nan

        # 2. Proporção ativa escalada ao Lead Time
        if ACTIVE_STATUS_IDS and DONE_STATUS_IDS:
            active_ms  = sum(ms for sid, ms in tis.items() if sid in ACTIVE_STATUS_IDS)
            done_ms    = sum(ms for sid, ms in tis.items() if sid in DONE_STATUS_IDS)
            workflow_ms = total_ms - done_ms  # exclui tempo pós-resolução em done

            if active_ms > 0 and workflow_ms > 0:
                frac = active_ms / workflow_ms   # fração ativa dentro do fluxo
                return lt * frac

        return np.nan  # sem dados confiáveis → NaN

    df["cycle_time"] = df.apply(cycle_time, axis=1)

    def touch_time_ms(row):
        tis = row.get("time_in_status_parsed", {})
        if not tis:
            return np.nan
        if ACTIVE_STATUS_IDS:
            return sum(ms for sid, ms in tis.items() if sid in ACTIVE_STATUS_IDS)
        # Sem mapeamento: soma tudo exceto status DONE
        if DONE_STATUS_IDS:
            return sum(ms for sid, ms in tis.items() if sid not in DONE_STATUS_IDS)
        # Fallback: soma total
        return sum(tis.values())

    df["touch_time_ms"] = df.apply(touch_time_ms, axis=1)
    df["touch_time_dias"] = df["touch_time_ms"] / 86400000

    # Eficiência de Fluxo = touch_time / lead_time
    def fluxo_eff(row):
        lt = row.get("lead_time", np.nan)
        tt = row.get("touch_time_dias", np.nan)
        if pd.notna(lt) and pd.notna(tt) and lt > 0:
            return min(tt / lt, 1.0)
        return np.nan

    df["flow_efficiency"] = df.apply(fluxo_eff, axis=1)

    # Vazão Qualificada
    def vazao_qualificada(row):
        if row["tipo_class"] == "Defeito":
            return PESO_DEFEITO
        if row["tipo_class"] in ("História", "Subtarefa"):
            ct = row.get("cycle_time", np.nan)
            if pd.isna(ct):
                return PESO_HISTORIA_4_10_DIAS  # default
            if ct <= 1:
                return PESO_HISTORIA_ATE_1_DIA
            if ct <= 3:
                return PESO_HISTORIA_1_3_DIAS
            if ct <= 10:
                return PESO_HISTORIA_4_10_DIAS
            return PESO_HISTORIA_11_MAIS_DIAS
        return 0.0

    df["vazao_qual"] = df.apply(vazao_qualificada, axis=1)

    # Origem defeito
    def origem_defeito(row):
        texto = " ".join([
            str(row.get("resumo", "")),
            str(row.get("labels", "")),
            str(row.get("categoria", "")),
        ]).lower()
        for kw in ORIGEM_CLIENTE_KEYWORDS:
            if kw in texto:
                return "Cliente"
        return "Interno"

    df["origem"] = df.apply(origem_defeito, axis=1)

    # Mês de criação e de resolução
    # resolutiondate pode ser null mesmo para itens Done quando o workflow do Jira
    # não seta a data automaticamente. Fallback: usa "atualizado" como proxy.
    df["mes_criado"] = df["criado"].apply(lambda d: d.strftime("%Y-%m") if pd.notna(d) else None)

    # Fallback para itens Concluídos sem resolutiondate:
    # usa status_cat_changed (data em que a categoria mudou para Done no CSV).
    # É mais preciso que "atualizado", pois não é afetado por comentários posteriores.
    if "status_cat_changed" in df.columns:
        df["status_cat_changed_dt"] = df["status_cat_changed"].apply(parse_jira_date)

    def _mes_resolvido(row):
        if pd.notna(row.get("resolvido")):
            return row["resolvido"].strftime("%Y-%m")
        if row.get("concluido"):
            fallback = row.get("status_cat_changed_dt")
            if pd.notna(fallback):
                return fallback.strftime("%Y-%m")
            # último recurso: atualizado
            atualizado = row.get("atualizado")
            if pd.notna(atualizado):
                return atualizado.strftime("%Y-%m")
        return None

    df["mes_resolvido"] = df.apply(_mes_resolvido, axis=1)

    return df


# ─────────────────────────────────────────────
# AGREGAÇÕES
# ─────────────────────────────────────────────

def get_months(df: pd.DataFrame) -> List[str]:
    meses = sorted(set(
        list(df["mes_criado"].dropna()) +
        list(df["mes_resolvido"].dropna())
    ))
    return meses


def label_mes(ym: str) -> str:
    """'2025-01' → 'Jan'"""
    y, m = ym.split("-")
    nomes = ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"]
    return nomes[int(m) - 1]


def label_mes_ano(ym: str) -> str:
    y, m = ym.split("-")
    nomes = ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"]
    return f"{nomes[int(m)-1]}\n{y}"


def throughput_mensal(df: pd.DataFrame, tipo: Optional[str] = None) -> Dict[str, int]:
    sub = df[df["concluido"]]
    if tipo:
        sub = sub[sub["tipo_class"] == tipo]
    return sub.groupby("mes_resolvido").size().to_dict()


def abertura_mensal(df: pd.DataFrame, tipo: Optional[str] = None) -> Dict[str, int]:
    sub = df.copy()
    if tipo:
        sub = sub[sub["tipo_class"] == tipo]
    return sub.groupby("mes_criado").size().to_dict()


def backlog_por_mes(df: pd.DataFrame, tipo: Optional[str] = None) -> Dict[str, int]:
    """
    Backlog acumulado ao final de cada mês:
    criados até o mês M e não resolvidos até o mês M.
    """
    meses = get_months(df)
    result = {}
    for ym in meses:
        dt_fim = datetime.strptime(ym, "%Y-%m")
        # último dia do mês
        if dt_fim.month == 12:
            dt_fim = dt_fim.replace(year=dt_fim.year + 1, month=1, day=1)
        else:
            dt_fim = dt_fim.replace(month=dt_fim.month + 1, day=1)
        sub = df[
            (df["criado"].notna()) &
            (df["criado"] < dt_fim) &
            (df["resolvido"].isna() | (df["resolvido"] >= dt_fim))
        ]
        if tipo:
            sub = sub[sub["tipo_class"] == tipo]
        result[ym] = len(sub)
    return result


def percentil85(values: list[float]) -> float:
    vals = [v for v in values if not np.isnan(v)]
    if not vals:
        return 0.0
    return float(np.percentile(vals, 85))


def percentil85_mensal(df: pd.DataFrame, col: str, tipo: Optional[str] = None) -> Dict[str, float]:
    sub = df[df["concluido"] & df[col].notna()]
    if tipo:
        sub = sub[sub["tipo_class"] == tipo]
    return sub.groupby("mes_resolvido")[col].apply(percentil85).to_dict()


def desvio_padrao_mensal(df: pd.DataFrame, col: str, tipo: Optional[str] = None) -> Dict[str, float]:
    sub = df[df["concluido"] & df[col].notna()]
    if tipo:
        sub = sub[sub["tipo_class"] == tipo]
    return sub.groupby("mes_resolvido")[col].std().fillna(0).to_dict()


def vazao_qualificada_mensal(df: pd.DataFrame, tipo: Optional[str] = None) -> Dict[str, float]:
    sub = df[df["concluido"]]
    if tipo:
        sub = sub[sub["tipo_class"] == tipo]
    return sub.groupby("mes_resolvido")["vazao_qual"].sum().to_dict()


def flow_efficiency_mensal(df: pd.DataFrame) -> dict[str, float]:
    sub = df[df["concluido"] & df["flow_efficiency"].notna()]
    return sub.groupby("mes_resolvido")["flow_efficiency"].mean().to_dict()


def retrabalho_mensal(df: pd.DataFrame) -> dict[str, float]:
    """
    Taxa de Retrabalho = Touch Time Defeitos / (Touch Time Defeitos + Touch Time Histórias)
    por mês de resolução.
    """
    sub = df[df["concluido"] & df["touch_time_ms"].notna() & (df["touch_time_ms"] > 0)]
    result = {}
    for ym, grp in sub.groupby("mes_resolvido"):
        tt_def = grp[grp["tipo_class"] == "Defeito"]["touch_time_ms"].sum()
        tt_his = grp[grp["tipo_class"] == "História"]["touch_time_ms"].sum()
        total = tt_def + tt_his
        result[ym] = tt_def / total if total > 0 else 0.0
    return result


def saude_backlog_mensal(df: pd.DataFrame) -> dict[str, float]:
    """
    Saúde Backlog = % Histórias no backlog (maior = mais saudável).
    """
    meses = get_months(df)
    result = {}
    for ym in meses:
        dt_fim = datetime.strptime(ym, "%Y-%m")
        if dt_fim.month == 12:
            dt_fim = dt_fim.replace(year=dt_fim.year + 1, month=1, day=1)
        else:
            dt_fim = dt_fim.replace(month=dt_fim.month + 1, day=1)
        sub = df[
            (df["criado"].notna()) &
            (df["criado"] < dt_fim) &
            (df["resolvido"].isna() | (df["resolvido"] >= dt_fim)) &
            (df["tipo_class"].isin(["Defeito", "História"]))
        ]
        total = len(sub)
        historias = len(sub[sub["tipo_class"] == "História"])
        result[ym] = historias / total if total > 0 else 1.0
    return result


def tempo_por_status_total(df: pd.DataFrame) -> Dict[str, float]:
    """Soma total de horas úteis (aprox.) por status ID."""
    totals: Dict[str, float] = defaultdict(float)
    for tis in df["time_in_status_parsed"]:
        for sid, ms in tis.items():
            totals[sid] += ms / 3600000  # ms → horas
    return dict(totals)


def tempo_por_status_mensal(df: pd.DataFrame) -> pd.DataFrame:
    """Percentual do tempo passado em cada status por mês de resolução.
    Retorna DataFrame com índice = mes (YYYY-MM) e colunas = status IDs."""
    sub = df[df["concluido"]]
    rows = []
    for ym, grp in sub.groupby("mes_resolvido"):
        totals: Dict[str, float] = defaultdict(float)
        for tis in grp["time_in_status_parsed"]:
            for sid, ms in tis.items():
                totals[sid] += ms
        grand_total = sum(totals.values())
        if grand_total > 0:
            row = {"mes": ym}
            for sid, ms in totals.items():
                row[sid] = ms / grand_total * 100
            rows.append(row)
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).set_index("mes").fillna(0)


STATUS_NAMES: Dict[str, str] = {
    "3":     "Em andamento",
    "10006": "Concluído",
    "10039": "Tarefas pendentes",
    "10179": "Ag. Testes",
    "10180": "Testando",
    "10213": "Ag. Code Review",
    "10284": "Ag. Ajuste de defeito",
    "10285": "Ajustando defeito",
    "10318": "Impedimento Dev",
    "10319": "Impedimento Testes",
    "10352": "Released",
}


def infer_status_names(df: pd.DataFrame) -> Dict[str, str]:
    """Retorna o nome real de cada status ID encontrado no TIS."""
    all_ids: Set[str] = set()
    for tis in df["time_in_status_parsed"]:
        all_ids |= set(tis.keys())

    return {sid: STATUS_NAMES.get(sid, f"Status {sid}") for sid in all_ids}


def vazao_por_equipe_mensal(df: pd.DataFrame) -> pd.DataFrame:
    sub = df[df["concluido"] & df["equipe"].notna() & (df["equipe"] != "")]
    pivot = sub.pivot_table(index="equipe", columns="mes_resolvido",
                             values="vazao_qual", aggfunc="sum", fill_value=0)
    return pivot


def vazao_por_responsavel_mensal(df: pd.DataFrame) -> pd.DataFrame:
    """Vazão qualificada por responsável — inclui todos os itens concluídos,
    independente de equipe preenchida."""
    sub = df[df["concluido"] & df["responsavel"].notna() & (df["responsavel"] != "")]
    pivot = sub.pivot_table(index="responsavel", columns="mes_resolvido",
                             values="vazao_qual", aggfunc="sum", fill_value=0)
    return pivot


# ─────────────────────────────────────────────
# GERAÇÃO DO DASHBOARD HTML
# ─────────────────────────────────────────────

COLORS = {
    "abertura": "#FFA500",
    "entregue": "#7B7FBF",
    "defeito": "#9B3D5C",
    "historia": "#5BC8D9",
    "backlog": "#7B7FBF",
    "lead_time": "#3B3FA0",
    "cycle_time": "#2E9E6E",
    "desvio_lt": "#7B7FBF",
    "desvio_ct": "#222222",
    "fluxo": "#9090E8",
    "fluxo_bench": "#00CC99",
    "bench_line": "#8888FF",
    "sla_nao": "#FFA500",
    "saude": "#7B7FBF",
    "retrabalho": "#FFA07A",
    "bench_rework": "#8888FF",
}

TEMPLATE = "plotly_white"
HEIGHT = 380


def fig_to_html(fig) -> str:
    return fig.to_html(full_html=False, include_plotlyjs=False, config={"displayModeBar": False})


def build_dashboard(df: pd.DataFrame) -> str:
    meses = get_months(df)
    if not meses:
        return "<p>Nenhum dado encontrado no CSV.</p>"

    labels = [label_mes(m) for m in meses]
    label_ano = [label_mes_ano(m) for m in meses]
    ultimo_mes = meses[-1]

    # ── Produtividade ────────────────────────────────
    tp_total = throughput_mensal(df)
    tp_def   = throughput_mensal(df, "Defeito")
    tp_his   = throughput_mensal(df, "História")
    ab_total = abertura_mensal(df)
    bl_total = backlog_por_mes(df)
    bl_def   = backlog_por_mes(df, "Defeito")
    bl_his   = backlog_por_mes(df, "História")
    vq_def   = vazao_qualificada_mensal(df, "Defeito")
    vq_his   = vazao_qualificada_mensal(df, "História")
    vq_equipe = vazao_por_equipe_mensal(df)

    backlog_atual = len(df[~df["concluido"]])
    entregas = [tp_total.get(m, 0) for m in meses]
    media_entregas = np.mean([e for e in entregas if e > 0]) if any(entregas) else 0
    burndown = backlog_atual / media_entregas if media_entregas > 0 else 0

    # KPIs cards
    kpi_html = f"""
    <div class="kpi-row">
      <div class="kpi-card">
        <div class="kpi-value">{backlog_atual}</div>
        <div class="kpi-label">Backlog</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-value">{media_entregas:.1f}</div>
        <div class="kpi-label">Média de Entregas</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-value">{burndown:.1f} Meses</div>
        <div class="kpi-label">Burn-Down Time</div>
      </div>
    </div>"""

    # Abertura x Throughput
    fig_abtp = go.Figure()
    fig_abtp.add_bar(x=labels, y=[ab_total.get(m, 0) for m in meses],
                     name="Itens Abertos", marker_color=COLORS["abertura"])
    fig_abtp.add_bar(x=labels, y=[tp_total.get(m, 0) for m in meses],
                     name="Itens Entregues", marker_color=COLORS["entregue"])
    fig_abtp.update_layout(title="Abertura x Throughput", barmode="group",
                            template=TEMPLATE, height=HEIGHT, legend=dict(orientation="h"))
    fig_abtp.update_traces(text=[ab_total.get(m, 0) for m in meses], textposition="outside", selector=dict(name="Itens Abertos"))
    fig_abtp.update_traces(text=[tp_total.get(m, 0) for m in meses], textposition="outside", selector=dict(name="Itens Entregues"))

    # Throughput Defeito x História
    fig_tp = go.Figure()
    fig_tp.add_bar(x=labels, y=[tp_def.get(m, 0) for m in meses],
                   name="Defeito", marker_color=COLORS["defeito"])
    fig_tp.add_bar(x=labels, y=[tp_his.get(m, 0) for m in meses],
                   name="História", marker_color=COLORS["historia"])
    fig_tp.update_layout(title="Throughput – Defeito x História", barmode="stack",
                          template=TEMPLATE, height=HEIGHT, legend=dict(orientation="h"))

    # Backlog Defeito x História
    fig_bl = go.Figure()
    fig_bl.add_bar(x=labels, y=[bl_def.get(m, 0) for m in meses],
                   name="Defeito", marker_color=COLORS["defeito"])
    fig_bl.add_bar(x=labels, y=[bl_his.get(m, 0) for m in meses],
                   name="História", marker_color=COLORS["historia"])
    fig_bl.update_layout(title="Backlog – Defeito x História", barmode="stack",
                          template=TEMPLATE, height=HEIGHT, legend=dict(orientation="h"))

    # Vazão Qualificada
    fig_vq = go.Figure()
    fig_vq.add_bar(x=labels, y=[vq_def.get(m, 0) for m in meses],
                   name="Defeito", marker_color=COLORS["defeito"])
    fig_vq.add_bar(x=labels, y=[vq_his.get(m, 0) for m in meses],
                   name="História", marker_color=COLORS["historia"])
    fig_vq.update_layout(title="Vazão Qualificada por Tipo (Defeito x História)", barmode="stack",
                          template=TEMPLATE, height=HEIGHT, legend=dict(orientation="h"))

    # Vazão por Equipe (tabela)
    if not vq_equipe.empty:
        equipe_cols = [m for m in meses if m in vq_equipe.columns]
        tab_rows = ""
        for equipe in vq_equipe.index:
            cells = "".join(f"<td>{vq_equipe.loc[equipe, m]:.1f}</td>" for m in equipe_cols)
            tab_rows += f"<tr><td>{equipe}</td>{cells}</tr>"
        tab_header = "".join(f"<th>{label_mes(m)}</th>" for m in equipe_cols)
        vq_equipe_html = f"""
        <div class="table-wrap">
          <h4>Vazão Qualificada Por Equipe</h4>
          <table class="data-table">
            <thead><tr><th>Equipe</th>{tab_header}</tr></thead>
            <tbody>{tab_rows}</tbody>
          </table>
        </div>"""
    else:
        vq_equipe_html = "<p><em>Dados de equipe não disponíveis.</em></p>"

    # ── Qualidade ────────────────────────────────────
    ab_def = abertura_mensal(df, "Defeito")
    tp_def2 = throughput_mensal(df, "Defeito")
    bl_def2 = backlog_por_mes(df, "Defeito")
    saude = saude_backlog_mensal(df)
    retrabalho = retrabalho_mensal(df)
    retrabalho_pct_atual = np.mean(list(retrabalho.values())) * 100 if retrabalho else 0

    # Percentual de retrabalho (KPI)
    kpi_qual_html = f"""
    <div class="kpi-row">
      <div class="kpi-card {'kpi-bad' if retrabalho_pct_atual > 20 else 'kpi-ok'}">
        <div class="kpi-value">{retrabalho_pct_atual:.1f}%</div>
        <div class="kpi-label">Percentual de Retrabalho</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-value">{saude.get(ultimo_mes, 1.0)*100:.0f}%</div>
        <div class="kpi-label">Saúde Backlog</div>
      </div>
    </div>"""

    # Abertura x Defeitos Entregues
    fig_adef = go.Figure()
    fig_adef.add_bar(x=labels, y=[ab_def.get(m, 0) for m in meses],
                     name="Defeitos Abertos", marker_color=COLORS["abertura"])
    fig_adef.add_bar(x=labels, y=[tp_def2.get(m, 0) for m in meses],
                     name="Defeitos Entregues", marker_color=COLORS["entregue"])
    fig_adef.update_layout(title="Abertura x Defeitos Entregues", barmode="group",
                            template=TEMPLATE, height=HEIGHT, legend=dict(orientation="h"))

    # Backlog Defeitos por Origem
    bl_def_cli = {}
    bl_def_int = {}
    for ym in meses:
        dt_fim = datetime.strptime(ym, "%Y-%m")
        if dt_fim.month == 12:
            dt_fim = dt_fim.replace(year=dt_fim.year + 1, month=1, day=1)
        else:
            dt_fim = dt_fim.replace(month=dt_fim.month + 1, day=1)
        sub_def = df[
            (df["tipo_class"] == "Defeito") &
            (df["criado"].notna()) & (df["criado"] < dt_fim) &
            (df["resolvido"].isna() | (df["resolvido"] >= dt_fim))
        ]
        bl_def_cli[ym] = len(sub_def[sub_def["origem"] == "Cliente"])
        bl_def_int[ym] = len(sub_def[sub_def["origem"] == "Interno"])

    fig_bl_orig = go.Figure()
    fig_bl_orig.add_bar(x=labels, y=[bl_def_cli.get(m, 0) for m in meses],
                         name="Cliente", marker_color=COLORS["defeito"])
    fig_bl_orig.add_bar(x=labels, y=[bl_def_int.get(m, 0) for m in meses],
                         name="Interno", marker_color=COLORS["historia"])
    fig_bl_orig.update_layout(title="Backlog Defeitos por Origem", barmode="stack",
                               template=TEMPLATE, height=HEIGHT, legend=dict(orientation="h"))

    # Saúde Backlog
    fig_saude = go.Figure()
    fig_saude.add_bar(x=labels, y=[saude.get(m, 0) * 100 for m in meses],
                      marker_color=COLORS["saude"], showlegend=False)
    fig_saude.update_layout(title="Saúde Backlog (%)", template=TEMPLATE, height=HEIGHT,
                             yaxis_ticksuffix="%")
    fig_saude.update_traces(text=[f"{saude.get(m,0)*100:.0f}%" for m in meses],
                             textposition="inside")

    # Taxa de Retrabalho
    fig_ret = go.Figure()
    fig_ret.add_scatter(x=labels, y=[retrabalho.get(m, 0) * 100 for m in meses],
                        mode="lines+markers", fill="tozeroy",
                        line=dict(color=COLORS["retrabalho"]),
                        name="Retrabalho")
    fig_ret.add_hline(y=BENCH_RETRABALHO * 100, line=dict(color=COLORS["bench_line"], dash="dash"),
                      annotation_text=f"Bench {BENCH_RETRABALHO*100:.0f}%")
    fig_ret.update_layout(title=f"Taxa de Retrabalho (Bench {BENCH_RETRABALHO*100:.0f}%)",
                           template=TEMPLATE, height=HEIGHT, yaxis_ticksuffix="%")

    # ── Velocidade ────────────────────────────────────
    lt85  = percentil85_mensal(df, "lead_time")
    ct85  = percentil85_mensal(df, "cycle_time")
    lt85d = percentil85_mensal(df, "lead_time", "Defeito")
    lt85h = percentil85_mensal(df, "lead_time", "História")
    ct85d = percentil85_mensal(df, "cycle_time", "Defeito")
    ct85h = percentil85_mensal(df, "cycle_time", "História")
    dp_lt = desvio_padrao_mensal(df, "lead_time")
    dp_ct = desvio_padrao_mensal(df, "cycle_time")
    dp_ltd = desvio_padrao_mensal(df, "lead_time", "Defeito")
    dp_lth = desvio_padrao_mensal(df, "lead_time", "História")
    dp_ctd = desvio_padrao_mensal(df, "cycle_time", "Defeito")
    dp_cth = desvio_padrao_mensal(df, "cycle_time", "História")
    fe     = flow_efficiency_mensal(df)

    lt85_geral = percentil85(list(lt85.values()))
    ct85_geral = percentil85(list(ct85.values()))

    kpi_vel_html = f"""
    <div class="kpi-row">
      <div class="kpi-card">
        <div class="kpi-value">{lt85_geral:.1f}</div>
        <div class="kpi-label">Lead Time P85 (dias)</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-value">{ct85_geral:.1f}</div>
        <div class="kpi-label">Cycle Time P85 (dias)</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-value">{np.mean(list(fe.values()))*100:.1f}%</div>
        <div class="kpi-label">Eficiência de Fluxo</div>
      </div>
    </div>""" if fe else f"""
    <div class="kpi-row">
      <div class="kpi-card">
        <div class="kpi-value">{lt85_geral:.1f}</div>
        <div class="kpi-label">Lead Time P85 (dias)</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-value">{ct85_geral:.1f}</div>
        <div class="kpi-label">Cycle Time P85 (dias)</div>
      </div>
    </div>"""

    # Lead Time vs Cycle Time P85
    fig_ltct = go.Figure()
    fig_ltct.add_bar(x=labels, y=[lt85.get(m, 0) for m in meses],
                     name="Lead Time P85", marker_color=COLORS["lead_time"])
    fig_ltct.add_bar(x=labels, y=[ct85.get(m, 0) for m in meses],
                     name="Cycle Time P85", marker_color=COLORS["cycle_time"])
    fig_ltct.update_layout(title="Lead Time vs Cycle Time (Percentil 85%)", barmode="group",
                            template=TEMPLATE, height=HEIGHT, legend=dict(orientation="h"))

    # Lead Time 85% por Tipo
    fig_lt_tipo = go.Figure()
    fig_lt_tipo.add_bar(x=labels, y=[lt85d.get(m, 0) for m in meses],
                        name="Defeito", marker_color=COLORS["defeito"])
    fig_lt_tipo.add_bar(x=labels, y=[lt85h.get(m, 0) for m in meses],
                        name="História", marker_color=COLORS["historia"])
    fig_lt_tipo.update_layout(title="Lead Time 85% por Tipo de Item", barmode="group",
                               template=TEMPLATE, height=HEIGHT, legend=dict(orientation="h"))

    # Cycle Time 85% por Tipo
    fig_ct_tipo = go.Figure()
    fig_ct_tipo.add_bar(x=labels, y=[ct85d.get(m, 0) for m in meses],
                        name="Defeito", marker_color=COLORS["defeito"])
    fig_ct_tipo.add_bar(x=labels, y=[ct85h.get(m, 0) for m in meses],
                        name="História", marker_color=COLORS["historia"])
    fig_ct_tipo.update_layout(title="Cycle Time 85% por Tipo de Item", barmode="group",
                               template=TEMPLATE, height=HEIGHT, legend=dict(orientation="h"))

    # Desvio Padrão Lead Time
    fig_dp_lt = go.Figure()
    fig_dp_lt.add_bar(x=labels, y=[dp_ltd.get(m, 0) for m in meses],
                      name="Defeito", marker_color=COLORS["defeito"])
    fig_dp_lt.add_bar(x=labels, y=[dp_lth.get(m, 0) for m in meses],
                      name="História", marker_color=COLORS["historia"])
    fig_dp_lt.update_layout(title="Desvio Padrão Lead Time", barmode="group",
                             template=TEMPLATE, height=HEIGHT, legend=dict(orientation="h"))

    # Desvio Padrão Cycle Time
    fig_dp_ct = go.Figure()
    fig_dp_ct.add_bar(x=labels, y=[dp_ctd.get(m, 0) for m in meses],
                      name="Defeito", marker_color=COLORS["defeito"])
    fig_dp_ct.add_bar(x=labels, y=[dp_cth.get(m, 0) for m in meses],
                      name="História", marker_color=COLORS["historia"])
    fig_dp_ct.update_layout(title="Desvio Padrão Cycle Time", barmode="group",
                             template=TEMPLATE, height=HEIGHT, legend=dict(orientation="h"))

    # Eficiência de Fluxo
    fig_fe = go.Figure()
    fig_fe.add_scatter(x=labels, y=[fe.get(m, 0) * 100 for m in meses],
                       mode="lines+markers", fill="tozeroy",
                       line=dict(color=COLORS["fluxo"]),
                       name="Eficiência de Fluxo")
    fig_fe.add_hline(y=BENCH_FLUXO * 100, line=dict(color=COLORS["fluxo_bench"], dash="dash"),
                     annotation_text=f"Ideal {BENCH_FLUXO*100:.0f}%")
    fig_fe.update_layout(title=f"Eficiência de Fluxo (Ideal {BENCH_FLUXO*100:.0f}%)",
                          template=TEMPLATE, height=HEIGHT, yaxis_ticksuffix="%")

    # Tempo por Status
    tps = tempo_por_status_total(df)
    status_names = infer_status_names(df)
    if tps:
        sorted_tps = sorted(tps.items(), key=lambda x: x[1], reverse=True)
        sid_labels = [status_names.get(k, f"ID {k}") for k, _ in sorted_tps]
        sid_vals   = [v for _, v in sorted_tps]
        bar_colors = []
        for k, _ in sorted_tps:
            if k in DONE_STATUS_IDS:
                bar_colors.append("#4CAF50")
            elif k in ACTIVE_STATUS_IDS:
                bar_colors.append(COLORS["historia"])
            else:
                bar_colors.append("#FFA500")
        fig_tps = go.Figure(go.Bar(x=sid_labels, y=sid_vals, marker_color=bar_colors,
                                    text=[f"{v:.0f}h" for v in sid_vals],
                                    textposition="outside"))
        fig_tps.update_layout(
            title="Tempo por Status (horas) — verde=done, azul=ativo, laranja=espera",
            template=TEMPLATE, height=HEIGHT, xaxis_tickangle=-20)
    else:
        fig_tps = go.Figure()
        fig_tps.update_layout(title="Tempo por Status – sem dados", template=TEMPLATE, height=HEIGHT)

    # Relação de Itens Entregues
    entregues = df[df["concluido"]].sort_values("resolvido", ascending=False).head(50)
    rel_rows = ""
    for _, row in entregues.iterrows():
        res = row["resolvido"].strftime("%d/%m/%Y") if pd.notna(row.get("resolvido")) else "–"
        lt  = f"{row['lead_time']:.1f}" if pd.notna(row.get("lead_time")) else "–"
        ct  = f"{row['cycle_time']:.1f}" if pd.notna(row.get("cycle_time")) else "–"
        vq  = f"{row['vazao_qual']:.1f}"
        rel_rows += (
            f"<tr><td>{row.get('key','')}</td>"
            f"<td>{str(row.get('resumo',''))[:60]}</td>"
            f"<td>{row.get('equipe','')}</td>"
            f"<td>{row.get('tipo_class','')}</td>"
            f"<td>{res}</td>"
            f"<td>{ct}</td><td>{lt}</td><td>{vq}</td></tr>"
        )
    relacao_html = f"""
    <div class="table-wrap">
      <h4>Relação de Itens Entregues (últimos 50)</h4>
      <table class="data-table">
        <thead>
          <tr>
            <th>Código</th><th>Título</th><th>Equipe</th><th>Tipo</th>
            <th>Dt. Resolução</th><th>Cycle Time</th><th>Lead Time</th><th>Vazão Qual.</th>
          </tr>
        </thead>
        <tbody>{rel_rows}</tbody>
      </table>
    </div>"""

    # ── Montagem HTML ────────────────────────────────
    plotlyjs = '<script src="https://cdn.plot.ly/plotly-latest.min.js"></script>'

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Indicadores de Eficiência de Tecnologia</title>
  {plotlyjs}
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ font-family: 'Segoe UI', sans-serif; background: #f4f5f7; color: #333; }}
    header {{
      background: #5B5FCF; color: white; text-align: center;
      padding: 18px; font-size: 1.4rem; font-weight: bold;
    }}
    .section {{ padding: 24px; }}
    .section-title {{
      color: #5B5FCF; font-size: 1.15rem; font-weight: bold;
      margin-bottom: 6px; border-bottom: 2px solid #5B5FCF; padding-bottom: 4px;
    }}
    .sub-title {{ font-size: 1rem; color: #444; margin: 16px 0 10px; }}
    .kpi-row {{ display: flex; gap: 16px; flex-wrap: wrap; margin: 16px 0; }}
    .kpi-card {{
      background: #888; color: white; border-radius: 10px;
      padding: 18px 24px; min-width: 150px; text-align: center;
    }}
    .kpi-card.kpi-bad {{ background: #c0392b; }}
    .kpi-card.kpi-ok {{ background: #27ae60; }}
    .kpi-value {{ font-size: 1.6rem; font-weight: bold; }}
    .kpi-label {{ font-size: 0.8rem; margin-top: 4px; }}
    .chart-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin: 16px 0; }}
    .chart-grid.single {{ grid-template-columns: 1fr; }}
    .chart-box {{ background: white; border-radius: 8px; padding: 12px; box-shadow: 0 1px 4px rgba(0,0,0,.08); }}
    .table-wrap {{ background: white; border-radius: 8px; padding: 16px; margin: 16px 0;
                   box-shadow: 0 1px 4px rgba(0,0,0,.08); overflow-x: auto; }}
    .table-wrap h4 {{ margin-bottom: 10px; color: #5B5FCF; }}
    .data-table {{ border-collapse: collapse; width: 100%; font-size: 0.82rem; }}
    .data-table th {{ background: #5B5FCF; color: white; padding: 6px 10px; text-align: left; }}
    .data-table td {{ padding: 5px 10px; border-bottom: 1px solid #eee; }}
    .data-table tr:hover td {{ background: #f0f0ff; }}
    .legend-box {{ background: white; border-radius: 8px; padding: 16px; margin: 16px 0;
                   box-shadow: 0 1px 4px rgba(0,0,0,.08); font-size: 0.88rem; line-height: 1.6; }}
    .legend-box b {{ color: #5B5FCF; }}
    hr {{ border: none; border-top: 1px solid #ddd; margin: 20px 0; }}
  </style>
</head>
<body>
  <header>Indicadores de Eficiência de Tecnologia</header>

  <!-- PRODUTIVIDADE -->
  <div class="section">
    <div class="section-title">Produtividade – Estamos conseguindo entregar valor para o cliente?</div>

    <div class="sub-title">– Abertura, Entregas e Backlog</div>
    {kpi_html}
    <div class="legend-box">
      <b>Backlog:</b> Itens criados que ainda não foram concluídos.<br>
      <b>Média de Entregas:</b> Quantidade de entregas por mês.<br>
      <b>Burn-Down Time:</b> Estimativa de meses necessários para finalizar os itens do backlog,
      baseado na média de entrega mensal. <b>Fórmula:</b> backlog / média de entregas mensais.
    </div>

    <div class="chart-grid">
      <div class="chart-box">{fig_to_html(fig_abtp)}</div>
      <div class="chart-box">
        {fig_to_html(fig_tp)}
      </div>
    </div>
    <div class="chart-grid">
      <div class="chart-box">{fig_to_html(fig_bl)}</div>
    </div>

    <div class="sub-title">– Vazão Qualificada</div>
    <div class="legend-box">
      No indicador de produtividade por Vazão Qualificada, são atribuídos pesos levando em
      consideração o tipo de item e seu respectivo Cycle Time (tempo de desenvolvimento):<br><br>
      Defeitos → <b>{PESO_DEFEITO} pontos</b><br>
      Histórias (até 1 dia) → <b>{PESO_HISTORIA_ATE_1_DIA} pontos</b><br>
      Histórias (1 a 3 dias) → <b>{PESO_HISTORIA_1_3_DIAS} pontos</b><br>
      Histórias (4 a 10 dias) → <b>{PESO_HISTORIA_4_10_DIAS} ponto</b><br>
      Histórias (11 dias ou mais) → <b>{PESO_HISTORIA_11_MAIS_DIAS} pontos</b>
    </div>
    <div class="chart-grid">
      <div class="chart-box">{fig_to_html(fig_vq)}</div>
      <div style="background:white;border-radius:8px;padding:12px;box-shadow:0 1px 4px rgba(0,0,0,.08);">
        {vq_equipe_html}
      </div>
    </div>
  </div>

  <hr>

  <!-- QUALIDADE -->
  <div class="section">
    <div class="section-title">Qualidade (corretivas) – Esse valor está sendo entregue com qualidade?</div>

    <div class="sub-title">– Aberturas, entregas e backlog de defeitos</div>
    {kpi_qual_html}

    <div class="chart-grid">
      <div class="chart-box">{fig_to_html(fig_adef)}</div>
      <div class="chart-box">{fig_to_html(fig_bl_orig)}</div>
    </div>

    <div class="sub-title">– Saúde do Backlog</div>
    <div class="legend-box">
      A Saúde do Backlog é determinada pela proporção de histórias em relação ao total de itens.
      Quanto maior o número de defeitos no backlog, menor será a saúde.<br>
      <b>OBS:</b> Quanto maior for a quantidade de defeitos em relação a histórias,
      menor será a saúde do backlog.
    </div>
    <div class="chart-grid">
      <div class="chart-box">{fig_to_html(fig_saude)}</div>
    </div>

    <div class="sub-title">– Taxa de Retrabalho</div>
    <div class="legend-box">
      O indicador de retrabalho mede a proporção do tempo gasto pela equipe em correções (defeitos)
      em comparação às melhorias (histórias), dentro de um mês.<br>
      Segundo o livro <b>"Accelerate"</b>, empresas de alta performance apresentam um índice de
      retrabalho inferior a {BENCH_RETRABALHO*100:.0f}%.<br>
      <b>Fórmula:</b> Touch Time Defeitos / (Touch Time Defeitos + Touch Time Histórias)
    </div>
    <div class="chart-grid">
      <div class="chart-box">{fig_to_html(fig_ret)}</div>
    </div>
  </div>

  <hr>

  <!-- VELOCIDADE -->
  <div class="section">
    <div class="section-title">Velocidade – Qual a cadência de entrega do time?</div>
    {kpi_vel_html}

    <div class="legend-box">
      <b>Lead Time:</b> Tempo total em dias corridos, desde a criação até a entrega do item.<br>
      <b>Cycle Time:</b> Tempo em dias corridos, desde o início do desenvolvimento até a entrega.<br>
      <b>Percentil 85%:</b> Com esse cálculo focamos nos valores mais comuns, eliminando extremos.
      Se uma equipe tem percentil 85% de 15 dias, indica que a equipe entrega em até 15 dias em 85% dos casos.
    </div>

    <div class="chart-grid">
      <div class="chart-box">{fig_to_html(fig_ltct)}</div>
      <div class="chart-box">{fig_to_html(fig_lt_tipo)}</div>
    </div>
    <div class="chart-grid">
      <div class="chart-box">{fig_to_html(fig_ct_tipo)}</div>
    </div>

    <div class="sub-title">– Desvio Padrão</div>
    <div class="legend-box">
      O desvio padrão do Cycle Time e do Lead Time quantifica a variação dos tempos de conclusão
      de itens em torno da média pela equipe.<br>
      <b>Desvio Padrão Elevado (negativo):</b> maior variabilidade nos processos.<br>
      <b>Desvio Padrão Baixo (positivo):</b> maior consistência no desempenho.
    </div>
    <div class="chart-grid">
      <div class="chart-box">{fig_to_html(fig_dp_lt)}</div>
      <div class="chart-box">{fig_to_html(fig_dp_ct)}</div>
    </div>

    <div class="sub-title">– Eficiência de Fluxo</div>
    <div class="legend-box">
      A eficiência de fluxo compara o <b>tempo ativo de trabalho (Touch Time) com o tempo total</b>,
      incluindo status de fila.<br>
      <b>Alta Eficiência (≥ {BENCH_FLUXO*100:.0f}%):</b> equipe trabalha de forma contínua e otimizada.<br>
      <b>Baixa Eficiência:</b> muitas pausas e esperas, sugerindo problemas no processo.<br>
      <em>Nota: eficiência de fluxo requer mapeamento dos status IDs para calcular Touch Time corretamente.</em>
    </div>
    <div class="chart-grid">
      <div class="chart-box">{fig_to_html(fig_fe)}</div>
    </div>

    <div class="sub-title">– Tempo por Status</div>
    <div class="legend-box">
      A métrica de tempo por status quantifica o tempo, em horas úteis, que cada item permanece
      em um determinado status dentro de um processo.<br>
      <b>OBS:</b> Os IDs de status abaixo são do Jira. Configure <code>ACTIVE_STATUS_IDS</code>
      e <code>DONE_STATUS_IDS</code> no topo do script para obter cálculos precisos de Touch Time.
    </div>
    <div class="chart-grid single">
      <div class="chart-box">{fig_to_html(fig_tps)}</div>
    </div>

    {relacao_html}
  </div>

  <footer style="text-align:center;padding:16px;color:#888;font-size:0.8rem;">
    Gerado em {datetime.now().strftime('%d/%m/%Y %H:%M')} a partir do CSV do Jira
  </footer>
</body>
</html>"""

    return html


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Dashboard de Indicadores Tech – Jira CSV")
    parser.add_argument("csv", nargs="?", default=CSV_PATH, help="Caminho do CSV exportado do Jira")
    parser.add_argument("--output", "-o", default="dashboard.html", help="Arquivo HTML de saída")
    parser.add_argument("--dump-status-ids", action="store_true",
                        help="Lista os IDs de status encontrados no campo [CHART] Time in Status")
    parser.add_argument("--map-status-ids", action="store_true",
                        help="Mostra o mapeamento inferido de IDs para categorias (ativo/espera/done)")
    args = parser.parse_args()

    csv_path = args.csv
    if not os.path.isfile(csv_path):
        print(f"Erro: arquivo não encontrado: {csv_path}", file=sys.stderr)
        sys.exit(1)

    print(f"Carregando {csv_path} ...")
    df = load_csv(csv_path)
    print(f"  {len(df)} itens carregados | {df['concluido'].sum()} concluídos | "
          f"{(~df['concluido']).sum()} em backlog")

    if args.map_status_ids:
        names = infer_status_names(df)
        tps = tempo_por_status_total(df)
        print("\nMapeamento inferido de Status IDs:")
        print(f"  {'ID':>8}  {'Horas':>8}  {'Categoria':>10}  Label")
        print("  " + "-"*60)
        for sid, label in sorted(names.items(), key=lambda x: -tps.get(x[0], 0)):
            cat = "DONE" if sid in DONE_STATUS_IDS else ("ATIVO" if sid in ACTIVE_STATUS_IDS else "ESPERA")
            print(f"  {sid:>8}  {tps.get(sid,0):>7.1f}h  {cat:>10}  {label}")
        print("\nAjuste ACTIVE_STATUS_IDS e DONE_STATUS_IDS no topo do script se necessário.")
        return

    if args.dump_status_ids:
        ids: dict[str, float] = defaultdict(float)
        for tis in df["time_in_status_parsed"]:
            for sid, ms in tis.items():
                ids[sid] += ms / 3600000
        print("\nIDs de Status encontrados (horas totais):")
        for sid, h in sorted(ids.items(), key=lambda x: -x[1]):
            print(f"  ID {sid:>8}: {h:>8.1f} h")
        print("\nConfigure ACTIVE_STATUS_IDS e DONE_STATUS_IDS no topo do script.")
        return

    out = args.output
    if not os.path.isabs(out):
        out = os.path.join(os.path.dirname(os.path.abspath(__file__)), out)

    print("Gerando dashboard ...")
    html = build_dashboard(df)
    with open(out, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Dashboard salvo em: {out}")
    print("Abra o arquivo no navegador para visualizar.")


if __name__ == "__main__":
    main()

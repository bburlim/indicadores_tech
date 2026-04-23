"""
Integração com Jira REST API v3 (Jira Cloud).

Busca issues via JQL, descobre automaticamente os IDs dos campos
customizados e retorna um DataFrame compatível com dashboard.py.

Configuração em .streamlit/secrets.toml:

    [jira]
    url       = "https://suaempresa.atlassian.net"
    email     = "usuario@empresa.com"
    api_token = "seu-api-token"
    jql       = "project = TEC ORDER BY created DESC"

Como gerar o API token:
    https://id.atlassian.com/manage-profile/security/api-tokens
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Dict, List, Optional
import time

import numpy as np
import pandas as pd
import requests
import streamlit as st
from requests.auth import HTTPBasicAuth

# ─────────────────────────────────────────────
# CAMPOS CUSTOMIZADOS — nomes buscados na API
# ─────────────────────────────────────────────

# O script descobre o ID real de cada campo pelo nome.
# Ajuste os nomes abaixo se o seu Jira usa nomes diferentes.
FIELD_NAMES = {
    "time_in_status":     "[CHART] Time in Status",
    "actual_start":       "Actual start",
    "actual_end":         "Actual end",
    "team_name":          "Team",
    "categoria_trabalho": "Categoria de trabalho",
    "categoria":          "Categoria",
    "sprint":             "Sprint",
}

# Mapeamento de statusCategory.name (EN) → PT para compatibilidade
STATUS_CATEGORY_MAP = {
    "To Do":       "Itens Pendentes",
    "In Progress": "Em andamento",
    "Done":        "Itens concluídos",
}


# ─────────────────────────────────────────────
# AUTENTICAÇÃO
# ─────────────────────────────────────────────

def _auth(email: str, api_token: str) -> HTTPBasicAuth:
    return HTTPBasicAuth(email, api_token)


def _headers() -> Dict:
    return {"Accept": "application/json", "Content-Type": "application/json"}


# ─────────────────────────────────────────────
# DESCOBERTA DE CAMPOS
# ─────────────────────────────────────────────

@st.cache_data(ttl=86400, show_spinner=False)
def discover_fields(jira_url: str, email: str, api_token: str) -> Dict[str, str]:
    """
    Retorna dict {apelido_interno: field_id} para todos os campos em FIELD_NAMES.
    Faz GET /rest/api/3/field e busca pelo nome (case-insensitive).
    Cache de 24h.
    """
    resp = requests.get(
        f"{jira_url}/rest/api/3/field",
        auth=_auth(email, api_token),
        headers=_headers(),
        timeout=30,
    )
    resp.raise_for_status()

    # Monta índice nome_lower → id
    name_to_id: Dict[str, str] = {}
    for f in resp.json():
        name_to_id[f["name"].lower()] = f["id"]

    result: Dict[str, str] = {}
    for alias, field_name in FIELD_NAMES.items():
        fid = name_to_id.get(field_name.lower())
        if fid:
            result[alias] = fid

    return result


# ─────────────────────────────────────────────
# BUSCA DE ISSUES
# ─────────────────────────────────────────────

def fetch_issues(
    jira_url: str,
    email: str,
    api_token: str,
    jql: str,
    field_map: Dict[str, str],
    page_size: int = 50,
    progress_callback=None,
) -> List[Dict]:
    """
    Busca todos os issues via POST /rest/api/3/search/jql (API atual do Jira Cloud).
    Usa paginação por cursor (nextPageToken) — o offset-based foi depreciado (410 Gone).
    """
    auth = _auth(email, api_token)

    standard = [
        "summary", "issuetype", "status", "created", "resolutiondate",
        "priority", "assignee", "reporter", "labels", "parent", "duedate",
    ]
    fields = list(dict.fromkeys(standard + list(field_map.values())))

    all_issues: List[Dict] = []
    next_page_token: Optional[str] = None

    while True:
        payload: Dict = {
            "jql":        jql,
            "maxResults": page_size,
            "fields":     fields,
            "expand":     "changelog",
        }
        if next_page_token:
            payload["nextPageToken"] = next_page_token

        resp = requests.post(
            f"{jira_url}/rest/api/3/search/jql",
            auth=auth,
            headers=_headers(),
            json=payload,
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()

        batch = data.get("issues", data.get("values", []))  # suporte a ambas as keys
        all_issues.extend(batch)

        if progress_callback:
            progress_callback(len(all_issues), f"{len(all_issues)} issues carregados")

        # Cursor para próxima página — ausente ou null quando é a última
        next_page_token = data.get("nextPageToken")
        if not next_page_token or data.get("isLast", False) or not batch:
            break

        time.sleep(0.1)

    return all_issues


# ─────────────────────────────────────────────
# PARSE DE DATAS E CAMPOS
# ─────────────────────────────────────────────

def _parse_date(val) -> Optional[datetime]:
    if not val:
        return None
    if isinstance(val, datetime):
        return val
    s = str(val).strip()
    # ISO 8601: 2025-01-15T10:30:00.000+0000
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d"):
        try:
            dt = datetime.strptime(s[:26], fmt[:len(fmt)])
            return dt.replace(tzinfo=None)
        except ValueError:
            continue
    try:
        return pd.to_datetime(s).to_pydatetime().replace(tzinfo=None)
    except Exception:
        return None


def _extract_team(val) -> str:
    if not val:
        return ""
    if isinstance(val, dict):
        return val.get("name", val.get("value", val.get("title", "")))
    if isinstance(val, list) and val:
        return _extract_team(val[0])
    return str(val).strip()


def _extract_string(val) -> str:
    if not val:
        return ""
    if isinstance(val, dict):
        return val.get("value", val.get("name", ""))
    if isinstance(val, list):
        return ", ".join(_extract_string(v) for v in val)
    return str(val).strip()


# ─────────────────────────────────────────────
# CONVERSÃO PARA DATAFRAME
# ─────────────────────────────────────────────

# Nomes de status que representam entrega (Done) no projeto ERM
_DONE_STATUS_NAMES = {"Concluído", "Released", "Done"}


def _first_active_date(issue: dict) -> Optional[datetime]:
    """
    Percorre o changelog e retorna a data da PRIMEIRA transição de status
    que indica início de trabalho (saída de 'To Do' / 'Backlog').
    Histories vêm em ordem cronológica reversa, então guardamos a mais antiga.
    Serve como fallback para actual_start quando o campo está vazio.
    """
    _NEW_STATUS_IDS = {"10039"}  # Tarefas pendentes (status inicial do projeto ERM)
    histories = issue.get("changelog", {}).get("histories", [])
    earliest: Optional[datetime] = None
    for history in histories:
        for item in history.get("items", []):
            if item.get("field") == "status" and item.get("from", "") in _NEW_STATUS_IDS:
                dt = _parse_date(history.get("created", ""))
                if dt:
                    earliest = dt  # como é reverso, sobrescreve até achar a mais antiga
    return earliest


def _changelog_active_ms(issue: dict) -> Optional[float]:
    """
    Calcula o tempo (em ms) que a issue passou em status ativos a partir
    do changelog.  Percorre as transições de status e acumula o intervalo
    entre a entrada e a saída de cada status ativo.

    Retorna None se não houver transições suficientes.
    """
    from dashboard import ACTIVE_STATUS_IDS
    histories = issue.get("changelog", {}).get("histories", [])

    # Coletar transições de status em ordem cronológica
    transitions: list = []  # [(datetime, to_id)]
    for history in histories:
        for item in history.get("items", []):
            if item.get("field") == "status":
                dt = _parse_date(history.get("created", ""))
                if dt:
                    transitions.append((dt, item.get("to", "")))

    if not transitions:
        return None

    transitions.sort(key=lambda t: t[0])

    total_ms = 0.0
    entered_active: Optional[datetime] = None

    for dt, to_id in transitions:
        if to_id in ACTIVE_STATUS_IDS:
            if entered_active is None:
                entered_active = dt
        else:
            if entered_active is not None:
                total_ms += (dt - entered_active).total_seconds() * 1000
                entered_active = None

    # Se ainda estava em status ativo no último registro, fechar com done_date
    if entered_active is not None:
        done_dt = _done_transition_date(issue)
        if done_dt:
            total_ms += (done_dt - entered_active).total_seconds() * 1000

    return total_ms if total_ms > 0 else None


def _done_transition_date(issue: dict) -> Optional[datetime]:
    """
    Percorre o changelog da issue e retorna a data da última transição
    para um status Done (Concluído / Released / Done).

    Usar o changelog garante que a data de entrega não seja alterada por
    eventos posteriores como comentários, edições ou mudanças de campo.
    Retorna None se não houver histórico de transição Done.
    """
    histories = issue.get("changelog", {}).get("histories", [])
    done_date: Optional[datetime] = None
    for history in histories:
        for item in history.get("items", []):
            if item.get("field") == "status" and item.get("toString", "") in _DONE_STATUS_NAMES:
                dt = _parse_date(history.get("created", ""))
                if dt:
                    done_date = dt  # mantém a mais recente caso haja reaberturas e re-fechamentos
    return done_date

def issues_to_dataframe(
    issues: List[Dict],
    field_map: Dict[str, str],
) -> pd.DataFrame:
    """
    Converte lista de issues da API Jira para DataFrame
    no mesmo formato produzido por dashboard.load_csv().
    """
    rows = []
    for issue in issues:
        f = issue.get("fields", {})

        # Categoria do status (PT-BR para compatibilidade com load_csv)
        cat_key = f.get("status", {}).get("statusCategory", {}).get("key", "")
        cat_en  = f.get("status", {}).get("statusCategory", {}).get("name", "")
        status_cat = STATUS_CATEGORY_MAP.get(cat_en, cat_en)

        # resolutiondate pode ser null mesmo para itens Done quando o workflow
        # do Jira não está configurado para setar a data automaticamente.
        # Fallback: data exata da transição para Done extraída do changelog.
        # Usa cat_key ("done") pois cat_en pode vir localizado ("Itens concluídos").
        resolvido_raw = f.get("resolutiondate", "") or ""
        resolvido_dt  = _parse_date(resolvido_raw)
        if resolvido_dt is None and cat_key == "done":
            resolvido_dt = _done_transition_date(issue)

        # Campos customizados via field_map
        tis_raw       = f.get(field_map.get("time_in_status", ""), "") or ""
        actual_start  = _parse_date(f.get(field_map.get("actual_start", ""), ""))
        if actual_start is None:
            actual_start = _first_active_date(issue)
        actual_end    = _parse_date(f.get(field_map.get("actual_end", ""), ""))
        equipe        = _extract_team(f.get(field_map.get("team_name", ""), ""))
        cat_trabalho  = _extract_string(f.get(field_map.get("categoria_trabalho", ""), ""))
        categoria     = _extract_string(f.get(field_map.get("categoria", ""), ""))

        # Sprint name
        sprint_raw = f.get(field_map.get("sprint", ""), []) or []
        sprint = ""
        if sprint_raw:
            if isinstance(sprint_raw, list) and sprint_raw:
                sv = sprint_raw[-1]  # sprint mais recente
                sprint = sv.get("name", "") if isinstance(sv, dict) else str(sv)
            elif isinstance(sprint_raw, dict):
                sprint = sprint_raw.get("name", "")

        assignee = f.get("assignee") or {}
        assignee_name = assignee.get("displayName", "")

        parent_raw     = f.get("parent") or {}
        parent_key     = parent_raw.get("key", "")
        parent_summary = parent_raw.get("fields", {}).get("summary", "") or parent_raw.get("summary", "")
        parent_type    = (parent_raw.get("fields", {}).get("issuetype", {}) or {}).get("name", "") or \
                         (parent_raw.get("issuetype", {}) or {}).get("name", "")

        row = {
            "key":                issue.get("key", ""),
            "resumo":             f.get("summary", ""),
            "tipo":               f.get("issuetype", {}).get("name", ""),
            "status":             f.get("status", {}).get("name", ""),
            "responsavel":        assignee_name,
            "parent_key":         parent_key,
            "parent_summary":     parent_summary,
            "parent_type":        parent_type,
            "status_cat":         status_cat,
            "status_cat_changed": "",
            "prioridade":         (f.get("priority") or {}).get("name", ""),
            "criado_str":         f.get("created", ""),
            "resolvido_str":      resolvido_raw,
            "actual_start_str":   "",
            "actual_end_str":     "",
            "criado":             _parse_date(f.get("created")),
            "resolvido":          resolvido_dt,
            "actual_start":       actual_start,
            "actual_end":         actual_end,
            "due_date":           _parse_date(f.get("duedate")),
            "equipe":             equipe,
            "time_in_status":     str(tis_raw) if tis_raw else "",
            "changelog_active_ms": _changelog_active_ms(issue),
            "categoria_trabalho": cat_trabalho,
            "categoria":          categoria,
            "labels":             ", ".join(f.get("labels", [])),
            "sprint":             sprint,
        }
        rows.append(row)

    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ─────────────────────────────────────────────
# FUNÇÃO PRINCIPAL — retorna DataFrame pronto
# ─────────────────────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def load_from_jira(
    jira_url: str,
    email: str,
    api_token: str,
    jql: str,
) -> pd.DataFrame:
    """
    Ponto de entrada principal.
    Busca issues do Jira e retorna DataFrame processado pelo dashboard.py.
    Cache de 1h — use st.cache_data.clear() para forçar atualização.
    """
    from dashboard import (
        parse_time_in_status,
        ACTIVE_STATUS_IDS,
        DONE_STATUS_IDS,
        DEFEITO_TYPES,
        HISTORIA_TYPES,
        SUBTAREFA_TYPES,
        PESO_DEFEITO,
        PESO_HISTORIA_ATE_1_DIA,
        PESO_HISTORIA_1_3_DIAS,
        PESO_HISTORIA_4_10_DIAS,
        PESO_HISTORIA_11_MAIS_DIAS,
        ORIGEM_CLIENTE_KEYWORDS,
    )

    field_map = discover_fields(jira_url, email, api_token)
    issues    = fetch_issues(jira_url, email, api_token, jql, field_map)

    df = issues_to_dataframe(issues, field_map)

    if df.empty:
        # Retorna DF vazio com todas as colunas para evitar KeyError no app
        return pd.DataFrame(columns=[
            "key", "resumo", "tipo", "status", "status_cat", "status_cat_changed",
            "prioridade", "criado_str", "resolvido_str", "actual_start_str",
            "actual_end_str", "criado", "resolvido", "actual_start", "actual_end",
            "equipe", "time_in_status", "time_in_status_parsed", "categoria_trabalho",
            "categoria", "labels", "sprint", "parent_key", "parent_summary", "parent_type",
            "due_date", "tipo_class", "concluido",
            "lead_time", "cycle_time", "touch_time_ms", "touch_time_dias",
            "flow_efficiency", "vazao_qual", "origem", "mes_criado", "mes_resolvido",
        ])

    # ── Aplica as mesmas transformações de dashboard.load_csv ──

    # Classificação Defeito / História
    def classifica_tipo(row):
        ct = str(row.get("categoria_trabalho", "")).strip().lower()
        t  = str(row.get("tipo", "")).strip()
        if ct:
            if "bug" in ct or "defeito" in ct:
                return "Defeito"
            if "hist" in ct or "story" in ct:
                return "História"
        if t in DEFEITO_TYPES:
            return "Defeito"
        if t in HISTORIA_TYPES:
            return "História"
        if t in SUBTAREFA_TYPES:
            return "Subtarefa"
        return "Outro"

    df["tipo_class"] = df.apply(classifica_tipo, axis=1)

    # Concluído
    df["concluido"] = df["status_cat"].str.strip().isin(
        ["Itens concluídos", "Done", "Concluído"]
    )

    # Lead Time
    def lead_time(row):
        if pd.notna(row.get("criado")) and pd.notna(row.get("resolvido")):
            return (row["resolvido"] - row["criado"]).total_seconds() / 86400
        return np.nan

    df["lead_time"] = df.apply(lead_time, axis=1)

    # Time in Status parse
    df["time_in_status_parsed"] = df["time_in_status"].apply(parse_time_in_status)

    # Cycle Time
    def cycle_time(row):
        if pd.notna(row.get("actual_start")) and pd.notna(row.get("resolvido")):
            return (row["resolvido"] - row["actual_start"]).total_seconds() / 86400
        tis = row.get("time_in_status_parsed", {})
        total_ms = sum(tis.values())
        lt = row.get("lead_time", np.nan)
        if not tis or total_ms == 0 or pd.isna(lt) or lt == 0:
            return np.nan
        if ACTIVE_STATUS_IDS and DONE_STATUS_IDS:
            active_ms = sum(ms for sid, ms in tis.items() if sid in ACTIVE_STATUS_IDS)
            done_ms   = sum(ms for sid, ms in tis.items() if sid in DONE_STATUS_IDS)
            workflow_ms = total_ms - done_ms
            if active_ms > 0 and workflow_ms > 0:
                return lt * (active_ms / workflow_ms)
        return np.nan

    df["cycle_time"] = df.apply(cycle_time, axis=1)

    # Touch Time
    def touch_time_ms(row):
        tis = row.get("time_in_status_parsed", {})
        if tis:
            if ACTIVE_STATUS_IDS:
                return sum(ms for sid, ms in tis.items() if sid in ACTIVE_STATUS_IDS)
            if DONE_STATUS_IDS:
                return sum(ms for sid, ms in tis.items() if sid not in DONE_STATUS_IDS)
            return sum(tis.values())
        # Fallback: tempo em status ativos calculado via changelog
        cl_ms = row.get("changelog_active_ms")
        if pd.notna(cl_ms) and cl_ms:
            return cl_ms
        return np.nan

    df["touch_time_ms"]   = df.apply(touch_time_ms, axis=1)
    df["touch_time_dias"] = df["touch_time_ms"] / 86400000

    # Flow Efficiency
    def flow_eff(row):
        lt = row.get("lead_time", np.nan)
        tt = row.get("touch_time_dias", np.nan)
        if pd.notna(lt) and pd.notna(tt) and lt > 0:
            return min(tt / lt, 1.0)
        return np.nan

    df["flow_efficiency"] = df.apply(flow_eff, axis=1)

    # Vazão Qualificada
    def vazao(row):
        if row["tipo_class"] == "Defeito":
            return PESO_DEFEITO
        if row["tipo_class"] in ("História", "Subtarefa"):
            ct = row.get("cycle_time", np.nan)
            if pd.isna(ct):
                return PESO_HISTORIA_4_10_DIAS
            if ct <= 1:
                return PESO_HISTORIA_ATE_1_DIA
            if ct <= 3:
                return PESO_HISTORIA_1_3_DIAS
            if ct <= 10:
                return PESO_HISTORIA_4_10_DIAS
            return PESO_HISTORIA_11_MAIS_DIAS
        return 0.0

    df["vazao_qual"] = df.apply(vazao, axis=1)

    # Origem
    def origem(row):
        texto = " ".join([
            str(row.get("resumo", "")),
            str(row.get("labels", "")),
            str(row.get("categoria", "")),
        ]).lower()
        for kw in ORIGEM_CLIENTE_KEYWORDS:
            if kw in texto:
                return "Cliente"
        return "Interno"

    df["origem"] = df.apply(origem, axis=1)

    # Mês criado / resolvido
    df["mes_criado"]    = df["criado"].apply(
        lambda d: d.strftime("%Y-%m") if pd.notna(d) else None
    )
    df["mes_resolvido"] = df["resolvido"].apply(
        lambda d: d.strftime("%Y-%m") if pd.notna(d) else None
    )

    return df


# ─────────────────────────────────────────────
# BUSCA DE ISSUES PAIS (ÉPICOS / OBJETIVOS)
# ─────────────────────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_parent_issues(
    jira_url: str,
    email: str,
    api_token: str,
    issue_keys: tuple,          # tuple para ser hashável pelo cache
) -> pd.DataFrame:
    """
    Dado um conjunto de chaves (ex.: épicos), busca esses issues na API
    e retorna key, summary, tipo, status_cat, parent_key e parent_summary.
    Usado para montar a cadeia Objetivo → Épico.
    """
    _EMPTY = pd.DataFrame(columns=[
        "key", "summary", "tipo", "status", "status_cat",
        "parent_key", "parent_summary", "parent_type",
        "start_date", "due_date",
    ])
    if not issue_keys:
        return _EMPTY

    jql = "key in (" + ", ".join(issue_keys) + ")"
    field_map = discover_fields(jira_url, email, api_token)

    try:
        issues = fetch_issues(jira_url, email, api_token, jql, field_map)
    except Exception:
        return _EMPTY

    rows = []
    for issue in issues:
        f = issue.get("fields", {})
        parent_raw  = f.get("parent") or {}
        p_fields    = parent_raw.get("fields") or {}
        cat_en      = f.get("status", {}).get("statusCategory", {}).get("name", "")
        rows.append({
            "key":            issue.get("key", ""),
            "summary":        f.get("summary", ""),
            "tipo":           f.get("issuetype", {}).get("name", ""),
            "status":         f.get("status", {}).get("name", ""),
            "status_cat":     STATUS_CATEGORY_MAP.get(cat_en, cat_en),
            "parent_key":     parent_raw.get("key", ""),
            "parent_summary": p_fields.get("summary", "") or parent_raw.get("summary", ""),
            "parent_type":    (p_fields.get("issuetype") or {}).get("name", ""),
            "start_date":     _parse_date(f.get(field_map.get("actual_start", "__x__"), ""))
                              or _parse_date(f.get("created")),
            "due_date":       _parse_date(f.get("duedate")),
        })

    return pd.DataFrame(rows) if rows else _EMPTY


# ─────────────────────────────────────────────
# HELPERS PARA app.py
# ─────────────────────────────────────────────

def jira_secrets_configured() -> bool:
    try:
        j = st.secrets.get("jira", {})
        return all(j.get(k) for k in ["url", "email", "api_token", "jql"])
    except Exception:
        return False


def get_jira_secrets() -> Dict:
    j = st.secrets["jira"]
    return {
        "jira_url":  j["url"].rstrip("/"),
        "email":     j["email"],
        "api_token": j["api_token"],
        "jql":       j["jql"],
    }


def debug_jql(jira_url: str, email: str, api_token: str, jql: str) -> Dict:
    """Faz uma chamada de teste com maxResults=1 e retorna a resposta bruta."""
    auth = _auth(email, api_token)
    payload = {"jql": jql, "maxResults": 1, "fields": ["summary", "issuetype", "status"]}
    resp = requests.post(
        f"{jira_url}/rest/api/3/search/jql",
        auth=auth, headers=_headers(), json=payload, timeout=30,
    )
    return {
        "status_code": resp.status_code,
        "jql_sent": jql,
        "response_keys": list(resp.json().keys()) if resp.ok else [],
        "total": resp.json().get("total", "n/a") if resp.ok else "n/a",
        "issues_count": len(resp.json().get("issues", resp.json().get("values", []))) if resp.ok else 0,
        "first_issue_key": (resp.json().get("issues") or resp.json().get("values") or [{}])[0].get("key", "—") if resp.ok else "—",
        "error": resp.text[:300] if not resp.ok else None,
    }


def test_connection(jira_url: str, email: str, api_token: str) -> tuple[bool, str]:
    """Testa credenciais via GET /rest/api/3/myself. Retorna (ok, mensagem)."""
    try:
        resp = requests.get(
            f"{jira_url}/rest/api/3/myself",
            auth=_auth(email, api_token),
            headers={"Accept": "application/json"},
            timeout=10,
        )
        if resp.status_code == 200:
            display = resp.json().get("displayName", email)
            return True, f"Conectado como {display}"
        return False, f"Erro {resp.status_code}: {resp.json().get('message', resp.text[:100])}"
    except Exception as e:
        return False, str(e)

"""
Integração com SharePoint via Microsoft Graph API.

Configuração necessária em .streamlit/secrets.toml (local)
ou nas Secrets do Streamlit Community Cloud:

    [sharepoint]
    tenant_id     = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    client_id     = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    client_secret = "seu-client-secret"
    site_url      = "https://suaempresa.sharepoint.com/sites/nome-do-site"
    file_path     = "Documentos Compartilhados/indicadores/jira.csv"
"""

import io
import requests
import msal
import streamlit as st


def _get_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    """Obtém token de acesso via client credentials (app-only)."""
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = msal.ConfidentialClientApplication(
        client_id,
        authority=authority,
        client_credential=client_secret,
    )
    result = app.acquire_token_for_client(
        scopes=["https://graph.microsoft.com/.default"]
    )
    if "access_token" not in result:
        error = result.get("error_description", result.get("error", "Erro desconhecido"))
        raise RuntimeError(f"Falha ao autenticar no Azure AD: {error}")
    return result["access_token"]


def _get_site_id(token: str, site_url: str) -> str:
    """
    Resolve o site_id do SharePoint a partir da URL.
    Ex: https://empresa.sharepoint.com/sites/meu-site
    """
    # Separa hostname e caminho do site
    # Ex: hostname = empresa.sharepoint.com | site_path = /sites/meu-site
    url = site_url.rstrip("/")
    parts = url.split("/", 3)          # ['https:', '', 'hostname', 'sites/nome']
    hostname = parts[2]
    site_path = "/" + parts[3] if len(parts) > 3 else ""

    endpoint = f"https://graph.microsoft.com/v1.0/sites/{hostname}:{site_path}"
    resp = requests.get(endpoint, headers={"Authorization": f"Bearer {token}"}, timeout=30)

    if resp.status_code != 200:
        raise RuntimeError(
            f"Site SharePoint não encontrado ({resp.status_code}): {resp.text[:300]}"
        )
    return resp.json()["id"]


def _get_drive_id(token: str, site_id: str) -> str:
    """Retorna o drive padrão (Documents) do site."""
    endpoint = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive"
    resp = requests.get(endpoint, headers={"Authorization": f"Bearer {token}"}, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"Drive não encontrado ({resp.status_code}): {resp.text[:300]}")
    return resp.json()["id"]


def download_csv(
    tenant_id: str,
    client_id: str,
    client_secret: str,
    site_url: str,
    file_path: str,
) -> io.BytesIO:
    """
    Baixa o arquivo CSV do SharePoint e retorna um objeto BytesIO.

    Parâmetros
    ----------
    tenant_id     : ID do tenant Azure AD
    client_id     : ID do app registrado
    client_secret : Secret do app
    site_url      : URL do site SharePoint (ex: https://empresa.sharepoint.com/sites/meu-site)
    file_path     : Caminho do arquivo dentro do drive
                    (ex: "Documentos Compartilhados/indicadores/jira.csv")
    """
    token   = _get_token(tenant_id, client_id, client_secret)
    site_id = _get_site_id(token, site_url)
    drive_id = _get_drive_id(token, site_id)

    # Monta endpoint para download direto pelo caminho
    encoded_path = file_path.strip("/")
    endpoint = (
        f"https://graph.microsoft.com/v1.0"
        f"/sites/{site_id}/drives/{drive_id}/root:/{encoded_path}:/content"
    )

    resp = requests.get(
        endpoint,
        headers={"Authorization": f"Bearer {token}"},
        timeout=60,
    )

    if resp.status_code == 404:
        raise FileNotFoundError(
            f"Arquivo não encontrado no SharePoint: {file_path}\n"
            "Verifique o caminho e as permissões do app."
        )
    if resp.status_code != 200:
        raise RuntimeError(
            f"Erro ao baixar arquivo ({resp.status_code}): {resp.text[:300]}"
        )

    return io.BytesIO(resp.content)


@st.cache_data(ttl=3600, show_spinner=False)
def load_from_sharepoint(
    tenant_id: str,
    client_id: str,
    client_secret: str,
    site_url: str,
    file_path: str,
) -> io.BytesIO:
    """
    Versão com cache do Streamlit (recarrega após 1 hora).
    Use st.cache_data.clear() para forçar atualização manual.
    """
    return download_csv(tenant_id, client_id, client_secret, site_url, file_path)


def secrets_configured() -> bool:
    """Verifica se as secrets do SharePoint estão configuradas."""
    try:
        sp = st.secrets.get("sharepoint", {})
        required = ["tenant_id", "client_id", "client_secret", "site_url", "file_path"]
        return all(sp.get(k) for k in required)
    except Exception:
        return False


def get_secrets() -> dict:
    """Retorna as secrets do SharePoint como dict."""
    sp = st.secrets["sharepoint"]
    return {
        "tenant_id":     sp["tenant_id"],
        "client_id":     sp["client_id"],
        "client_secret": sp["client_secret"],
        "site_url":      sp["site_url"],
        "file_path":     sp["file_path"],
    }

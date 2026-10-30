"""
PDFocus Web — Felipe Ceballos
Busca referencias en PDFs almacenados en Google Drive y los devuelve marcados.
Sistema de login por token + contraseña por empresa, almacenado en Google Sheets.
Caché del índice en Google Sheets para sobrevivir reinicios y deploys.

VERSIÓN MULTI-EMPRESA
  Tipos de perfil:
    EMPRESA   → solo ve los documentos de SUS carpetas (Manifiestos, Facturas, Otros).
    UNIVERSAL → solo ve la carpeta de manifiestos compartidos (una sola para todos los
                perfiles universales).
    GENERAL   → ve los documentos de TODAS las empresas y de la universal.

  Compatibilidad: una EMPRESA sin carpetas propias usa las carpetas de siempre
  (FOLDER_MANIFIESTOS / FOLDER_FACTURAS). Así nada cambia hasta que se le asignen.

Estructura Google Sheets (usuarios_pdfocus):
  A: empresa | B: token | C: activo (SI/NO) | D: fecha_creacion | E: password_hash
  F: tipo (EMPRESA/UNIVERSAL/GENERAL) | G: carpeta manifiestos | H: carpeta facturas
  I: carpeta otros documentos
"""

import os
import re
import io
import json
import time
import threading
import traceback
import secrets
import string
from datetime import datetime

import gzip
import base64
import pymupdf as fitz
from flask import (
    Flask, request, jsonify, send_file,
    render_template, redirect, url_for, make_response, session
)
from werkzeug.security import generate_password_hash, check_password_hash
import requests as _requests
from google.oauth2 import service_account
from google.auth.transport.requests import Request as _AuthRequest

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "pdfocus_secret_2026_xK9mN3pQ")

# ── CONFIG ────────────────────────────────────────────────────
# Carpetas "de siempre" (las que usa KINO hoy). Se siguen usando para las empresas
# que todavía no tienen carpetas propias asignadas.
FOLDER_ID_MANIFIESTOS = os.environ.get("FOLDER_MANIFIESTOS", "")
FOLDER_ID_FACTURAS    = os.environ.get("FOLDER_FACTURAS", "")
# Carpeta de Drive donde se crean automáticamente las carpetas de cada empresa.
# Debe estar compartida con la cuenta de servicio como EDITOR.
FOLDER_RAIZ_EMPRESAS  = os.environ.get("FOLDER_RAIZ_EMPRESAS", "")
SERVICE_ACCOUNT_JSON  = os.environ.get("GOOGLE_SERVICE_ACCOUNT", "")
ADMIN_TOKEN           = os.environ.get("ADMIN_TOKEN", "pdfocus_kino_2026")
SHEETS_ID             = os.environ.get("SHEETS_ID", "1a-lIDPDbimXLVys0vS6keqls-fz4bCU1pq28qiZ2qJY")
BASE_URL              = os.environ.get("BASE_URL", "https://pdfocus-web-1sbt.onrender.com")

# Tipos de documento y modo de resaltado de cada uno
TIPOS_DOC   = ("manifiestos", "facturas", "otros")
MODO_DOC    = {"manifiestos": "manifesto", "facturas": "factura", "otros": "manifesto"}
NOMBRE_DOC  = {"manifiestos": "Manifiestos", "facturas": "Facturas", "otros": "Otros documentos"}

# Tipos de perfil
PERFIL_EMPRESA   = "EMPRESA"
PERFIL_UNIVERSAL = "UNIVERSAL"
PERFIL_GENERAL   = "GENERAL"
PERFILES = (PERFIL_EMPRESA, PERFIL_UNIVERSAL, PERFIL_GENERAL)

# Nombre del archivo de caché en Google Drive (se mantiene como fallback)
DRIVE_CACHE_FILENAME  = "pdfocus_index_cache.json"

# Hoja de Sheets para el caché del índice
CACHE_SHEET_NAME = "pdfocus_cache"
# Tamaño máximo de cada chunk en Sheets (50.000 chars por celda, usamos 45.000 para seguridad)
CACHE_CHUNK_SIZE = 45000

# Ruta temporal para el caché local
INDEX_CACHE_PATH = "/tmp/pdfocus_index_cache.json"

# Caché en memoria. La clave es el ID de la CARPETA de Drive:
#   _INDEX_CACHE[folder_id] = {file_id: {"name", "compact" (gzip bytes), "modified_time"}}
# El campo "compact" se guarda comprimido para reducir uso de RAM ~75%.
_INDEX_CACHE = {}
_INDEX_LOCK  = threading.Lock()
_INDEX_BUILT = {}          # folder_id -> True cuando ya está indexada
_STARTUP_DONE = threading.Event()


def _compress_compact(text: str) -> bytes:
    return gzip.compress(text.encode("utf-8"), compresslevel=1)


def _decompress_compact(data: bytes) -> str:
    return gzip.decompress(data).decode("utf-8")

# Cache de usuarios (incluye activos e inactivos)
_USERS_CACHE  = {}
_USERS_LOCK   = threading.Lock()
_USERS_LOADED = {"ts": 0}

# Tiempo máximo de espera para cada llamada a Google (segundos): (conexión, respuesta).
# Sin esto, si Google no responde, el proceso se queda esperando para siempre.
_TIMEOUT_API      = (15, 60)
_TIMEOUT_DESCARGA = (15, 180)

# Intervalo de actualización incremental (15 minutos)
_UPDATE_INTERVAL = 900


# ── GOOGLE SERVICES (REST directo — sin googleapiclient.discovery) ────────────
def _get_credentials():
    info = json.loads(SERVICE_ACCOUNT_JSON)
    scopes = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
    ]
    return service_account.Credentials.from_service_account_info(info, scopes=scopes)


def _get_token():
    """Devuelve un access token válido para las APIs de Google."""
    creds = _get_credentials()
    creds.refresh(_AuthRequest())
    return creds.token


def _drive_get(path, params=None):
    token = _get_token()
    url = f"https://www.googleapis.com/drive/v3/{path}"
    r = _requests.get(url, headers={"Authorization": f"Bearer {token}"}, params=params or {}, timeout=_TIMEOUT_API)
    r.raise_for_status()
    return r.json()


def _drive_get_media(file_id):
    token = _get_token()
    url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
    r = _requests.get(url, headers={"Authorization": f"Bearer {token}"}, stream=True, timeout=_TIMEOUT_DESCARGA)
    r.raise_for_status()
    return r.content


def _sheets_get(range_):
    token = _get_token()
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEETS_ID}/values/{_url_encode(range_)}"
    r = _requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=_TIMEOUT_API)
    r.raise_for_status()
    return r.json()


def _sheets_update(range_, values):
    token = _get_token()
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEETS_ID}/values/{_url_encode(range_)}?valueInputOption=RAW"
    r = _requests.put(url, headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                      json={"values": values}, timeout=_TIMEOUT_API)
    r.raise_for_status()
    return r.json()


def _sheets_append(range_, values):
    token = _get_token()
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEETS_ID}/values/{_url_encode(range_)}:append?valueInputOption=RAW&insertDataOption=INSERT_ROWS"
    r = _requests.post(url, headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                       json={"values": values}, timeout=_TIMEOUT_API)
    r.raise_for_status()
    return r.json()


def _sheets_clear(range_):
    token = _get_token()
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEETS_ID}/values/{_url_encode(range_)}:clear"
    r = _requests.post(url, headers={"Authorization": f"Bearer {token}"}, timeout=_TIMEOUT_API)
    r.raise_for_status()
    return r.json()


def _sheets_batch_update(body):
    token = _get_token()
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEETS_ID}:batchUpdate"
    r = _requests.post(url, headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                       json=body, timeout=_TIMEOUT_API)
    r.raise_for_status()
    return r.json()


def _sheets_get_meta():
    token = _get_token()
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEETS_ID}?fields=sheets.properties"
    r = _requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=_TIMEOUT_API)
    r.raise_for_status()
    return r.json()


def _url_encode(s):
    from urllib.parse import quote
    return quote(s, safe="!$&'()*+,;=:@/")



def _drive_crear_carpeta(nombre, parent_id):
    """Crea una carpeta en Drive dentro de parent_id y devuelve su ID."""
    token = _get_token()
    r = _requests.post(
        "https://www.googleapis.com/drive/v3/files",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        params={"supportsAllDrives": "true", "fields": "id"},
        json={"name": nombre, "mimeType": "application/vnd.google-apps.folder", "parents": [parent_id]}, timeout=_TIMEOUT_API)
    r.raise_for_status()
    return r.json()["id"]


# ── CACHÉ EN GOOGLE SHEETS ────────────────────────────────────
def _migrar_claves_cache(data):
    """
    El caché de la versión anterior usaba las claves "manifiestos"/"facturas".
    Ahora la clave es el ID de la carpeta. Se convierten sin reindexar nada.
    """
    if "manifiestos" in data and FOLDER_ID_MANIFIESTOS:
        data.setdefault(FOLDER_ID_MANIFIESTOS, {}).update(data.pop("manifiestos"))
    if "facturas" in data and FOLDER_ID_FACTURAS:
        data.setdefault(FOLDER_ID_FACTURAS, {}).update(data.pop("facturas"))
    data.pop("manifiestos", None)
    data.pop("facturas", None)
    return data


def _ensure_cache_sheet():
    """Crea la hoja pdfocus_cache si no existe."""
    try:
        meta = _sheets_get_meta()
        names = [s["properties"]["title"] for s in meta.get("sheets", [])]
        if CACHE_SHEET_NAME not in names:
            _sheets_batch_update({"requests": [{"addSheet": {"properties": {"title": CACHE_SHEET_NAME}}}]})
            print(f"[SHEETS-CACHE] Hoja '{CACHE_SHEET_NAME}' creada.", flush=True)
        return True
    except Exception as e:
        print(f"[SHEETS-CACHE] Error verificando hoja: {e}", flush=True)
        return False


def _save_index_to_drive():
    """Guarda el índice comprimido en Google Sheets (chunks de base64)."""
    try:
        with _INDEX_LOCK:
            data = dict(_INDEX_CACHE)
        total = sum(len(v) for v in data.values())
        if total == 0:
            return
        # Descomprimir compact (bytes) a string antes de serializar a JSON
        serializable = {}
        for sec_key, entries in data.items():
            serializable[sec_key] = {}
            for fid, info in entries.items():
                entry = dict(info)
                if isinstance(entry.get("compact"), bytes):
                    entry["compact"] = _decompress_compact(entry["compact"])
                serializable[sec_key][fid] = entry
        # Comprimir con gzip y codificar en base64
        json_bytes = json.dumps(serializable, ensure_ascii=False).encode("utf-8")
        compressed = gzip.compress(json_bytes, compresslevel=9)
        b64 = base64.b64encode(compressed).decode("ascii")
        # Dividir en chunks
        chunks = [b64[i:i+CACHE_CHUNK_SIZE] for i in range(0, len(b64), CACHE_CHUNK_SIZE)]
        print(f"[SHEETS-CACHE] Guardando {total} PDFs en {len(chunks)} chunks ({len(b64)} chars)...", flush=True)

        _ensure_cache_sheet()

        # Limpiar hoja y escribir chunks
        _sheets_clear(f"{CACHE_SHEET_NAME}!A:B")
        rows = [[str(i), chunk] for i, chunk in enumerate(chunks)]
        _sheets_update(f"{CACHE_SHEET_NAME}!A1", rows)
        print(f"[SHEETS-CACHE] ✅ Caché guardado en Sheets: {total} PDFs, {len(chunks)} chunks.", flush=True)
    except Exception as e:
        print(f"[SHEETS-CACHE] Error guardando caché: {e}", flush=True)


def _load_index_from_drive():
    """Carga el índice comprimido desde Google Sheets. Reintenta 3 veces."""
    for intento in range(1, 4):
        try:
            print(f"[SHEETS-CACHE] Intento {intento}/3 de carga desde Sheets...", flush=True)
            result = _sheets_get(f"{CACHE_SHEET_NAME}!A:B")
            rows = result.get("values", [])
            if not rows:
                print("[SHEETS-CACHE] No hay caché en Sheets.", flush=True)
                return False
            # Ordenar por número de chunk y concatenar
            rows.sort(key=lambda r: int(r[0]) if r and r[0].isdigit() else 0)
            b64 = "".join(r[1] for r in rows if len(r) > 1)
            if not b64:
                print("[SHEETS-CACHE] Caché vacío (sin datos en columna B).", flush=True)
                return False
            print(f"[SHEETS-CACHE] {len(rows)} chunks leídos ({len(b64)} chars). Descomprimiendo...", flush=True)
            compressed = base64.b64decode(b64)
            json_bytes = gzip.decompress(compressed)
            data = json.loads(json_bytes.decode("utf-8"))
            if not data:
                print("[SHEETS-CACHE] JSON vacío tras descomprimir.", flush=True)
                return False
            # Comprimir compact en memoria para ahorrar RAM
            for sec_key, entries in data.items():
                for fid, info in entries.items():
                    if isinstance(info.get("compact"), str):
                        info["compact"] = _compress_compact(info["compact"])
            data = _migrar_claves_cache(data)
            with _INDEX_LOCK:
                _INDEX_CACHE.clear()
                _INDEX_CACHE.update(data)
                for sec_key in data:
                    _INDEX_BUILT[sec_key] = True
            total = sum(len(v) for v in data.values())
            print(f"[SHEETS-CACHE] ✅ Índice cargado desde Sheets: {total} PDFs.", flush=True)
            try:
                # Guardar en disco con compact como string (para compatibilidad)
                disk_data = {}
                for sec_key, entries in data.items():
                    disk_data[sec_key] = {}
                    for fid, info in entries.items():
                        disk_data[sec_key][fid] = dict(info)
                        if isinstance(info.get("compact"), bytes):
                            disk_data[sec_key][fid]["compact"] = _decompress_compact(info["compact"])
                with open(INDEX_CACHE_PATH, "w", encoding="utf-8") as f:
                    json.dump(disk_data, f, ensure_ascii=False)
            except Exception:
                pass
            return True
        except Exception as e:
            print(f"[SHEETS-CACHE] ❌ Error intento {intento}/3: {e}", flush=True)
            traceback.print_exc()
            if intento < 3:
                time.sleep(5)
    print("[SHEETS-CACHE] ❌ Falló carga desde Sheets después de 3 intentos.", flush=True)
    return False


def _save_index_to_disk():
    try:
        with _INDEX_LOCK:
            data = dict(_INDEX_CACHE)
        # Descomprimir compact (bytes) a string antes de serializar
        serializable = {}
        for sec_key, entries in data.items():
            serializable[sec_key] = {}
            for fid, info in entries.items():
                entry = dict(info)
                if isinstance(entry.get("compact"), bytes):
                    entry["compact"] = _decompress_compact(entry["compact"])
                serializable[sec_key][fid] = entry
        with open(INDEX_CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(serializable, f, ensure_ascii=False)
        total = sum(len(v) for v in data.values())
        print(f"[CACHE-LOCAL] Copia local guardada: {total} PDFs.", flush=True)
    except Exception as e:
        print(f"[CACHE-LOCAL] Error guardando copia local: {e}", flush=True)


def _load_index_from_disk():
    if not os.path.exists(INDEX_CACHE_PATH):
        return False
    try:
        with open(INDEX_CACHE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not data:
            return False
        # Comprimir compact en memoria para ahorrar RAM
        for sec_key, entries in data.items():
            for fid, info in entries.items():
                if isinstance(info.get("compact"), str):
                    info["compact"] = _compress_compact(info["compact"])
        data = _migrar_claves_cache(data)
        with _INDEX_LOCK:
            _INDEX_CACHE.clear()
            _INDEX_CACHE.update(data)
            for sec_key in data:
                _INDEX_BUILT[sec_key] = True
        total = sum(len(v) for v in data.values())
        print(f"[CACHE-LOCAL] Índice cargado desde /tmp: {total} PDFs.", flush=True)
        return True
    except Exception as e:
        print(f"[CACHE-LOCAL] Error cargando desde /tmp: {e}", flush=True)
        return False



# ── GOOGLE SHEETS — USUARIOS ──────────────────────────────────
# Columnas: A=empresa, B=token, C=activo, D=fecha_creacion, E=password_hash,
#           F=tipo, G=carpeta manifiestos, H=carpeta facturas, I=carpeta otros

def _celda(row, i):
    return row[i].strip() if len(row) > i and row[i] else ""


def _load_users_from_sheets():
    try:
        result = _sheets_get("usuarios_pdfocus!A2:I1000")
        rows = result.get("values", [])
        users = {}
        for row in rows:
            if len(row) < 3:
                continue
            empresa = _celda(row, 0)
            token   = _celda(row, 1)
            if not token:
                continue
            tipo = _celda(row, 5).upper() or PERFIL_EMPRESA
            if tipo not in PERFILES:
                tipo = PERFIL_EMPRESA
            users[token] = {
                "empresa": empresa,
                "activo": _celda(row, 2).upper() == "SI",
                "fecha_creacion": _celda(row, 3),
                "password_hash": _celda(row, 4),
                "tipo": tipo,
                "carpetas": {
                    "manifiestos": _celda(row, 6),
                    "facturas":    _celda(row, 7),
                    "otros":       _celda(row, 8),
                },
            }
        return users
    except Exception as e:
        print(f"[SHEETS] Error cargando usuarios: {e}", flush=True)
        return {}


_USERS_LOADING = threading.Event()  # evita cargas simultáneas

def _reload_users_sync():
    """Carga usuarios desde Sheets de forma síncrona."""
    users = _load_users_from_sheets()
    with _USERS_LOCK:
        _USERS_CACHE.clear()
        _USERS_CACHE.update(users)
        _USERS_LOADED["ts"] = time.time()

def _get_users(force_reload=False):
    """Devuelve el caché de usuarios. Si necesita recarga, lo hace en background."""
    now = time.time()
    with _USERS_LOCK:
        cache_age = now - _USERS_LOADED["ts"]
        needs_reload = force_reload or cache_age > 300
        current = dict(_USERS_CACHE)
        cache_empty = not current

    if cache_empty and _USERS_LOADING.is_set():
        _USERS_LOADING.wait(timeout=8)
        with _USERS_LOCK:
            current = dict(_USERS_CACHE)

    if needs_reload and not _USERS_LOADING.is_set() and not cache_empty:
        def _do_reload():
            if _USERS_LOADING.is_set():
                return
            _USERS_LOADING.set()
            try:
                _reload_users_sync()
            finally:
                _USERS_LOADING.clear()
        threading.Thread(target=_do_reload, daemon=True).start()

    return current


def _users_snapshot():
    """Usuarios en memoria; si está vacío, los carga directo."""
    if _USERS_LOADING.is_set():
        _USERS_LOADING.wait(timeout=5)
    with _USERS_LOCK:
        users = dict(_USERS_CACHE)
    if not users:
        try:
            _reload_users_sync()
            with _USERS_LOCK:
                users = dict(_USERS_CACHE)
        except Exception as e:
            print(f"[USERS] Error cargando usuarios directo: {e}", flush=True)
    return users


def _fila_de_token(token):
    result = _sheets_get("usuarios_pdfocus!B2:B1000")
    for i, row in enumerate(result.get("values", [])):
        if row and row[0].strip() == token:
            return i + 2
    return None


def _add_user_to_sheets(empresa, token, tipo=PERFIL_EMPRESA, carpetas=None):
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M")
    c = carpetas or {}
    try:
        _sheets_append("usuarios_pdfocus!A:I", [[
            empresa, token, "SI", fecha, "", tipo,
            c.get("manifiestos", ""), c.get("facturas", ""), c.get("otros", ""),
        ]])
        _reload_users_sync()
        return True
    except Exception as e:
        print(f"[SHEETS] Error agregando usuario: {e}", flush=True)
        return False


def _set_user_carpetas(token, carpetas):
    try:
        fila = _fila_de_token(token)
        if not fila:
            return False
        _sheets_update(f"usuarios_pdfocus!G{fila}:I{fila}", [[
            carpetas.get("manifiestos", ""), carpetas.get("facturas", ""), carpetas.get("otros", ""),
        ]])
        _reload_users_sync()
        return True
    except Exception as e:
        print(f"[SHEETS] Error guardando carpetas: {e}", flush=True)
        return False


def _set_user_password(token, new_password):
    """Establece o cambia la contraseña de una empresa. Guarda el hash en columna E."""
    try:
        fila = _fila_de_token(token)
        if not fila:
            return False
        _sheets_update(f"usuarios_pdfocus!E{fila}", [[generate_password_hash(new_password)]])
        _reload_users_sync()
        return True
    except Exception as e:
        print(f"[SHEETS] Error cambiando contraseña: {e}", flush=True)
        return False


def _set_user_status(token, activo):
    try:
        fila = _fila_de_token(token)
        if not fila:
            return False
        _sheets_update(f"usuarios_pdfocus!C{fila}", [["SI" if activo else "NO"]])
        _reload_users_sync()
        return True
    except Exception as e:
        print(f"[SHEETS] Error cambiando estado: {e}", flush=True)
        return False


def _delete_user_from_sheets(token):
    try:
        row_num = _fila_de_token(token)
        if not row_num:
            return False
        meta = _sheets_get_meta()
        sheet_id = None
        for sh in meta.get("sheets", []):
            if sh["properties"]["title"] == "usuarios_pdfocus":
                sheet_id = sh["properties"]["sheetId"]
        if sheet_id is None:
            sheet_id = meta["sheets"][0]["properties"]["sheetId"]
        _sheets_batch_update({"requests": [{
            "deleteDimension": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": row_num - 1,
                    "endIndex": row_num,
                }
            }
        }]})
        _reload_users_sync()
        return True
    except Exception as e:
        print(f"[SHEETS] Error eliminando usuario: {e}", flush=True)
        return False


def _asegurar_encabezados_usuarios():
    """Escribe los títulos de las columnas nuevas (F:I) si faltan. No toca datos."""
    try:
        r = _sheets_get("usuarios_pdfocus!F1:I1")
        if not r.get("values"):
            _sheets_update("usuarios_pdfocus!F1:I1", [[
                "tipo", "carpeta_manifiestos", "carpeta_facturas", "carpeta_otros"
            ]])
    except Exception as e:
        print(f"[SHEETS] No se pudieron escribir encabezados: {e}", flush=True)


def _generate_token(length=10):
    chars = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(chars) for _ in range(length))


def _validate_token(token):
    """Valida si un token existe y está activo. Retorna info del usuario o None."""
    if not token:
        return None
    users = _get_users()
    if not users:
        if _USERS_LOADING.is_set():
            _USERS_LOADING.wait(timeout=5)
        with _USERS_LOCK:
            users = dict(_USERS_CACHE)
    if not users:
        try:
            users = _load_users_from_sheets()
            with _USERS_LOCK:
                _USERS_CACHE.clear()
                _USERS_CACHE.update(users)
                _USERS_LOADED["ts"] = time.time()
        except Exception as e:
            print(f"[TOKEN] Error cargando usuarios directo: {e}", flush=True)
    user = users.get(token)
    if user and user.get("activo"):
        return user
    return None


def _is_authenticated(token):
    """Verifica si el token está autenticado en la sesión Flask actual."""
    return session.get("auth_token") == token


# ── CARPETAS POR PERFIL ───────────────────────────────────────
def _carpetas_legado():
    c = {}
    if FOLDER_ID_MANIFIESTOS:
        c["manifiestos"] = [FOLDER_ID_MANIFIESTOS]
    if FOLDER_ID_FACTURAS:
        c["facturas"] = [FOLDER_ID_FACTURAS]
    return c


def _usa_legado(u):
    return u.get("tipo") == PERFIL_EMPRESA and not any(u.get("carpetas", {}).values())


def _carpetas_propias(u):
    """Carpetas de un perfil EMPRESA o UNIVERSAL: {tipo_doc: [folder_id]}."""
    if u.get("tipo") == PERFIL_GENERAL:
        return {}
    if _usa_legado(u):
        return _carpetas_legado()
    c = u.get("carpetas", {})
    tipos = ("manifiestos",) if u.get("tipo") == PERFIL_UNIVERSAL else TIPOS_DOC
    return {t: [c[t]] for t in tipos if c.get(t)}


def _mapa_carpetas(users=None):
    """
    Todas las carpetas que se indexan: {folder_id: {"tipo": tipo_doc, "duenos": set(nombres)}}.
    Incluye las de empresas inactivas (sus documentos siguen siendo visibles para GENERAL).
    """
    users = users if users is not None else _users_snapshot()
    mapa = {}
    for u in users.values():
        for t, fids in _carpetas_propias(u).items():
            for fid in fids:
                m = mapa.setdefault(fid, {"tipo": t, "duenos": set()})
                m["duenos"].add("UNIVERSAL" if u.get("tipo") == PERFIL_UNIVERSAL else u.get("empresa", ""))
    return mapa


def _carpetas_de_usuario(u, users=None):
    """{tipo_doc: [folder_id, ...]} que puede consultar este perfil."""
    if u.get("tipo") == PERFIL_GENERAL:
        out = {}
        for fid, m in _mapa_carpetas(users).items():
            out.setdefault(m["tipo"], []).append(fid)
        return out
    return _carpetas_propias(u)


def _tipos_de_usuario(u):
    if u.get("tipo") == PERFIL_UNIVERSAL:
        return ["manifiestos"]
    return list(TIPOS_DOC)


def _carpeta_universal(users=None):
    users = users if users is not None else _users_snapshot()
    for u in users.values():
        if u.get("tipo") == PERFIL_UNIVERSAL and u.get("carpetas", {}).get("manifiestos"):
            return u["carpetas"]["manifiestos"]
    return ""


def _id_de_link(texto):
    """Acepta un link de carpeta de Drive o el ID directamente."""
    texto = (texto or "").strip()
    if not texto:
        return ""
    m = re.search(r"/folders/([A-Za-z0-9_-]+)", texto) or re.search(r"[?&]id=([A-Za-z0-9_-]+)", texto)
    if m:
        return m.group(1)
    if re.fullmatch(r"[A-Za-z0-9_-]{10,}", texto):
        return texto
    raise ValueError(f"No reconozco este link o ID de carpeta: {texto[:60]}")


def _crear_carpetas(empresa, tipo, existentes=None):
    """
    Crea en Drive las carpetas que falten para el perfil.
    EMPRESA   → <raíz>/<EMPRESA>/{Manifiestos, Facturas, Otros documentos}
    UNIVERSAL → reutiliza la carpeta universal si ya existe; si no, <raíz>/UNIVERSAL - Manifiestos
    """
    existentes = dict(existentes or {})
    if tipo == PERFIL_GENERAL:
        return {}
    if tipo == PERFIL_UNIVERSAL:
        if not existentes.get("manifiestos"):
            existentes["manifiestos"] = _carpeta_universal()
        if not existentes.get("manifiestos"):
            if not FOLDER_RAIZ_EMPRESAS:
                raise ValueError("Falta configurar FOLDER_RAIZ_EMPRESAS en Render.")
            existentes["manifiestos"] = _drive_crear_carpeta("UNIVERSAL - Manifiestos", FOLDER_RAIZ_EMPRESAS)
        return {"manifiestos": existentes["manifiestos"], "facturas": "", "otros": ""}
    faltan = [t for t in TIPOS_DOC if not existentes.get(t)]
    if faltan:
        if not FOLDER_RAIZ_EMPRESAS:
            raise ValueError("Falta configurar FOLDER_RAIZ_EMPRESAS en Render.")
        padre = _drive_crear_carpeta(empresa, FOLDER_RAIZ_EMPRESAS)
        for t in faltan:
            existentes[t] = _drive_crear_carpeta(NOMBRE_DOC[t], padre)
    return {t: existentes.get(t, "") for t in TIPOS_DOC}


# ── GOOGLE DRIVE — PDFs ───────────────────────────────────────
def list_pdfs_in_folder(folder_id, include_modified_time=False):

    def _get_subfolders(fid):
        subs = []
        pt = None
        while True:
            params = {
                "q": f"'{fid}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false",
                "fields": "nextPageToken, files(id, name)",
                "pageSize": 1000,
            }
            if pt:
                params["pageToken"] = pt
            r = _drive_get("files", params)
            subs.extend([(f["id"], f.get("name", "")) for f in r.get("files", [])])
            pt = r.get("nextPageToken")
            if not pt:
                break
        return subs

    def _get_pdfs(fid):
        pdfs = []
        pt = None
        fields = "nextPageToken, files(id, name, modifiedTime)" if include_modified_time else "nextPageToken, files(id, name)"
        while True:
            params = {
                "q": f"'{fid}' in parents and mimeType='application/pdf' and trashed=false",
                "fields": fields,
                "pageSize": 1000,
            }
            if pt:
                params["pageToken"] = pt
            r = _drive_get("files", params)
            pdfs.extend(r.get("files", []))
            pt = r.get("nextPageToken")
            if not pt:
                break
        return pdfs

    # Cada PDF lleva "ruta": las subcarpetas desde la carpeta principal
    # (ej: "2026/2026-08-27 IMPORTACION YIWU"). Se usa para mostrar la carpeta y el año.
    all_pdfs = []
    queue = [(folder_id, "")]
    visited = set()
    while queue:
        fid, ruta = queue.pop(0)
        if fid in visited:
            continue
        visited.add(fid)
        for f in _get_pdfs(fid):
            f["ruta"] = ruta
            all_pdfs.append(f)
        for sub_id, sub_nombre in _get_subfolders(fid):
            queue.append((sub_id, f"{ruta}/{sub_nombre}" if ruta else sub_nombre))
    return all_pdfs


def download_pdf_bytes(file_id):
    return _drive_get_media(file_id)



# ── NORMALIZACIÓN ─────────────────────────────────────────────
HY_RE = re.compile(r"[-­‐‑‒–—―−﹣－⁃⁻₋‒⁃]")
_SEP  = "\x00"
_BORDER_EXTRAS = set(" &|,;:()[]{}\"'`~^")


def normalize_text(s):
    if not s:
        return ""
    s = HY_RE.sub("-", s)
    s = s.replace("", "").replace("﻿", "").replace("­", "")
    return re.sub(r"\s+", " ", s).strip()


def normalize_compact(s):
    return normalize_text(s).replace(" ", "")


def _is_border_char(ch, ref_norm):
    if ch == _SEP:
        return True
    if ch in _BORDER_EXTRAS:
        return True
    if "-" in ref_norm:
        return not (ch.isalnum() or ch == "-")
    elif ref_norm.isdigit():
        return not ch.isdigit()
    else:
        return not ch.isalnum()


def _boundary_ok(text, idx, fin, ref_norm):
    before = text[idx - 1] if idx > 0 else _SEP
    after  = text[fin]     if fin < len(text) else _SEP
    if after == "+" or before == "+":
        return False
    return _is_border_char(before, ref_norm) and _is_border_char(after, ref_norm)


def ref_in_text_safe(ref_norm, text):
    idx = text.find(ref_norm)
    while idx != -1:
        fin = idx + len(ref_norm)
        if _boundary_ok(text, idx, fin, ref_norm):
            return True
        idx = text.find(ref_norm, idx + 1)
    return False


def generar_variantes(ref):
    r = normalize_text(ref).strip()
    tiene_guion = "-" in r
    if not re.search(r"[\/\-\s]", r):
        return [r]
    partes = re.split(r"[\/\-\s]+", r)
    if len(partes) <= 1:
        return [r]
    A = partes[0]
    B = " ".join(partes[1:]).strip()
    candidatas = [
        f"{A}-{B}", f"{A}/{B}", f"{A} {B}",
        f"{A} - {B}", f"{A} / {B}", f"{A}{B}",
    ]
    resultado = set()
    for v in candidatas:
        c = normalize_compact(v)
        if tiene_guion and "-" not in c and "/" not in c:
            continue
        resultado.add(v)
    return list(resultado)


# ── ORDENAMIENTO POR FECHA ────────────────────────────────────
_MESES_ES = {
    "ene": 1,  "enero": 1,
    "feb": 2,  "febrero": 2,
    "mar": 3,  "marzo": 3,
    "abr": 4,  "abril": 4,
    "may": 5,  "mayo": 5,
    "jun": 6,  "junio": 6,
    "jul": 7,  "julio": 7,
    "ago": 8,  "agosto": 8,
    "sep": 9,  "sept": 9, "septiembre": 9,
    "oct": 10, "octubre": 10,
    "nov": 11, "noviembre": 11,
    "dic": 12, "diciembre": 12,
}


def _fecha_clave(nombre):
    n = nombre.upper()
    m = re.search(r"(\d{4})[_\-](\d{2})[_\-](\d{2})", n)
    if m:
        return (int(m.group(1)), int(m.group(2)), int(m.group(3)))
    mes_patron = "|".join(sorted(_MESES_ES.keys(), key=len, reverse=True)).upper()
    m = re.search(
        r"(?:(\d{1,2})\s+)?(" + mes_patron + r")(?:\s+(\d{1,2}))?\s+(\d{4})",
        n
    )
    if m:
        dia1, mes_str, dia2, anio = m.group(1), m.group(2), m.group(3), m.group(4)
        dia = int(dia1 or dia2 or 1)
        mes = _MESES_ES.get(mes_str.lower(), 0)
        return (int(anio), mes, dia)
    m = re.search(r"(\d{4})", n)
    if m:
        return (int(m.group(1)), 0, 0)
    return (0, 0, 0)


# ── BÚSQUEDA Y ANOTACIÓN ──────────────────────────────────────
def find_reference_multi_from_words(words, ref_norm):
    if not words:
        return []
    parts, index_map, cursor = [], [], 0
    for w in words:
        raw = normalize_text(w[4]).replace(" ", "")
        if not raw:
            continue
        if parts:
            index_map.append((cursor, cursor + 1, None))
            parts.append(_SEP)
            cursor += 1
        index_map.append((cursor, cursor + len(raw), fitz.Rect(w[0], w[1], w[2], w[3])))
        parts.append(raw)
        cursor += len(raw)
    full_text = "".join(parts)
    rects = []
    idx = full_text.find(ref_norm)
    while idx != -1:
        fin = idx + len(ref_norm)
        if _boundary_ok(full_text, idx, fin, ref_norm):
            rs = [r for s, e, r in index_map if r is not None and e > idx and s < fin]
            if rs:
                R = rs[0]
                for rr in rs[1:]:
                    R |= rr
                rects.append(R)
        idx = full_text.find(ref_norm, idx + 1)
    return rects


def find_reference_multi_nojoin(words, ref_norm):
    if not words:
        return []
    SAFE = " \n\r\t.,:;()[]{}-/*"
    parts, index_map, cursor = [], [], 0
    for w in words:
        raw = normalize_text(w[4]).replace(" ", "")
        if not raw:
            continue
        index_map.append((cursor, cursor + len(raw), fitz.Rect(w[0], w[1], w[2], w[3])))
        parts.append(raw)
        cursor += len(raw)
    full_text = "".join(parts)
    rects = []
    idx = full_text.find(ref_norm)
    while idx != -1:
        fin = idx + len(ref_norm)
        before = full_text[idx - 1] if idx > 0 else " "
        after  = full_text[fin]     if fin < len(full_text) else " "
        if (before in SAFE or not before.isalnum()) and (after in SAFE or not after.isalnum()):
            rs = [r for s, e, r in index_map if r is not None and e > idx and s < fin]
            if rs:
                R = rs[0]
                for rr in rs[1:]:
                    R |= rr
                rects.append(R)
        idx = full_text.find(ref_norm, idx + 1)
    return rects


def find_reference_manifesto(words, ref_norm):
    r = find_reference_multi_from_words(words, ref_norm)
    return r if r else find_reference_multi_nojoin(words, ref_norm)


def find_reference_facturas(words, ref_norm):
    if not words:
        return []
    return [
        fitz.Rect(w[0], w[1], w[2], w[3])
        for w in words
        if normalize_text(w[4]).replace(" ", "") == ref_norm
    ]


def find_reference_isiigo(words, ref_norm):
    if not words:
        return []
    rects = []
    for w in words:
        pc = normalize_text(w[4]).replace(" ", "")
        idx = pc.find(ref_norm)
        while idx != -1:
            fin = idx + len(ref_norm)
            if _boundary_ok(pc, idx, fin, ref_norm):
                rects.append(fitz.Rect(w[0], w[1], w[2], w[3]))
                break
            idx = pc.find(ref_norm, idx + 1)
    return rects


def annotate_pdf_bytes(pdf_bytes, ref_raw, mode="manifesto"):
    ref_norm = normalize_compact(ref_raw)
    vcs = list(dict.fromkeys(
        [normalize_compact(v) for v in generar_variantes(ref_raw)] + [ref_norm]
    ))
    color = (0.20, 0.90, 0.20) if mode == "manifesto" else (0.05, 0.45, 0.80)

    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    pages = [doc.load_page(p) for p in range(len(doc))]
    pwc = {}
    for p, pg in enumerate(pages):
        try:
            pwc[p] = pg.get_text("words")
        except Exception:
            pwc[p] = []

    found = False
    for var in vcs:
        if found:
            break
        for p, words in pwc.items():
            if mode == "manifesto":
                rects = find_reference_manifesto(words, var) or find_reference_manifesto(words, var + ".")
            else:
                rects = (
                    find_reference_facturas(words, var)
                    or find_reference_facturas(words, var + ".")
                    or find_reference_isiigo(words, var)
                    or find_reference_isiigo(words, var + ".")
                )
            for R in rects:
                R2 = fitz.Rect(R.x0 - 1, R.y0 - 2, R.x1 + 1, R.y1 + 2)
                pages[p].draw_rect(R2, color=color, fill=color, fill_opacity=0.40, width=0, overlay=True)
                try:
                    annot = pages[p].add_highlight_annot(R2)
                    annot.set_colors(stroke=color)
                    annot.set_opacity(0.5)
                    annot.update()
                except Exception:
                    pass
                found = True

    out = io.BytesIO()
    doc.save(out, garbage=1)
    doc.close()
    out.seek(0)
    return out.read(), found


# ── FECHA Y AÑO PARA ORDENAR Y FILTRAR (no afecta la búsqueda) ─
_RE_ANIO = re.compile(r"(?<!\d)(19[89]\d|20\d{2})(?!\d)")


def _fecha_valida(texto):
    """Fecha del nombre, ignorando números que no son un año real (ej: "INVOICE 8812")."""
    k = _fecha_clave(texto or "")
    return k if 1990 <= k[0] <= 2100 else (0, 0, 0)


def _fecha_carpeta(ruta):
    if not ruta:
        return (0, 0, 0)
    k = _fecha_valida(ruta.split("/")[-1])
    return k if k != (0, 0, 0) else _fecha_valida(ruta)


def _anio_doc(ruta, nombre):
    for t in (ruta, nombre):
        m = _RE_ANIO.search(t or "")
        if m:
            return m.group(1)
    return ""


# ── ÍNDICE (por carpeta de Drive) ─────────────────────────────
# build_index / update_index_incremental reciben (folder_id, clave); la clave del
# índice ES el folder_id, así cada carpeta se indexa una sola vez aunque la usen
# varios perfiles.
def build_compact_from_bytes(pdf_bytes):
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    except Exception:
        return ""
    tokens = []
    for p in range(len(doc)):
        try:
            for w in doc.load_page(p).get_text("words"):
                tok = normalize_text(w[4]).replace(" ", "")
                if tok:
                    tokens.append(tok)
        except Exception:
            pass
    doc.close()
    return _SEP.join(tokens)


def build_index(folder_id, section_key):
    with _INDEX_LOCK:
        if _INDEX_BUILT.get(section_key):
            return
    print(f"[INDEX] Construyendo índice para {section_key}...", flush=True)
    pdfs = list_pdfs_in_folder(folder_id, include_modified_time=True)
    print(f"[INDEX] {section_key}: {len(pdfs)} PDFs encontrados.", flush=True)
    new_entries = {}
    for i, f in enumerate(pdfs):
        try:
            data = download_pdf_bytes(f["id"])
            compact = build_compact_from_bytes(data)
            new_entries[f["id"]] = {
                "name": f["name"],
                "compact": _compress_compact(compact),
                "modified_time": f.get("modifiedTime", ""),
                "ruta": f.get("ruta", ""),
            }
            print(f"  ✔ Indexado: {f['name']}", flush=True)
        except Exception as e:
            print(f"  ✖ Error indexando {f['name']}: {e}", flush=True)
        # Guardar progreso parcial en Sheets cada 50 PDFs
        if (i + 1) % 50 == 0:
            with _INDEX_LOCK:
                _INDEX_CACHE[section_key] = dict(new_entries)
            print(f"[INDEX] Guardando progreso parcial: {i+1}/{len(pdfs)}...", flush=True)
            _save_index_to_disk()
            _save_index_to_drive()
    with _INDEX_LOCK:
        _INDEX_CACHE[section_key] = new_entries
        _INDEX_BUILT[section_key] = True
    print(f"[INDEX] {section_key}: {len(new_entries)} PDFs indexados. LISTO.", flush=True)
    _save_index_to_disk()
    _save_index_to_drive()


def update_index_incremental(folder_id, section_key):
    print(f"[UPDATE] Revisando cambios en {section_key}...", flush=True)
    try:
        pdfs = list_pdfs_in_folder(folder_id, include_modified_time=True)
    except Exception as e:
        print(f"[UPDATE] Error listando {section_key}: {e}", flush=True)
        return

    with _INDEX_LOCK:
        current = dict(_INDEX_CACHE.get(section_key, {}))

    drive_ids = {f["id"] for f in pdfs}
    removed = [fid for fid in current if fid not in drive_ids]
    for fid in removed:
        print(f"  ✖ Eliminado del índice: {current[fid]['name']}", flush=True)
        del current[fid]

    nuevos = 0
    rutas = 0
    for f in pdfs:
        fid = f["id"]
        mod = f.get("modifiedTime", "")
        if fid in current and current[fid].get("modified_time") == mod:
            # Sin cambios en el PDF: solo se completa/actualiza la ruta (sin descargar nada)
            if current[fid].get("ruta") != f.get("ruta", ""):
                current[fid] = dict(current[fid], ruta=f.get("ruta", ""))
                rutas += 1
            continue
        try:
            data = download_pdf_bytes(fid)
            compact = build_compact_from_bytes(data)
            current[fid] = {
                "name": f["name"],
                "compact": _compress_compact(compact),
                "modified_time": mod,
                "ruta": f.get("ruta", ""),
            }
            print(f"  ✔ Actualizado: {f['name']}", flush=True)
            nuevos += 1
            # Guardar progreso en Sheets cada 50 PDFs nuevos
            if nuevos % 50 == 0:
                with _INDEX_LOCK:
                    _INDEX_CACHE[section_key] = dict(current)
                    _INDEX_BUILT[section_key] = True
                print(f"[UPDATE] Guardando progreso parcial: {nuevos} nuevos...", flush=True)
                _save_index_to_disk()
                _save_index_to_drive()
        except Exception as e:
            print(f"  ✖ Error actualizando {f['name']}: {e}", flush=True)

    with _INDEX_LOCK:
        _INDEX_CACHE[section_key] = current
        _INDEX_BUILT[section_key] = True

    if nuevos > 0 or removed or rutas:
        _save_index_to_disk()
        _save_index_to_drive()

    print(f"[UPDATE] {section_key}: {nuevos} nuevos/modificados, {len(removed)} eliminados. Total: {len(current)}.", flush=True)


def ensure_index(section_key, folder_id):
    if not _INDEX_BUILT.get(section_key):
        build_index(folder_id, section_key)



def _indexar_carpetas_en_background(fids, reconstruir=False):
    """
    Actualiza (o reconstruye) carpetas en segundo plano. Devuelve False si no se pudo.

    Protección: una carpeta cuyo índice todavía NO está cargado en memoria y que el
    arranque aún no ha procesado no se toca, porque la actualización la creería vacía y
    la volvería a indexar completa. Las carpetas ya cargadas se actualizan siempre.
    """
    if not reconstruir:
        with _INDEX_LOCK:
            cargadas = [f for f in fids if _INDEX_BUILT.get(f)]
        pendientes = [f for f in fids if f not in cargadas]
        if pendientes and not _STARTUP_DONE.is_set():
            print(f"[INDEX] {len(pendientes)} carpeta(s) aún cargando; quedan para la revisión automática.", flush=True)
            fids = cargadas
        if not fids:
            return False
    def _run():
        for fid in fids:
            try:
                if reconstruir:
                    with _INDEX_LOCK:
                        _INDEX_BUILT.pop(fid, None)
                        _INDEX_CACHE.pop(fid, None)
                    build_index(fid, fid)
                else:
                    update_index_incremental(fid, fid)
            except Exception as e:
                print(f"[INDEX] Error indexando carpeta {fid}: {e}", flush=True)
    threading.Thread(target=_run, daemon=True).start()
    return True


def _resumen_indice(mapa=None):
    """Totales por tipo de documento para el panel admin."""
    mapa = mapa if mapa is not None else _mapa_carpetas()
    info = {t: {"listo": True, "pdfs": 0} for t in TIPOS_DOC}
    with _INDEX_LOCK:
        for fid, m in mapa.items():
            t = m["tipo"]
            info[t]["pdfs"] += len(_INDEX_CACHE.get(fid, {}))
            if not _INDEX_BUILT.get(fid):
                info[t]["listo"] = False
    return info


# ── DECORADOR DE AUTENTICACIÓN ────────────────────────────────
def require_token(f):
    """Decorador: requiere token válido + contraseña verificada en sesión."""
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.args.get("token") or request.cookies.get("pdfocus_token")
        user_info = _validate_token(token)
        if not user_info:
            return redirect(url_for("login_page"))

        if user_info.get("password_hash"):
            if not _is_authenticated(token):
                session["pending_token"] = token
                return redirect(url_for("password_page"))

        empresa = user_info["empresa"]
        return f(*args, empresa=empresa, token=token, user=user_info, **kwargs)
    return decorated


# ── RUTAS PÚBLICAS ────────────────────────────────────────────
@app.route("/login")
def login_page():
    return render_template("login.html")


@app.route("/password")
def password_page():
    """Página de ingreso de contraseña (cuando la empresa tiene contraseña configurada)."""
    token = session.get("pending_token") or request.args.get("token") or request.cookies.get("pdfocus_token")
    user_info = _validate_token(token)
    if not user_info:
        return redirect(url_for("login_page"))
    # Si no tiene contraseña configurada aún, dar acceso directo
    if not user_info.get("password_hash"):
        session["auth_token"] = token
        resp = make_response(redirect(url_for("index") + f"?token={token}"))
        resp.set_cookie("pdfocus_token", token, max_age=60 * 60 * 24 * 30)
        return resp
    return render_template("password.html", empresa=user_info["empresa"], token=token)


@app.route("/verificar_password", methods=["POST"])
def verificar_password():
    """Verifica la contraseña ingresada por el usuario."""
    token    = request.form.get("token", "").strip()
    password = request.form.get("password", "").strip()

    user_info = _validate_token(token)
    if not user_info:
        return redirect(url_for("login_page"))

    password_hash = user_info.get("password_hash", "")

    if not password_hash:
        # Sin contraseña configurada — acceso directo
        session["auth_token"] = token
        resp = make_response(redirect(url_for("index") + f"?token={token}"))
        resp.set_cookie("pdfocus_token", token, max_age=60 * 60 * 24 * 30)
        return resp

    if check_password_hash(password_hash, password):
        session["auth_token"] = token
        session.permanent = True
        resp = make_response(redirect(url_for("index") + f"?token={token}"))
        resp.set_cookie("pdfocus_token", token, max_age=60 * 60 * 24 * 30)
        return resp
    else:
        return render_template("password.html",
                               empresa=user_info["empresa"],
                               token=token,
                               error="Contraseña incorrecta. Intenta de nuevo.")


@app.route("/acceso")
def acceso():
    """Valida el token de la URL y redirige al flujo correcto."""
    token = request.args.get("token", "").strip()
    user_info = _validate_token(token)
    if not user_info:
        return render_template("login.html", error="Token inválido o inactivo.")

    resp = make_response(redirect(url_for("index") + f"?token={token}"))
    resp.set_cookie("pdfocus_token", token, max_age=60 * 60 * 24 * 30)
    return resp


@app.route("/status")
def status():
    return jsonify(_resumen_indice())


# ── RUTAS PROTEGIDAS ──────────────────────────────────────────
@app.route("/")
@require_token
def index(empresa=None, token=None, user=None):
    tipos = _tipos_de_usuario(user)
    resp = make_response(render_template(
        "index.html", empresa=empresa, token=token,
        perfil=user.get("tipo", PERFIL_EMPRESA), tipos=tipos,
    ))
    resp.set_cookie("pdfocus_token", token, max_age=60 * 60 * 24 * 30)
    return resp


@app.route("/buscar", methods=["POST"])
@require_token
def buscar(empresa=None, token=None, user=None):
    data   = request.get_json() or {}
    codigo = (data.get("codigo") or "").strip()
    tipo   = (data.get("tipo") or "ambos").lower()

    if not codigo:
        return jsonify({"error": "Escribe un código para buscar."}), 400

    ref_norm = normalize_compact(codigo)
    resultados = []
    vistos = set()

    users = _users_snapshot()
    carpetas = _carpetas_de_usuario(user, users)
    es_general = user.get("tipo") == PERFIL_GENERAL
    duenos = {fid: m["duenos"] for fid, m in _mapa_carpetas(users).items()} if es_general else {}

    permitidos = _tipos_de_usuario(user)
    tipos = permitidos if tipo in ("ambos", "todos") else [t for t in permitidos if t == tipo]

    for t in tipos:
        for folder_id in carpetas.get(t, []):
            ensure_index(folder_id, folder_id)
            with _INDEX_LOCK:
                index_data = dict(_INDEX_CACHE.get(folder_id, {}))
            for file_id, info in index_data.items():
                if file_id in vistos:
                    continue
                compact = info["compact"]
                if isinstance(compact, bytes):
                    compact = _decompress_compact(compact)
                if ref_in_text_safe(ref_norm, compact):
                    vistos.add(file_id)
                    ruta = info.get("ruta", "")
                    f_doc = _fecha_valida(info["name"])
                    f_carp = _fecha_carpeta(ruta)
                    item = {
                        "nombre":   info["name"],
                        "tipo":     "otros" if t == "otros" else MODO_DOC[t],
                        "drive_id": file_id,
                        "ruta":     ruta,
                        "anio":     _anio_doc(ruta, info["name"]),
                        "fecha":    list(f_doc if f_doc != (0, 0, 0) else f_carp),
                        "fecha_carpeta": list(f_carp),
                    }
                    if es_general:
                        item["empresa"] = " / ".join(sorted(duenos.get(folder_id, []))) or ""
                    resultados.append(item)

    if not resultados:
        return jsonify({"resultados": [], "mensaje": "No se encontró la referencia en ningún PDF."})

    resultados.sort(key=lambda x: tuple(x["fecha"]), reverse=True)
    return jsonify({"resultados": resultados})


def _carpeta_del_archivo(user, drive_id):
    """Devuelve (folder_id, tipo_doc) si el archivo está en una carpeta permitida; si no, None."""
    for t, fids in _carpetas_de_usuario(user).items():
        for fid in fids:
            with _INDEX_LOCK:
                if drive_id in _INDEX_CACHE.get(fid, {}):
                    return fid, t
    return None


@app.route("/ver_pdf")
@require_token
def ver_pdf(empresa=None, token=None, user=None):
    drive_id = request.args.get("drive_id", "")
    codigo   = request.args.get("codigo", "").strip()

    if not drive_id or not codigo:
        return "Parámetros faltantes.", 400

    ubicacion = _carpeta_del_archivo(user, drive_id)
    if not ubicacion:
        return "No tienes acceso a este documento.", 403
    _fid, tipo_doc = ubicacion

    try:
        pdf_bytes = download_pdf_bytes(drive_id)
    except Exception as e:
        return f"No se pudo descargar el PDF: {e}", 500

    try:
        marked_bytes, found = annotate_pdf_bytes(pdf_bytes, codigo, mode=MODO_DOC[tipo_doc])
    except Exception:
        marked_bytes = pdf_bytes

    return send_file(
        io.BytesIO(marked_bytes),
        mimetype="application/pdf",
        as_attachment=False,
        download_name=f"PDFocus_{codigo}.pdf",
    )


# ── PANEL DE ADMINISTRACIÓN ───────────────────────────────────
def _check_admin():
    token = request.headers.get("X-Admin-Token") or request.args.get("admin_token") or request.cookies.get("pdfocus_admin")
    return token == ADMIN_TOKEN


@app.route("/admin")
def admin_panel():
    if not _check_admin():
        return render_template("admin_login.html")
    users = _users_snapshot()
    mapa = _mapa_carpetas(users)
    index_info = _resumen_indice(mapa)

    # Datos por empresa para la tabla
    filas = {}
    with _INDEX_LOCK:
        for token, u in users.items():
            conteo = {}
            for t, fids in _carpetas_de_usuario(u, users).items():
                conteo[t] = sum(len(_INDEX_CACHE.get(fid, {})) for fid in fids)
            filas[token] = dict(u, conteo=conteo, legado=_usa_legado(u))
    orden = {PERFIL_GENERAL: 0, PERFIL_UNIVERSAL: 1, PERFIL_EMPRESA: 2}
    filas = dict(sorted(filas.items(), key=lambda kv: (orden.get(kv[1]["tipo"], 9),
                                                        not kv[1]["activo"], kv[1]["empresa"].lower())))
    return render_template(
        "admin.html", users=filas, index_info=index_info,
        raiz_ok=bool(FOLDER_RAIZ_EMPRESAS), carpeta_universal=_carpeta_universal(users),
        legado={"manifiestos": FOLDER_ID_MANIFIESTOS, "facturas": FOLDER_ID_FACTURAS},
        base_url=BASE_URL,
    )


@app.route("/admin/index_status")
def admin_index_status():
    if not _check_admin():
        return jsonify({"error": "No autorizado"}), 401
    return jsonify(_resumen_indice())


@app.route("/admin/login", methods=["POST"])
def admin_login():
    token = request.form.get("admin_token", "").strip()
    if token != ADMIN_TOKEN:
        return render_template("admin_login.html", error="Token incorrecto.")
    resp = make_response(redirect(url_for("admin_panel")))
    resp.set_cookie("pdfocus_admin", token, max_age=60 * 60 * 8)
    return resp


@app.route("/admin/logout")
def admin_logout():
    resp = make_response(redirect(url_for("admin_panel")))
    resp.delete_cookie("pdfocus_admin")
    return resp


@app.route("/admin/agregar_empresa", methods=["POST"])
def admin_agregar_empresa():
    if not _check_admin():
        return jsonify({"error": "No autorizado"}), 401
    data = request.get_json(silent=True) or {}
    empresa = str(data.get("empresa") or request.form.get("empresa") or "").strip()
    tipo = str(data.get("tipo") or PERFIL_EMPRESA).upper()
    if tipo not in PERFILES:
        tipo = PERFIL_EMPRESA
    if not empresa:
        return jsonify({"error": "Nombre de empresa requerido"}), 400

    try:
        carpetas = _crear_carpetas(empresa, tipo)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        print(f"[ADMIN] Error creando carpetas: {e}", flush=True)
        return jsonify({"error": f"No se pudieron crear las carpetas en Drive: {e}"}), 500

    token = _generate_token()
    ok = _add_user_to_sheets(empresa, token, tipo, carpetas)
    if ok:
        fids = [fid for fid in carpetas.values() if fid]
        if fids:
            _indexar_carpetas_en_background(fids)
        url = f"{BASE_URL}/?token={token}"
        return jsonify({"ok": True, "empresa": empresa, "token": token, "url": url, "tipo": tipo})
    return jsonify({"error": "No se pudo guardar en Sheets"}), 500


@app.route("/admin/carpetas", methods=["POST"])
def admin_carpetas():
    """Asigna carpetas a un perfil (link o ID) y crea automáticamente las que queden vacías."""
    if not _check_admin():
        return jsonify({"error": "No autorizado"}), 401
    data  = request.get_json() or {}
    token = data.get("token", "").strip()
    users = _users_snapshot()
    u = users.get(token)
    if not u:
        return jsonify({"error": "Empresa no encontrada"}), 404
    if u["tipo"] == PERFIL_GENERAL:
        return jsonify({"error": "El perfil General no tiene carpetas propias: ve las de todos."}), 400
    try:
        nuevas = {t: _id_de_link(data.get(t, "")) for t in TIPOS_DOC}
        if u["tipo"] == PERFIL_UNIVERSAL:
            nuevas["facturas"] = nuevas["otros"] = ""
        if data.get("crear_faltantes"):
            nuevas = _crear_carpetas(u["empresa"], u["tipo"], nuevas)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": f"No se pudieron crear las carpetas en Drive: {e}"}), 500

    if not _set_user_carpetas(token, nuevas):
        return jsonify({"error": "No se pudo guardar en Sheets"}), 500
    fids = [fid for fid in nuevas.values() if fid]
    if fids:
        _indexar_carpetas_en_background(fids)
    return jsonify({"ok": True, "carpetas": nuevas})


@app.route("/admin/actualizar_empresa", methods=["POST"])
def admin_actualizar_empresa():
    """Trae solo los PDFs nuevos/modificados de las carpetas de un perfil (rápido)."""
    if not _check_admin():
        return jsonify({"error": "No autorizado"}), 401
    token = (request.get_json() or {}).get("token", "").strip()
    users = _users_snapshot()
    u = users.get(token)
    if not u:
        return jsonify({"error": "Empresa no encontrada"}), 404
    fids = [fid for lst in _carpetas_de_usuario(u, users).values() for fid in lst]
    if not fids:
        return jsonify({"error": "Esta empresa no tiene carpetas"}), 400
    if not _indexar_carpetas_en_background(fids):
        return jsonify({"error": "El índice todavía se está cargando. Espera a que diga 'Listo' e intenta de nuevo."}), 409
    return jsonify({"ok": True, "mensaje": "Buscando documentos nuevos…"})


@app.route("/admin/rebuild_empresa", methods=["POST"])
def admin_rebuild_empresa():
    """Reindexa solo las carpetas de un perfil."""
    if not _check_admin():
        return jsonify({"error": "No autorizado"}), 401
    token = (request.get_json() or {}).get("token", "").strip()
    users = _users_snapshot()
    u = users.get(token)
    if not u:
        return jsonify({"error": "Empresa no encontrada"}), 404
    fids = [fid for lst in _carpetas_de_usuario(u, users).values() for fid in lst]
    if not fids:
        return jsonify({"error": "Esta empresa no tiene carpetas"}), 400

    def _run():
        for fid in fids:
            with _INDEX_LOCK:
                _INDEX_BUILT.pop(fid, None)
                _INDEX_CACHE.pop(fid, None)
            try:
                build_index(fid, fid)
            except Exception as e:
                print(f"[INDEX] Error reindexando {fid}: {e}", flush=True)
    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"ok": True, "mensaje": f"Reindexando {len(fids)} carpeta(s)."})


@app.route("/admin/set_password", methods=["POST"])
def admin_set_password():
    """Establece o cambia la contraseña de una empresa."""
    if not _check_admin():
        return jsonify({"error": "No autorizado"}), 401
    data     = request.get_json() or {}
    token    = data.get("token", "").strip()
    password = data.get("password", "").strip()
    if not token or not password:
        return jsonify({"error": "Token y contraseña requeridos"}), 400
    if len(password) < 4:
        return jsonify({"error": "La contraseña debe tener al menos 4 caracteres"}), 400
    ok = _set_user_password(token, password)
    return jsonify({"ok": ok})


@app.route("/admin/toggle_empresa", methods=["POST"])
def admin_toggle_empresa():
    if not _check_admin():
        return jsonify({"error": "No autorizado"}), 401
    data   = request.get_json() or {}
    token  = data.get("token", "").strip()
    activo = data.get("activo", True)
    ok = _set_user_status(token, activo)
    return jsonify({"ok": ok})


@app.route("/admin/eliminar_empresa", methods=["POST"])
def admin_eliminar_empresa():
    if not _check_admin():
        return jsonify({"error": "No autorizado"}), 401
    data  = request.get_json() or {}
    token = data.get("token", "").strip()
    ok = _delete_user_from_sheets(token)
    return jsonify({"ok": ok})


@app.route("/admin/rebuild_index", methods=["POST"])
def admin_rebuild_index():
    if not _check_admin():
        return jsonify({"error": "No autorizado"}), 401
    with _INDEX_LOCK:
        _INDEX_BUILT.clear()
        _INDEX_CACHE.clear()
    try:
        if os.path.exists(INDEX_CACHE_PATH):
            os.remove(INDEX_CACHE_PATH)
    except Exception:
        pass

    def _delete_drive_cache_and_rebuild():
        try:
            _sheets_clear(f"{CACHE_SHEET_NAME}!A:B")
            print("[SHEETS-CACHE] Caché en Sheets eliminado.", flush=True)
        except Exception as e:
            print(f"[SHEETS-CACHE] Error eliminando caché: {e}", flush=True)
        _rebuild_all()

    threading.Thread(target=_delete_drive_cache_and_rebuild, daemon=True).start()
    return jsonify({"ok": True, "mensaje": "Reconstrucción iniciada."})


# ── RUTAS DE QR ───────────────────────────────────────────────
@app.route("/admin/qr/<token_cliente>")
def admin_qr_imagen(token_cliente):
    """Devuelve el QR del enlace de acceso del cliente como imagen PNG."""
    if not _check_admin():
        return jsonify({"error": "No autorizado"}), 401
    try:
        import qrcode
    except ImportError:
        return "Librería qrcode no instalada. Agrega 'qrcode[pil]' a requirements.txt.", 500

    url_cliente = f"{BASE_URL}/?token={token_cliente}"
    qr = qrcode.QRCode(
        version=None,
        error_correction=qrcode.constants.ERROR_CORRECT_H,
        box_size=10,
        border=4,
    )
    qr.add_data(url_cliente)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return send_file(buf, mimetype="image/png", download_name=f"qr_{token_cliente}.png")


@app.route("/admin/qr_page/<token_cliente>")
def admin_qr_page(token_cliente):
    """Página HTML con QR + info del cliente, lista para imprimir o compartir."""
    if not _check_admin():
        return redirect(url_for("admin_panel"))

    users = _get_users()
    user_info = users.get(token_cliente, {})
    empresa = user_info.get("empresa", token_cliente)
    url_cliente = f"{BASE_URL}/?token={token_cliente}"

    # Generar QR como base64 para embeber en HTML
    try:
        import qrcode
        qr = qrcode.QRCode(
            version=None,
            error_correction=qrcode.constants.ERROR_CORRECT_H,
            box_size=12,
            border=4,
        )
        qr.add_data(url_cliente)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        qr_b64 = base64.b64encode(buf.getvalue()).decode("ascii")
        qr_src = f"data:image/png;base64,{qr_b64}"
        qr_ok = True
    except Exception as e:
        qr_src = ""
        qr_ok = False
        qr_error = str(e)

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>QR — {empresa}</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    background: #f0f4f8;
    min-height: 100vh;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 24px;
  }}
  .card {{
    background: white;
    border-radius: 16px;
    box-shadow: 0 4px 24px rgba(0,0,0,0.10);
    padding: 40px 32px 32px;
    max-width: 420px;
    width: 100%;
    text-align: center;
  }}
  .logo {{
    font-size: 13px;
    font-weight: 700;
    letter-spacing: 3px;
    color: #94a3b8;
    text-transform: uppercase;
    margin-bottom: 8px;
  }}
  h1 {{
    font-size: 22px;
    font-weight: 700;
    color: #1e293b;
    margin-bottom: 28px;
  }}
  .qr-wrap {{
    background: white;
    border: 2px solid #e2e8f0;
    border-radius: 12px;
    display: inline-block;
    padding: 12px;
    margin-bottom: 24px;
  }}
  .qr-wrap img {{ display: block; width: 220px; height: 220px; }}
  .token-box {{
    background: #f8fafc;
    border: 1px solid #e2e8f0;
    border-radius: 8px;
    padding: 12px 16px;
    margin-bottom: 12px;
  }}
  .token-label {{
    font-size: 11px;
    color: #94a3b8;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 1px;
    margin-bottom: 4px;
  }}
  .token-value {{
    font-family: 'Courier New', monospace;
    font-size: 16px;
    font-weight: 700;
    color: #0f172a;
    letter-spacing: 2px;
  }}
  .url-box {{
    background: #f8fafc;
    border: 1px solid #e2e8f0;
    border-radius: 8px;
    padding: 10px 16px;
    margin-bottom: 24px;
    word-break: break-all;
    font-size: 12px;
    color: #475569;
  }}
  .btns {{
    display: flex;
    gap: 10px;
    flex-wrap: wrap;
    justify-content: center;
  }}
  .btn {{
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 10px 18px;
    border-radius: 8px;
    font-size: 14px;
    font-weight: 600;
    cursor: pointer;
    text-decoration: none;
    border: none;
    transition: opacity .15s;
  }}
  .btn:hover {{ opacity: .85; }}
  .btn-primary {{ background: #3b82f6; color: white; }}
  .btn-outline {{ background: white; color: #374151; border: 1.5px solid #d1d5db; }}
  .btn-green {{ background: #22c55e; color: white; }}
  .back {{ margin-top: 20px; }}
  .back a {{ font-size: 13px; color: #94a3b8; text-decoration: none; }}
  .back a:hover {{ color: #3b82f6; }}
  @media print {{
    body {{ background: white; }}
    .card {{ box-shadow: none; }}
    .btns, .back {{ display: none; }}
  }}
</style>
</head>
<body>
<div class="card">
  <div class="logo">PDFocus</div>
  <h1>{empresa}</h1>

  {"<div class='qr-wrap'><img src='" + qr_src + "' alt='QR Code'></div>" if qr_ok else f"<p style='color:red'>Error generando QR: {qr_error}</p>"}

  <div class="token-box">
    <div class="token-label">Token de acceso</div>
    <div class="token-value">{token_cliente}</div>
  </div>

  <div class="url-box">{url_cliente}</div>

  <div class="btns">
    <a href="{url_cliente}" target="_blank" class="btn btn-primary">
      🔗 Abrir enlace
    </a>
    <a href="/admin/qr/{token_cliente}" download="qr_{empresa}.png" class="btn btn-green">
      ⬇ Descargar QR
    </a>
    <button onclick="window.print()" class="btn btn-outline">
      🖨 Imprimir
    </button>
  </div>

  <div class="back">
    <a href="/admin">← Volver al panel</a>
  </div>
</div>
</body>
</html>"""

    return html


def _rebuild_all():
    for fid in _mapa_carpetas():
        try:
            build_index(fid, fid)
        except Exception as e:
            print(f"[INDEX] Error construyendo {fid}: {e}", flush=True)


def _update_all(etiqueta):
    for fid in _mapa_carpetas():
        try:
            update_index_incremental(fid, fid)
        except Exception as e:
            print(f"[{etiqueta}] Error actualizando {fid}: {e}", flush=True)


# ── ACTUALIZACIÓN INCREMENTAL CADA 15 MINUTOS ────────────────
def _hourly_update_loop():
    _STARTUP_DONE.wait()
    while True:
        time.sleep(_UPDATE_INTERVAL)
        print("[HOURLY] Iniciando actualización incremental...", flush=True)
        _reload_users_sync()
        _update_all("HOURLY")
        print("[HOURLY] Actualización incremental completada.", flush=True)


# ── ARRANQUE DEL ÍNDICE ──────────────────────────────────────
def _start_index_background():
    def _build():
        try:
            print("[STARTUP] Iniciando construcción del índice...", flush=True)
            if not SERVICE_ACCOUNT_JSON:
                print("[STARTUP] ERROR: GOOGLE_SERVICE_ACCOUNT está vacío.", flush=True)
                return
            # Las carpetas a indexar dependen de los usuarios: se cargan primero
            if _USERS_LOADING.is_set():
                _USERS_LOADING.wait(timeout=30)
            if not _USERS_CACHE:
                _reload_users_sync()
            _asegurar_encabezados_usuarios()

            # 1. Caché local /tmp (reinicios rápidos del mismo worker)
            if _load_index_from_disk():
                print("[STARTUP] ✅ Índice desde /tmp. Corriendo incremental de arranque...", flush=True)
                _update_all("STARTUP")
                return

            # 2. Caché en Sheets (sobrevive deploys y reinicios de worker)
            if _load_index_from_drive():
                total = sum(len(v) for v in _INDEX_CACHE.values())
                print(f"[STARTUP] ✅ Índice desde Sheets ({total} PDFs). Corriendo incremental de arranque...", flush=True)
                _update_all("STARTUP")
                return

            # 3. Construir desde cero
            print("[STARTUP] Sin caché. Construyendo desde cero...", flush=True)
            _rebuild_all()
            print("[STARTUP] Índice listo.", flush=True)
        finally:
            _STARTUP_DONE.set()

    threading.Thread(target=_build, daemon=True).start()
    threading.Thread(target=_hourly_update_loop, daemon=True).start()


# Precargar usuarios en background al arrancar. Se marca el Event ANTES de lanzar
# el thread para que admin_panel, _validate_token y el índice esperen correctamente.
_USERS_LOADING.set()
def _initial_users_load():
    try:
        _reload_users_sync()
    finally:
        _USERS_LOADING.clear()
threading.Thread(target=_initial_users_load, daemon=True).start()
_start_index_background()

if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

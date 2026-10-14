"""
PDFocus Web — Felipe Ceballos
Busca referencias en PDFs almacenados en Google Drive y los devuelve marcados.
Sistema de login por token + contraseña por empresa, almacenado en Google Sheets.
Cache en Google Drive: el índice se guarda en Drive para sobrevivir deploys.

Estructura Google Sheets (usuarios_pdfocus):
  A: empresa | B: token | C: activo (SI/NO) | D: fecha_creacion | E: password_hash
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
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "pdfocus_secret_2026_xK9mN3pQ")

# ── CONFIG ────────────────────────────────────────────────────
FOLDER_ID_MANIFIESTOS = os.environ.get("FOLDER_MANIFIESTOS", "")
FOLDER_ID_FACTURAS    = os.environ.get("FOLDER_FACTURAS", "")
SERVICE_ACCOUNT_JSON  = os.environ.get("GOOGLE_SERVICE_ACCOUNT", "")
ADMIN_TOKEN           = os.environ.get("ADMIN_TOKEN", "pdfocus_kino_2026")
SHEETS_ID             = os.environ.get("SHEETS_ID", "1a-lIDPDbimXLVys0vS6keqls-fz4bCU1pq28qiZ2qJY")

# Nombre del archivo de caché en Google Drive (se mantiene como fallback)
DRIVE_CACHE_FILENAME  = "pdfocus_index_cache.json"

# Hoja de Sheets para el caché del índice
CACHE_SHEET_NAME = "pdfocus_cache"
# Tamaño máximo de cada chunk en Sheets (50.000 chars por celda, usamos 45.000 para seguridad)
CACHE_CHUNK_SIZE = 45000

# Ruta temporal para el caché local
INDEX_CACHE_PATH = "/tmp/pdfocus_index_cache.json"

# Cache en memoria — el campo "compact" se guarda comprimido (gzip bytes)
# para reducir uso de RAM ~75%. Se descomprime solo al buscar.
_INDEX_CACHE = {}
_INDEX_LOCK  = threading.Lock()
_INDEX_BUILT = {"manifiestos": False, "facturas": False}


def _compress_compact(text: str) -> bytes:
    return gzip.compress(text.encode("utf-8"), compresslevel=1)


def _decompress_compact(data: bytes) -> str:
    return gzip.decompress(data).decode("utf-8")

# Cache de usuarios
_USERS_CACHE  = {}
_USERS_LOCK   = threading.Lock()
_USERS_LOADED = {"ts": 0}

# Intervalo de actualización incremental (1 hora)
_UPDATE_INTERVAL = 3600


# ── GOOGLE SERVICES ───────────────────────────────────────────
def _get_credentials():
    info = json.loads(SERVICE_ACCOUNT_JSON)
    scopes = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
    ]
    return service_account.Credentials.from_service_account_info(info, scopes=scopes)


def _get_drive_service():
    creds = _get_credentials()
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _get_sheets_service():
    creds = _get_credentials()
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


# ── CACHÉ EN GOOGLE SHEETS ────────────────────────────────────
# Usamos Sheets en vez de Drive porque los service accounts no tienen
# cuota de almacenamiento propia en Drive.
# El índice se comprime (gzip) y codifica en base64, luego se divide
# en chunks de 45.000 chars y se guarda una fila por chunk.
# Hoja: CACHE_SHEET_NAME, columna A = número de chunk, columna B = datos.

def _ensure_cache_sheet(svc):
    """Crea la hoja pdfocus_cache si no existe."""
    try:
        meta = svc.spreadsheets().get(spreadsheetId=SHEETS_ID).execute()
        names = [s["properties"]["title"] for s in meta.get("sheets", [])]
        if CACHE_SHEET_NAME not in names:
            svc.spreadsheets().batchUpdate(
                spreadsheetId=SHEETS_ID,
                body={"requests": [{"addSheet": {"properties": {"title": CACHE_SHEET_NAME}}}]}
            ).execute()
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

        svc = _get_sheets_service()
        _ensure_cache_sheet(svc)

        # Limpiar hoja y escribir chunks
        svc.spreadsheets().values().clear(
            spreadsheetId=SHEETS_ID,
            range=f"{CACHE_SHEET_NAME}!A:B",
        ).execute()
        rows = [[str(i), chunk] for i, chunk in enumerate(chunks)]
        svc.spreadsheets().values().update(
            spreadsheetId=SHEETS_ID,
            range=f"{CACHE_SHEET_NAME}!A1",
            valueInputOption="RAW",
            body={"values": rows},
        ).execute()
        print(f"[SHEETS-CACHE] ✅ Caché guardado en Sheets: {total} PDFs, {len(chunks)} chunks.", flush=True)
    except Exception as e:
        print(f"[SHEETS-CACHE] Error guardando caché: {e}", flush=True)


def _load_index_from_drive():
    """Carga el índice comprimido desde Google Sheets."""
    try:
        svc = _get_sheets_service()
        result = svc.spreadsheets().values().get(
            spreadsheetId=SHEETS_ID,
            range=f"{CACHE_SHEET_NAME}!A:B",
        ).execute()
        rows = result.get("values", [])
        if not rows:
            print("[SHEETS-CACHE] No hay caché en Sheets.", flush=True)
            return False
        # Ordenar por número de chunk y concatenar
        rows.sort(key=lambda r: int(r[0]) if r and r[0].isdigit() else 0)
        b64 = "".join(r[1] for r in rows if len(r) > 1)
        if not b64:
            return False
        compressed = base64.b64decode(b64)
        json_bytes = gzip.decompress(compressed)
        data = json.loads(json_bytes.decode("utf-8"))
        if not data:
            return False
        # Comprimir compact en memoria para ahorrar RAM
        for sec_key, entries in data.items():
            for fid, info in entries.items():
                if isinstance(info.get("compact"), str):
                    info["compact"] = _compress_compact(info["compact"])
        with _INDEX_LOCK:
            _INDEX_CACHE.clear()
            _INDEX_CACHE.update(data)
            for sec_key in ("manifiestos", "facturas"):
                if sec_key in data and len(data[sec_key]) > 0:
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
        print(f"[SHEETS-CACHE] Error cargando caché: {e}", flush=True)
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
        with _INDEX_LOCK:
            _INDEX_CACHE.clear()
            _INDEX_CACHE.update(data)
            for sec_key in ("manifiestos", "facturas"):
                if sec_key in data and len(data[sec_key]) > 0:
                    _INDEX_BUILT[sec_key] = True
        total = sum(len(v) for v in data.values())
        print(f"[CACHE-LOCAL] Índice cargado desde /tmp: {total} PDFs.", flush=True)
        return True
    except Exception as e:
        print(f"[CACHE-LOCAL] Error cargando desde /tmp: {e}", flush=True)
        return False


# ── GOOGLE SHEETS — USUARIOS ──────────────────────────────────
# Columnas: A=empresa, B=token, C=activo, D=fecha_creacion, E=password_hash

def _load_users_from_sheets():
    try:
        svc = _get_sheets_service()
        result = svc.spreadsheets().values().get(
            spreadsheetId=SHEETS_ID,
            range="usuarios_pdfocus!A2:E1000"
        ).execute()
        rows = result.get("values", [])
        users = {}
        for row in rows:
            if len(row) < 3:
                continue
            empresa       = row[0].strip() if row[0] else ""
            token         = row[1].strip() if row[1] else ""
            activo        = row[2].strip().upper() if row[2] else "NO"
            fecha         = row[3].strip() if len(row) > 3 else ""
            password_hash = row[4].strip() if len(row) > 4 else ""
            if token and activo == "SI":
                users[token] = {
                    "empresa": empresa,
                    "activo": True,
                    "fecha_creacion": fecha,
                    "password_hash": password_hash,
                }
        return users
    except Exception as e:
        print(f"[SHEETS] Error cargando usuarios: {e}", flush=True)
        return {}


_USERS_LOADING = threading.Event()  # evita cargas simultáneas

def _reload_users_sync():
    """Carga usuarios desde Sheets de forma síncrona. Solo llamar desde threads de background."""
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

    # Si el caché está vacío y hay una carga en progreso (startup), esperar hasta 8s
    if cache_empty and _USERS_LOADING.is_set():
        _USERS_LOADING.wait(timeout=8)
        with _USERS_LOCK:
            current = dict(_USERS_CACHE)

    if needs_reload and not _USERS_LOADING.is_set() and not cache_empty:
        # Recarga periódica en background (caché ya tiene datos)
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


def _add_user_to_sheets(empresa, token):
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M")
    try:
        svc = _get_sheets_service()
        # Columna E (password_hash) vacía al crear — admin la establece después
        svc.spreadsheets().values().append(
            spreadsheetId=SHEETS_ID,
            range="usuarios_pdfocus!A:E",
            valueInputOption="RAW",
            body={"values": [[empresa, token, "SI", fecha, ""]]}
        ).execute()
        _reload_users_sync()
        return True
    except Exception as e:
        print(f"[SHEETS] Error agregando usuario: {e}", flush=True)
        return False


def _set_user_password(token, new_password):
    """Establece o cambia la contraseña de una empresa. Guarda el hash en columna E."""
    try:
        svc = _get_sheets_service()
        result = svc.spreadsheets().values().get(
            spreadsheetId=SHEETS_ID,
            range="usuarios_pdfocus!B2:B1000"
        ).execute()
        rows = result.get("values", [])
        for i, row in enumerate(rows):
            if row and row[0].strip() == token:
                row_num = i + 2
                hashed = generate_password_hash(new_password)
                svc.spreadsheets().values().update(
                    spreadsheetId=SHEETS_ID,
                    range=f"usuarios_pdfocus!E{row_num}",
                    valueInputOption="RAW",
                    body={"values": [[hashed]]}
                ).execute()
                _reload_users_sync()
                return True
        return False
    except Exception as e:
        print(f"[SHEETS] Error cambiando contraseña: {e}", flush=True)
        return False


def _set_user_status(token, activo):
    try:
        svc = _get_sheets_service()
        result = svc.spreadsheets().values().get(
            spreadsheetId=SHEETS_ID,
            range="usuarios_pdfocus!B2:B1000"
        ).execute()
        rows = result.get("values", [])
        for i, row in enumerate(rows):
            if row and row[0].strip() == token:
                row_num = i + 2
                nuevo_estado = "SI" if activo else "NO"
                svc.spreadsheets().values().update(
                    spreadsheetId=SHEETS_ID,
                    range=f"usuarios_pdfocus!C{row_num}",
                    valueInputOption="RAW",
                    body={"values": [[nuevo_estado]]}
                ).execute()
                _reload_users_sync()
                return True
        return False
    except Exception as e:
        print(f"[SHEETS] Error cambiando estado: {e}", flush=True)
        return False


def _delete_user_from_sheets(token):
    try:
        svc = _get_sheets_service()
        result = svc.spreadsheets().values().get(
            spreadsheetId=SHEETS_ID,
            range="usuarios_pdfocus!B2:B1000"
        ).execute()
        rows = result.get("values", [])
        for i, row in enumerate(rows):
            if row and row[0].strip() == token:
                row_num = i + 2
                sheet_id = svc.spreadsheets().get(
                    spreadsheetId=SHEETS_ID
                ).execute()["sheets"][0]["properties"]["sheetId"]
                svc.spreadsheets().batchUpdate(
                    spreadsheetId=SHEETS_ID,
                    body={"requests": [{
                        "deleteDimension": {
                            "range": {
                                "sheetId": sheet_id,
                                "dimension": "ROWS",
                                "startIndex": row_num - 1,
                                "endIndex": row_num,
                            }
                        }
                    }]}
                ).execute()
                _reload_users_sync()
                return True
        return False
    except Exception as e:
        print(f"[SHEETS] Error eliminando usuario: {e}", flush=True)
        return False


def _generate_token(length=10):
    chars = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(chars) for _ in range(length))


def _validate_token(token):
    """Valida si un token existe y está activo. Retorna info del usuario o None."""
    if not token:
        return None
    users = _get_users()
    # Si caché vacío, esperar máx 5s a que el background loader termine
    if not users:
        if _USERS_LOADING.is_set():
            _USERS_LOADING.wait(timeout=5)
        with _USERS_LOCK:
            users = dict(_USERS_CACHE)
    # Fallback síncrono si sigue vacío
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


# ── GOOGLE DRIVE — PDFs ───────────────────────────────────────
def list_pdfs_in_folder(folder_id, include_modified_time=False):
    svc = _get_drive_service()

    def _get_subfolders(fid):
        subs = []
        pt = None
        while True:
            r = svc.files().list(
                q=f"'{fid}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false",
                fields="nextPageToken, files(id)",
                pageSize=1000,
                pageToken=pt,
            ).execute()
            subs.extend([f["id"] for f in r.get("files", [])])
            pt = r.get("nextPageToken")
            if not pt:
                break
        return subs

    def _get_pdfs(fid):
        pdfs = []
        pt = None
        fields = "nextPageToken, files(id, name, modifiedTime)" if include_modified_time else "nextPageToken, files(id, name)"
        while True:
            r = svc.files().list(
                q=f"'{fid}' in parents and mimeType='application/pdf' and trashed=false",
                fields=fields,
                pageSize=1000,
                pageToken=pt,
            ).execute()
            pdfs.extend(r.get("files", []))
            pt = r.get("nextPageToken")
            if not pt:
                break
        return pdfs

    all_pdfs = []
    queue = [folder_id]
    visited = set()
    while queue:
        fid = queue.pop(0)
        if fid in visited:
            continue
        visited.add(fid)
        all_pdfs.extend(_get_pdfs(fid))
        queue.extend(_get_subfolders(fid))
    return all_pdfs


def download_pdf_bytes(file_id):
    svc = _get_drive_service()
    req = svc.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    dl = MediaIoBaseDownload(buf, r
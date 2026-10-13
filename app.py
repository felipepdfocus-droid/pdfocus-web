"""
PDFocus Web — Felipe Ceballos
Busca referencias en PDFs almacenados en Google Drive y los devuelve marcados.
Sistema de login por token por empresa, almacenado en Google Sheets.
Cache en disco: el índice se guarda en /tmp/pdfocus_index_cache.json
para sobrevivir reinicios sin reconstruir desde cero.
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

import pymupdf as fitz
from flask import (
    Flask, request, jsonify, send_file,
    render_template, redirect, url_for, make_response
)
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

app = Flask(__name__)

# ── CONFIG ────────────────────────────────────────────────────
FOLDER_ID_MANIFIESTOS = os.environ.get("FOLDER_MANIFIESTOS", "")
FOLDER_ID_FACTURAS    = os.environ.get("FOLDER_FACTURAS", "")
SERVICE_ACCOUNT_JSON  = os.environ.get("GOOGLE_SERVICE_ACCOUNT", "")
ADMIN_TOKEN           = os.environ.get("ADMIN_TOKEN", "pdfocus_kino_2026")
SHEETS_ID             = os.environ.get("SHEETS_ID", "1a-lIDPDbimXLVys0vS6keqls-fz4bCU1pq28qiZ2qJY")

# Ruta del cache en disco (persiste entre reinicios del proceso en Render)
INDEX_CACHE_PATH = "/tmp/pdfocus_index_cache.json"

# Cache en memoria
_INDEX_CACHE = {}          # {section_key: {file_id: {name, compact, modified_time}}}
_INDEX_LOCK  = threading.Lock()
_INDEX_BUILT = {"manifiestos": False, "facturas": False}

# Cache de usuarios (se recarga desde Sheets en cada validación o cada 5 min)
_USERS_CACHE  = {}           # {token: {empresa, activo, fecha_creacion}}
_USERS_LOCK   = threading.Lock()
_USERS_LOADED = {"ts": 0}    # timestamp última carga

# Intervalo de actualización incremental (1 hora)
_UPDATE_INTERVAL = 3600


# ── CACHE EN DISCO ────────────────────────────────────────────
def _save_index_to_disk():
    """Guarda el índice en disco para sobrevivir reinicios."""
    try:
        with _INDEX_LOCK:
            data = dict(_INDEX_CACHE)
        # El compact puede ser muy grande; guardamos todo
        with open(INDEX_CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        total = sum(len(v) for v in data.values())
        print(f"[CACHE] Índice guardado en disco: {total} PDFs.", flush=True)
    except Exception as e:
        print(f"[CACHE] Error guardando índice en disco: {e}", flush=True)


def _load_index_from_disk():
    """Carga el índice desde disco si existe. Retorna True si lo cargó."""
    if not os.path.exists(INDEX_CACHE_PATH):
        print("[CACHE] No hay cache en disco.", flush=True)
        return False
    try:
        with open(INDEX_CACHE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not data:
            print("[CACHE] Cache en disco vacío.", flush=True)
            return False
        with _INDEX_LOCK:
            _INDEX_CACHE.clear()
            _INDEX_CACHE.update(data)
            for sec_key in ("manifiestos", "facturas"):
                if sec_key in data and len(data[sec_key]) > 0:
                    _INDEX_BUILT[sec_key] = True
        total = sum(len(v) for v in data.values())
        print(f"[CACHE] Índice cargado desde disco: {total} PDFs. ¡Inicio rápido!", flush=True)
        return True
    except Exception as e:
        print(f"[CACHE] Error cargando índice desde disco: {e}", flush=True)
        return False


# ── GOOGLE SERVICES ───────────────────────────────────────────
def _get_credentials():
    info = json.loads(SERVICE_ACCOUNT_JSON)
    scopes = [
        "https://www.googleapis.com/auth/drive.readonly",
        "https://www.googleapis.com/auth/spreadsheets",
    ]
    return service_account.Credentials.from_service_account_info(info, scopes=scopes)


def _get_drive_service():
    creds = _get_credentials()
    return build("drive", "v3", credentials=creds)


def _get_sheets_service():
    creds = _get_credentials()
    return build("sheets", "v4", credentials=creds)


# ── GOOGLE SHEETS — USUARIOS ──────────────────────────────────
# Estructura de la hoja "usuarios_pdfocus":
# Columna A: empresa | B: token | C: activo (SI/NO) | D: fecha_creacion

def _load_users_from_sheets():
    """Carga todos los usuarios activos desde Google Sheets."""
    try:
        svc = _get_sheets_service()
        result = svc.spreadsheets().values().get(
            spreadsheetId=SHEETS_ID,
            range="usuarios_pdfocus!A2:D1000"
        ).execute()
        rows = result.get("values", [])
        users = {}
        for row in rows:
            if len(row) < 3:
                continue
            empresa = row[0].strip() if row[0] else ""
            token   = row[1].strip() if row[1] else ""
            activo  = row[2].strip().upper() if row[2] else "NO"
            fecha   = row[3].strip() if len(row) > 3 else ""
            if token and activo == "SI":
                users[token] = {
                    "empresa": empresa,
                    "activo": True,
                    "fecha_creacion": fecha,
                }
        return users
    except Exception as e:
        print(f"[SHEETS] Error cargando usuarios: {e}", flush=True)
        return {}


def _get_users(force_reload=False):
    """Devuelve el cache de usuarios, recargando si han pasado más de 5 minutos."""
    now = time.time()
    with _USERS_LOCK:
        if force_reload or (now - _USERS_LOADED["ts"]) > 300:
            users = _load_users_from_sheets()
            _USERS_CACHE.clear()
            _USERS_CACHE.update(users)
            _USERS_LOADED["ts"] = now
        return dict(_USERS_CACHE)


def _add_user_to_sheets(empresa, token):
    """Agrega una nueva empresa/token a Google Sheets."""
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M")
    try:
        svc = _get_sheets_service()
        svc.spreadsheets().values().append(
            spreadsheetId=SHEETS_ID,
            range="usuarios_pdfocus!A:D",
            valueInputOption="RAW",
            body={"values": [[empresa, token, "SI", fecha]]}
        ).execute()
        # Forzar recarga del cache
        _get_users(force_reload=True)
        return True
    except Exception as e:
        print(f"[SHEETS] Error agregando usuario: {e}", flush=True)
        return False


def _set_user_status(token, activo):
    """Activa o desactiva un token en Google Sheets."""
    try:
        svc = _get_sheets_service()
        result = svc.spreadsheets().values().get(
            spreadsheetId=SHEETS_ID,
            range="usuarios_pdfocus!B2:B1000"
        ).execute()
        rows = result.get("values", [])
        for i, row in enumerate(rows):
            if row and row[0].strip() == token:
                row_num = i + 2  # +2 porque empieza en fila 2
                nuevo_estado = "SI" if activo else "NO"
                svc.spreadsheets().values().update(
                    spreadsheetId=SHEETS_ID,
                    range=f"usuarios_pdfocus!C{row_num}",
                    valueInputOption="RAW",
                    body={"values": [[nuevo_estado]]}
                ).execute()
                _get_users(force_reload=True)
                return True
        return False
    except Exception as e:
        print(f"[SHEETS] Error cambiando estado: {e}", flush=True)
        return False


def _delete_user_from_sheets(token):
    """Elimina una fila de Google Sheets por token."""
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
                _get_users(force_reload=True)
                return True
        return False
    except Exception as e:
        print(f"[SHEETS] Error eliminando usuario: {e}", flush=True)
        return False


def _generate_token(length=10):
    """Genera un token aleatorio alfanumérico en mayúsculas."""
    chars = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(chars) for _ in range(length))


def _validate_token(token):
    """Valida si un token existe y está activo. Retorna empresa o None."""
    if not token:
        return None
    users = _get_users()
    user = users.get(token)
    if user and user.get("activo"):
        return user["empresa"]
    return None


# ── GOOGLE DRIVE ──────────────────────────────────────────────
def list_pdfs_in_folder(folder_id, include_modified_time=False):
    """Lista todos los PDFs en la carpeta y sus subcarpetas (recursivo)."""
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
    dl = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        _, done = dl.next_chunk()
    buf.seek(0)
    return buf.read()


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


# ── ÍNDICE ────────────────────────────────────────────────────
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
    for f in pdfs:
        try:
            data = download_pdf_bytes(f["id"])
            compact = build_compact_from_bytes(data)
            new_entries[f["id"]] = {
                "name": f["name"],
                "compact": compact,
                "modified_time": f.get("modifiedTime", ""),
            }
            print(f"  ✔ Indexado: {f['name']}", flush=True)
        except Exception as e:
            print(f"  ✖ Error indexando {f['name']}: {e}", flush=True)
    with _INDEX_LOCK:
        _INDEX_CACHE[section_key] = new_entries
        _INDEX_BUILT[section_key] = True
    print(f"[INDEX] {section_key}: {len(new_entries)} PDFs indexados. LISTO.", flush=True)
    # Guardar en disco después de cada sección
    _save_index_to_disk()


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
    for f in pdfs:
        fid = f["id"]
        mod = f.get("modifiedTime", "")
        if fid in current and current[fid].get("modified_time") == mod:
            continue
        try:
            data = download_pdf_bytes(fid)
            compact = build_compact_from_bytes(data)
            current[fid] = {
                "name": f["name"],
                "compact": compact,
                "modified_time": mod,
            }
            print(f"  ✔ Actualizado: {f['name']}", flush=True)
            nuevos += 1
        except Exception as e:
            print(f"  ✖ Error actualizando {f['name']}: {e}", flush=True)

    with _INDEX_LOCK:
        _INDEX_CACHE[section_key] = current

    if nuevos > 0 or removed:
        _save_index_to_disk()

    print(f"[UPDATE] {section_key}: {nuevos} nuevos/modificados, {len(removed)} eliminados. Total: {len(current)}.", flush=True)


def ensure_index(section_key, folder_id):
    if not _INDEX_BUILT.get(section_key):
        build_index(folder_id, section_key)


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
                pages[p].draw_rect(
                    R2,
                    color=color,
                    fill=color,
                    fill_opacity=0.40,
                    width=0,
                    overlay=True,
                )
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


# ── DECORADOR DE AUTENTICACIÓN ────────────────────────────────
def require_token(f):
    """Decorador: requiere token válido en query param o cookie."""
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.args.get("token") or request.cookies.get("pdfocus_token")
        empresa = _validate_token(token)
        if not empresa:
            return redirect(url_for("login_page"))
        return f(*args, empresa=empresa, token=token, **kwargs)
    return decorated


# ── RUTAS PÚBLICAS ────────────────────────────────────────────
@app.route("/login")
def login_page():
    """Página de login (redirección cuando no hay token válido)."""
    return render_template("login.html")


@app.route("/acceso")
def acceso():
    """
    Valida el token que viene en la URL (?token=XYZ) y si es válido
    setea una cookie de sesión y redirige a la app principal.
    """
    token = request.args.get("token", "").strip()
    empresa = _validate_token(token)
    if not empresa:
        return render_template("login.html", error="Token inválido o inactivo.")
    # Setear cookie con el token
    resp = make_response(redirect(url_for("index") + f"?token={token}"))
    resp.set_cookie("pdfocus_token", token, max_age=60 * 60 * 24 * 30)  # 30 días
    return resp


@app.route("/status")
def status():
    """Estado del índice — usado por UptimeRobot para keep-alive."""
    with _INDEX_LOCK:
        info = {
            sec: {
                "listo": _INDEX_BUILT.get(sec, False),
                "pdfs": len(_INDEX_CACHE.get(sec, {})),
            }
            for sec in ("manifiestos", "facturas")
        }
    return jsonify(info)


# ── RUTAS PROTEGIDAS (requieren token) ────────────────────────
@app.route("/")
@require_token
def index(empresa=None, token=None):
    resp = make_response(render_template("index.html", empresa=empresa, token=token))
    # Renovar cookie en cada visita
    resp.set_cookie("pdfocus_token", token, max_age=60 * 60 * 24 * 30)
    return resp


@app.route("/buscar", methods=["POST"])
@require_token
def buscar(empresa=None, token=None):
    data   = request.get_json()
    codigo = (data.get("codigo") or "").strip()
    tipo   = (data.get("tipo") or "ambos").lower()

    if not codigo:
        return jsonify({"error": "Escribe un código para buscar."}), 400

    ref_norm = normalize_compact(codigo)
    resultados = []

    secciones = []
    if tipo in ("ambos", "manifiestos") and FOLDER_ID_MANIFIESTOS:
        secciones.append(("manifiestos", FOLDER_ID_MANIFIESTOS, "manifesto"))
    if tipo in ("ambos", "facturas") and FOLDER_ID_FACTURAS:
        secciones.append(("facturas", FOLDER_ID_FACTURAS, "factura"))

    for sec_key, folder_id, mode in secciones:
        ensure_index(sec_key, folder_id)
        index_data = _INDEX_CACHE.get(sec_key, {})
        for file_id, info in index_data.items():
            if ref_in_text_safe(ref_norm, info["compact"]):
                resultados.append({
                    "nombre":   info["name"],
                    "tipo":     mode,
                    "drive_id": file_id,
                })

    if not resultados:
        return jsonify({"resultados": [], "mensaje": "No se encontró la referencia en ningún PDF."})

    resultados.sort(key=lambda x: _fecha_clave(x["nombre"]), reverse=True)
    return jsonify({"resultados": resultados})


@app.route("/ver_pdf")
@require_token
def ver_pdf(empresa=None, token=None):
    drive_id = request.args.get("drive_id", "")
    codigo   = request.args.get("codigo", "").strip()
    tipo     = request.args.get("tipo", "manifesto").lower()

    if not drive_id or not codigo:
        return "Parámetros faltantes.", 400

    try:
        pdf_bytes = download_pdf_bytes(drive_id)
    except Exception as e:
        print(f"[VER_PDF] Error descargando {drive_id}: {e}", flush=True)
        return f"No se pudo descargar el PDF: {e}", 500

    try:
        mode = "manifesto" if "manifest" in tipo or tipo == "manifesto" else "factura"
        marked_bytes, found = annotate_pdf_bytes(pdf_bytes, codigo, mode=mode)
        if not found:
            print(f"[VER_PDF] Referencia '{codigo}' no encontrada en {drive_id}", flush=True)
    except Exception as e:
        print(f"[VER_PDF] Error anotando PDF {drive_id}: {e}", flush=True)
        marked_bytes = pdf_bytes

    return send_file(
        io.BytesIO(marked_bytes),
        mimetype="application/pdf",
        as_attachment=False,
        download_name=f"PDFocus_{codigo}.pdf",
    )


# ── PANEL DE ADMINISTRACIÓN ───────────────────────────────────
def _check_admin():
    """Verifica si la petición tiene el token de admin válido (header o query param)."""
    token = request.headers.get("X-Admin-Token") or request.args.get("admin_token") or request.cookies.get("pdfocus_admin")
    return token == ADMIN_TOKEN


@app.route("/admin")
def admin_panel():
    """Panel de administración — requiere ADMIN_TOKEN."""
    if not _check_admin():
        return render_template("admin_login.html")
    users = _get_users(force_reload=True)
    with _INDEX_LOCK:
        index_info = {
            sec: {
                "listo": _INDEX_BUILT.get(sec, False),
                "pdfs": len(_INDEX_CACHE.get(sec, {})),
            }
            for sec in ("manifiestos", "facturas")
        }
    return render_template("admin.html", users=users, index_info=index_info)


@app.route("/admin/login", methods=["POST"])
def admin_login():
    token = request.form.get("admin_token", "").strip()
    if token != ADMIN_TOKEN:
        return render_template("admin_login.html", error="Token incorrecto.")
    resp = make_response(redirect(url_for("admin_panel")))
    resp.set_cookie("pdfocus_admin", token, max_age=60 * 60 * 8)  # 8 horas
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
    empresa = (request.form.get("empresa") or request.get_json(silent=True, force=True) or {}).get("empresa") or ""
    if isinstance(empresa, dict):
        empresa = ""
    empresa = str(empresa).strip()
    if not empresa:
        data = request.get_json(silent=True) or {}
        empresa = data.get("empresa", "").strip()
    if not empresa:
        return jsonify({"error": "Nombre de empresa requerido"}), 400
    token = _generate_token()
    ok = _add_user_to_sheets(empresa, token)
    if ok:
        url = f"https://pdfocus-web-1sbt.onrender.com/?token={token}"
        return jsonify({"ok": True, "empresa": empresa, "token": token, "url": url})
    return jsonify({"error": "No se pudo guardar en Sheets"}), 500


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
        _INDEX_BUILT["manifiestos"] = False
        _INDEX_BUILT["facturas"]    = False
        _INDEX_CACHE.clear()
    # Borrar cache en disco para forzar reconstrucción completa
    try:
        if os.path.exists(INDEX_CACHE_PATH):
            os.remove(INDEX_CACHE_PATH)
    except Exception:
        pass
    threading.Thread(target=_rebuild_all, daemon=True).start()
    return jsonify({"ok": True, "mensaje": "Reconstrucción iniciada."})


def _rebuild_all():
    if FOLDER_ID_MANIFIESTOS:
        build_index(FOLDER_ID_MANIFIESTOS, "manifiestos")
    if FOLDER_ID_FACTURAS:
        build_index(FOLDER_ID_FACTURAS, "facturas")


# ── ACTUALIZACIÓN INCREMENTAL CADA HORA ──────────────────────
def _hourly_update_loop():
    while not (_INDEX_BUILT.get("manifiestos") and _INDEX_BUILT.get("facturas")):
        time.sleep(60)

    while True:
        time.sleep(_UPDATE_INTERVAL)
        print("[HOURLY] Iniciando actualización incremental...", flush=True)
        if FOLDER_ID_MANIFIESTOS:
            try:
                update_index_incremental(FOLDER_ID_MANIFIESTOS, "manifiestos")
            except Exception as e:
                print(f"[HOURLY] Error actualizando manifiestos: {e}", flush=True)
        if FOLDER_ID_FACTURAS:
            try:
                update_index_incremental(FOLDER_ID_FACTURAS, "facturas")
            except Exception as e:
                print(f"[HOURLY] Error actualizando facturas: {e}", flush=True)
        print("[HOURLY] Actualización incremental completada.", flush=True)


# ── ARRANQUE DEL ÍNDICE ──────────────────────────────────────
def _start_index_background():
    def _build():
        print("[STARTUP] Iniciando construcción del índice...", flush=True)
        print(f"[STARTUP] FOLDER_MANIFIESTOS={FOLDER_ID_MANIFIESTOS}", flush=True)
        print(f"[STARTUP] FOLDER_FACTURAS={FOLDER_ID_FACTURAS}", flush=True)
        print(f"[STARTUP] JSON len={len(SERVICE_ACCOUNT_JSON)}", flush=True)
        if not SERVICE_ACCOUNT_JSON:
            print("[STARTUP] ERROR: GOOGLE_SERVICE_ACCOUNT está vacío.", flush=True)
            return

        # Intentar cargar desde cache en disco primero
        cache_loaded = _load_index_from_disk()
        if cache_loaded:
            print("[STARTUP] Índice listo (desde cache en disco).", flush=True)
            # Aún así hacer actualización incremental en background para nuevos PDFs
            print("[STARTUP] Verificando cambios nuevos en Drive...", flush=True)
            if FOLDER_ID_MANIFIESTOS:
                try:
                    update_index_incremental(FOLDER_ID_MANIFIESTOS, "manifiestos")
                except Exception as e:
                    print(f"[STARTUP] Error actualizando manifiestos: {e}", flush=True)
            if FOLDER_ID_FACTURAS:
                try:
                    update_index_incremental(FOLDER_ID_FACTURAS, "facturas")
                except Exception as e:
                    print(f"[STARTUP] Error actualizando facturas: {e}", flush=True)
            print("[STARTUP] Verificación de cambios completada.", flush=True)
            return

        # No hay cache — construir desde cero
        if FOLDER_ID_MANIFIESTOS:
            try:
                build_index(FOLDER_ID_MANIFIESTOS, "manifiestos")
            except Exception as e:
                print(f"[STARTUP] Error indexando manifiestos: {e}", flush=True)
                traceback.print_exc()
        if FOLDER_ID_FACTURAS:
            try:
                build_index(FOLDER_ID_FACTURAS, "facturas")
            except Exception as e:
                print(f"[STARTUP] Error indexando facturas: {e}", flush=True)
                traceback.print_exc()
        print("[STARTUP] Índice listo.", flush=True)

    t = threading.Thread(target=_build, daemon=True)
    t.start()
    t2 = threading.Thread(target=_hourly_update_loop, daemon=True)
    t2.start()


_start_index_background()

if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

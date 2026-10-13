"""
PDFocus Web — Felipe Ceballos
Busca referencias en PDFs almacenados en Google Drive y los devuelve marcados.
"""

import os
import re
import io
import json
import time
import threading
import traceback
import tempfile
import hashlib

import pymupdf as fitz
from flask import Flask, request, jsonify, send_file, render_template
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

app = Flask(__name__)

# ── CONFIG ────────────────────────────────────────────────────
FOLDER_ID_MANIFIESTOS = os.environ.get("FOLDER_MANIFIESTOS", "")
FOLDER_ID_FACTURAS    = os.environ.get("FOLDER_FACTURAS", "")
SERVICE_ACCOUNT_JSON  = os.environ.get("GOOGLE_SERVICE_ACCOUNT", "")

# Cache en memoria
_INDEX_CACHE = {}
_INDEX_LOCK  = threading.Lock()
_INDEX_BUILT = {"manifiestos": False, "facturas": False}

# ── GOOGLE DRIVE ──────────────────────────────────────────────
def _get_drive_service():
    info = json.loads(SERVICE_ACCOUNT_JSON)
    creds = service_account.Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
    return build("drive", "v3", credentials=creds)


def list_pdfs_in_folder(folder_id):
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
        while True:
            r = svc.files().list(
                q=f"'{fid}' in parents and mimeType='application/pdf' and trashed=false",
                fields="nextPageToken, files(id, name)",
                pageSize=1000,
                pageToken=pt,
            ).execute()
            pdfs.extend(r.get("files", []))
            pt = r.get("nextPageToken")
            if not pt:
                break
        return pdfs

    # BFS por todas las subcarpetas
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
    # "+" antes o después de la ref → es ref compuesta (S-644WJ+S-653WJ)
    # → no es un match válido de ninguna de las dos refs por separado
    if after == "+" or before == "+":
        return False
    return _is_border_char(before, ref_norm) and _is_border_char(after, ref_norm)


def ref_in_text_safe(ref_norm, text):
    # Búsqueda con _SEP como separador de palabras (más precisa)
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
    """
    Extrae (año, mes, dia) del nombre del archivo para ordenar.
    Formatos soportados:
      - "GK-SANSE-JX-SEP 15 2025.pdf"    → (2025, 9, 15)
      - "27 NOV 2025 JX-SANSE.pdf"        → (2025, 11, 27)
      - "MANIF OCT 2025.pdf"              → (2025, 10, 1)
      - "2025-09-15 algo.pdf"             → (2025, 9, 15)
    Si no encuentra fecha devuelve (0, 0, 0) → queda al final.
    """
    n = nombre.upper()

    # Formato ISO: 2025-09-15
    m = re.search(r"(\d{4})[_\-](\d{2})[_\-](\d{2})", n)
    if m:
        return (int(m.group(1)), int(m.group(2)), int(m.group(3)))

    # Mes abreviado/completo en español con año y día opcional
    # Ejemplos: "SEP 15 2025", "27 NOV 2025", "OCT 2025"
    mes_patron = "|".join(sorted(_MESES_ES.keys(), key=len, reverse=True)).upper()
    # Patron: [dia] MES [dia] AÑO
    m = re.search(
        r"(?:(\d{1,2})\s+)?(" + mes_patron + r")(?:\s+(\d{1,2}))?\s+(\d{4})",
        n
    )
    if m:
        dia1, mes_str, dia2, anio = m.group(1), m.group(2), m.group(3), m.group(4)
        dia = int(dia1 or dia2 or 1)
        mes = _MESES_ES.get(mes_str.lower(), 0)
        return (int(anio), mes, dia)

    # Solo año de 4 dígitos
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
    pdfs = list_pdfs_in_folder(folder_id)
    print(f"[INDEX] {section_key}: {len(pdfs)} PDFs encontrados.", flush=True)
    new_entries = {}
    for f in pdfs:
        try:
            data = download_pdf_bytes(f["id"])
            compact = build_compact_from_bytes(data)
            new_entries[f["id"]] = {
                "name": f["name"],
                "compact": compact,
            }
            print(f"  ✔ Indexado: {f['name']}", flush=True)
        except Exception as e:
            print(f"  ✖ Error indexando {f['name']}: {e}", flush=True)
    with _INDEX_LOCK:
        _INDEX_CACHE[section_key] = new_entries
        _INDEX_BUILT[section_key] = True
    print(f"[INDEX] {section_key}: {len(new_entries)} PDFs indexados. LISTO.", flush=True)


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
    """
    Marca el PDF quemando rectángulos directamente en el contenido
    (draw_rect con fill semitransparente) en lugar de usar anotaciones.
    Esto asegura que las marcas sean visibles en todos los visores,
    incluyendo navegadores móviles (Android/iOS) que ignoran las anotaciones PDF.
    """
    ref_norm = normalize_compact(ref_raw)
    vcs = list(dict.fromkeys(
        [normalize_compact(v) for v in generar_variantes(ref_raw)] + [ref_norm]
    ))

    # Verde fosforescente tipo resaltador para manifiestos, azul para facturas
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
                # Expandir ligeramente el rect para que se vea mejor
                R2 = fitz.Rect(R.x0 - 1, R.y0 - 2, R.x1 + 1, R.y1 + 2)
                # 1) draw_rect quema la marca en el contenido (visible en la mayoría de visores)
                pages[p].draw_rect(
                    R2,
                    color=color,
                    fill=color,
                    fill_opacity=0.40,
                    width=0,
                    overlay=True,       # forzar que quede encima del contenido
                )
                # 2) Anotación highlight estándar como respaldo
                # (algunos visores móviles solo muestran una de las dos)
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


# ── RUTAS FLASK ───────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/buscar", methods=["POST"])
def buscar():
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

    # Ordenar del más reciente al más antiguo por fecha extraída del nombre
    resultados.sort(key=lambda x: _fecha_clave(x["nombre"]), reverse=True)

    return jsonify({"resultados": resultados})


@app.route("/ver_pdf")
def ver_pdf():
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
        marked_bytes = pdf_bytes  # devolver sin marcar antes de fallar

    return send_file(
        io.BytesIO(marked_bytes),
        mimetype="application/pdf",
        as_attachment=False,
        download_name=f"PDFocus_{codigo}.pdf",
    )


@app.route("/rebuild_index", methods=["POST"])
def rebuild_index():
    token = request.headers.get("X-Admin-Token", "")
    if token != os.environ.get("ADMIN_TOKEN", "pdfocus_admin"):
        return jsonify({"error": "No autorizado"}), 401
    with _INDEX_LOCK:
        _INDEX_BUILT["manifiestos"] = False
        _INDEX_BUILT["facturas"]    = False
        _INDEX_CACHE.clear()
    threading.Thread(target=_rebuild_all, daemon=True).start()
    return jsonify({"ok": True, "mensaje": "Reconstrucción iniciada en segundo plano."})


def _rebuild_all():
    if FOLDER_ID_MANIFIESTOS:
        build_index(FOLDER_ID_MANIFIESTOS, "manifiestos")
    if FOLDER_ID_FACTURAS:
        build_index(FOLDER_ID_FACTURAS, "facturas")


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


# Lanza el índice al importar el módulo (gunicorn lo importa al arrancar)
_start_index_background()

if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

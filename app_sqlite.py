# app_sqlite.py
import os
import io
import re
import bcrypt
import sqlite3
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict
from flask import (
    Flask, render_template, request, redirect, url_for,
    session, send_file, abort, flash, jsonify
)
from werkzeug.middleware.proxy_fix import ProxyFix
from dotenv import load_dotenv; load_dotenv()
from collections import defaultdict
import pandas as pd

# =========================
# CONFIGURAÇÕES
# =========================
DB_PATH = os.getenv("DB_PATH", os.path.abspath("contracheque.db"))
SECRET_KEY = os.getenv("SECRET_KEY", "troque-esta-secret-key-em-producao")
STORAGE_DIR = os.getenv("STORAGE_DIR", os.path.abspath("contracheques_split"))
ADMIN_MATRICULAS = os.getenv("ADMIN_MATRICULAS", "")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")
COMPLEMENT_FILES = [
    ("essencial", os.path.abspath("12essenciais.xlsx")),
    ("estabilidade", os.path.abspath("59estabilidade.xlsx")),
    ("aviso_previo", os.path.abspath("160aviso.xlsx")),
]
_COMPLEMENT_MAP_CACHE: Dict[str, str] = {}

# =========================
# APP
# =========================
app = Flask(__name__, template_folder="templates", static_folder="static")
app.secret_key = SECRET_KEY
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_port=1, x_host=1)

# =========================
# SCHEMA + CONEXÃO
# =========================
DDL_USERS = """
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    matricula TEXT NOT NULL UNIQUE,
    nome TEXT,
    cpf TEXT,
    department TEXT,
    position TEXT,
    email TEXT,
    last_login_at TEXT,
    is_admin INTEGER NOT NULL DEFAULT 0,
    password_hash TEXT NOT NULL,
    must_change_password INTEGER NOT NULL DEFAULT 1
);
"""

DDL_PAYSLIPS = """
CREATE TABLE IF NOT EXISTS payslips (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    referencia TEXT NOT NULL,
    file_path TEXT NOT NULL,
    file_name TEXT,
    issued_by_admin INTEGER NOT NULL DEFAULT 0,
    viewed_at TEXT,
    downloaded_at TEXT,
    UNIQUE(user_id, referencia),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);
"""

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True) if os.path.dirname(DB_PATH) else None
    conn = get_db()
    try:
        conn.executescript(DDL_USERS + DDL_PAYSLIPS)
        _ensure_columns(conn)
        _sync_user_complements(conn)
        bootstrap_admins(conn)
        _backfill_last_login(conn)
        conn.commit()
    finally:
        conn.close()

# =========================
# HELPERS
# =========================
def build_matricula_candidates(user_input: str):
    s = (user_input or "").strip()
    cands = {s}
    digits = "".join(ch for ch in s if ch.isdigit())
    if digits:
        cands.add(digits)
        for width in (6, 7, 8):
            if len(digits) < width:
                cands.add(digits.zfill(width))
    return list(cands)

def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode("utf-8")[:72], bcrypt.gensalt()).decode("utf-8")

def verify_password(plain: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(plain.encode("utf-8")[:72], hashed.encode("utf-8"))
    except Exception:
        return False

def _normalize_to_abs(p: str) -> str:
    """
    Normaliza separadores vindos do Windows, resolve .. e retorna caminho absoluto.
    Aceita caminhos relativos gravados no DB.
    """
    p = (p or "").replace("\\", "/")
    P = Path(p)
    if not P.is_absolute():
        P = Path(os.getcwd()) / P
    return str(P.resolve())

def bootstrap_admins(conn=None):
    admin_set = _admin_matriculas_set()
    if not admin_set:
        return

    owns_conn = conn is None
    if owns_conn:
        conn = get_db()
    cur = conn.cursor()
    try:
        for matricula in admin_set:
            cur.execute("SELECT id FROM users WHERE matricula=?", (matricula,))
            row = cur.fetchone()
            if row:
                cur.execute("UPDATE users SET is_admin=1 WHERE id=?", (row["id"],))
                continue
            if not ADMIN_PASSWORD:
                continue
            cur.execute(
                """
                INSERT INTO users (matricula, nome, password_hash, must_change_password, is_admin)
                VALUES (?,?,?,?,1)
                """,
                (matricula, "Administrador", hash_password(ADMIN_PASSWORD), 0)
            )
    finally:
        cur.close()
        if owns_conn:
            conn.commit()
            conn.close()

def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def _admin_matriculas_set():
    return {m.strip() for m in ADMIN_MATRICULAS.replace(";", ",").split(",") if m.strip()}

def _is_admin_matricula(matricula: str) -> bool:
    admin_set = _admin_matriculas_set()
    if not admin_set:
        return False
    candidates = build_matricula_candidates(matricula)
    return any(c in admin_set for c in candidates)

def _ensure_columns(conn):
    def ensure(table: str, columns: dict):
        existing = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}
        for name, ddl in columns.items():
            if name not in existing:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")

    ensure("users", {
        "cpf": "cpf TEXT",
        "department": "department TEXT",
        "position": "position TEXT",
        "email": "email TEXT",
        "last_login_at": "last_login_at TEXT",
        "is_admin": "is_admin INTEGER NOT NULL DEFAULT 0",
        "complemento": "complemento TEXT",
    })
    ensure("payslips", {
        "file_name": "file_name TEXT",
        "issued_by_admin": "issued_by_admin INTEGER NOT NULL DEFAULT 0",
        "viewed_at": "viewed_at TEXT",
        "downloaded_at": "downloaded_at TEXT",
    })

def _matricula_variants(value: str):
    raw = str(value or "").strip()
    digits = "".join(ch for ch in raw if ch.isdigit())
    variants = set()
    if raw:
        variants.add(raw)
    if digits:
        variants.add(digits)
        variants.add(digits.lstrip("0") or "0")
        for width in (6, 7, 8, 9, 10, 11):
            if len(digits) <= width:
                variants.add(digits.zfill(width))
    return [v for v in variants if v]

def _xlsx_read_shared_strings(zf: zipfile.ZipFile):
    names = set(zf.namelist())
    if "xl/sharedStrings.xml" not in names:
        return []
    ns = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    shared = []
    for si in root.findall("x:si", ns):
        text_nodes = si.findall(".//x:t", ns)
        shared.append("".join((t.text or "") for t in text_nodes))
    return shared

def _xlsx_first_sheet_path(zf: zipfile.ZipFile):
    names = set(zf.namelist())
    preferred = "xl/worksheets/sheet1.xml"
    if preferred in names:
        return preferred
    candidates = sorted(n for n in names if n.startswith("xl/worksheets/") and n.endswith(".xml"))
    return candidates[0] if candidates else None

def _xlsx_cell_value(cell, ns, shared):
    cell_type = cell.attrib.get("t")
    if cell_type == "inlineStr":
        return "".join((t.text or "") for t in cell.findall(".//x:t", ns))
    v = cell.find("x:v", ns)
    if v is None or v.text is None:
        return ""
    if cell_type == "s":
        try:
            idx = int(v.text)
            return shared[idx] if 0 <= idx < len(shared) else ""
        except Exception:
            return ""
    return v.text

def _iter_xlsx_rows_ab(file_path: str):
    ns = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    with zipfile.ZipFile(file_path, "r") as zf:
        sheet_path = _xlsx_first_sheet_path(zf)
        if not sheet_path:
            return
        shared = _xlsx_read_shared_strings(zf)
        root = ET.fromstring(zf.read(sheet_path))
        for row in root.findall(".//x:sheetData/x:row", ns):
            col_a = ""
            col_b = ""
            for cell in row.findall("x:c", ns):
                ref = cell.attrib.get("r", "")
                letters = "".join(ch for ch in ref if ch.isalpha()).upper()
                if letters == "A":
                    col_a = _xlsx_cell_value(cell, ns, shared)
                elif letters == "B":
                    col_b = _xlsx_cell_value(cell, ns, shared)
                if col_a and col_b:
                    break
            yield col_a, col_b

def _load_complement_map_from_sheets():
    global _COMPLEMENT_MAP_CACHE
    mapping = {}
    for complemento, file_path in COMPLEMENT_FILES:
        if not os.path.isfile(file_path):
            continue
        for col_a, _ in _iter_xlsx_rows_ab(file_path):
            keys = _matricula_variants(str(col_a))
            if not keys:
                continue
            for key in keys:
                mapping.setdefault(key, complemento)
    _COMPLEMENT_MAP_CACHE = mapping
    return mapping

def _complemento_for_matricula(matricula: str):
    if not _COMPLEMENT_MAP_CACHE:
        _load_complement_map_from_sheets()
    for key in _matricula_variants(matricula):
        if key in _COMPLEMENT_MAP_CACHE:
            return _COMPLEMENT_MAP_CACHE[key]
    return None

def _sync_user_complements(conn):
    mapping = _load_complement_map_from_sheets()
    if not mapping:
        return
    cur = conn.cursor()
    try:
        cur.execute("SELECT id, matricula FROM users")
        rows = cur.fetchall()
        for row in rows:
            complemento = None
            for key in _matricula_variants(row["matricula"]):
                if key in mapping:
                    complemento = mapping[key]
                    break
            cur.execute("UPDATE users SET complemento=? WHERE id=?", (complemento, row["id"]))
    finally:
        cur.close()

def _backfill_last_login(conn):
    """
    Preenche last_login_at baseado em visualizações/baixados existentes,
    sem sobrescrever valores já registrados.
    """
    conn.execute(
        """
        UPDATE users
        SET last_login_at = (
            SELECT MAX(ts) FROM (
                SELECT MAX(viewed_at) AS ts FROM payslips WHERE user_id=users.id
                UNION ALL
                SELECT MAX(downloaded_at) AS ts FROM payslips WHERE user_id=users.id
            )
        )
        WHERE last_login_at IS NULL
        """
    )

def _norm_name(name: str) -> str:
    name = (name or "").strip()
    if not name:
        return ""
    return re.sub(r"\s+", " ", name).upper()

# =========================
# AUTH HELPERS
# =========================
def current_user_id():
    return session.get("user_id")

def is_admin():
    return bool(session.get("is_admin"))

def login_required(fn):
    from functools import wraps
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not current_user_id():
            return redirect(url_for("login"))
        return fn(*args, **kwargs)
    return wrapper

def admin_required(fn):
    from functools import wraps
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not current_user_id():
            return redirect(url_for("login"))
        if not is_admin():
            return redirect(url_for("dashboard"))
        return fn(*args, **kwargs)
    return wrapper

# =========================
# ROTAS
# =========================
@app.route("/")
def index():
    if current_user_id():
        return redirect(url_for("dashboard"))
    return redirect(url_for("login"))

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        matricula_in = (request.form.get("matricula") or "").strip()
        senha = request.form.get("senha") or ""
        if not matricula_in or not senha:
            flash("Informe matrícula e senha.", "error")
            return render_template("login.html")

        candidates = build_matricula_candidates(matricula_in)

        conn = get_db()
        cur = conn.cursor()

        row = None
        if candidates:
            placeholders = ",".join(["?"] * len(candidates))
            cur.execute(
                f"""
                SELECT id, matricula, password_hash, must_change_password,
                       COALESCE(nome, '') AS nome,
                       COALESCE(is_admin, 0) AS is_admin
                FROM users
                WHERE matricula IN ({placeholders})
                ORDER BY LENGTH(matricula) DESC
                LIMIT 1
                """,
                candidates
            )
            row = cur.fetchone()
        cur.close()
        conn.close()

        if not row:
            flash("Credenciais inválidas.", "error")
            return render_template("login.html")

        uid, matricula_db, pwd_hash, must_change, nome, is_admin_flag = row
        if verify_password(senha, pwd_hash):
            if _is_admin_matricula(matricula_db) and not is_admin_flag:
                conn = get_db()
                cur = conn.cursor()
                cur.execute("UPDATE users SET is_admin=1 WHERE id=?", (uid,))
                conn.commit()
                cur.close()
                conn.close()
                is_admin_flag = 1

            conn = get_db()
            cur = conn.cursor()
            cur.execute("UPDATE users SET last_login_at=? WHERE id=?", (_now_iso(), uid))
            conn.commit()
            cur.close()
            conn.close()

            session["user_id"] = uid
            session["matricula"] = matricula_db
            session["nome"] = nome
            session["is_admin"] = 1 if is_admin_flag else 0
            if must_change:  # 1 = precisa trocar
                return redirect(url_for("change_password"))
            return redirect(url_for("dashboard"))
        else:
            flash("Credenciais inválidas.", "error")

    return render_template("login.html")

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

@app.route("/change_password", methods=["GET", "POST"])
@login_required
def change_password():
    if request.method == "POST":
        pwd1 = request.form.get("newpwd") or ""
        pwd2 = request.form.get("confirmpwd") or ""
        if len(pwd1) < 6:
            flash("A nova senha deve ter pelo menos 6 caracteres.", "error")
            return render_template("change_password.html")

        if pwd1 != pwd2:
            flash("As senhas não conferem.", "error")
            return render_template("change_password.html")

        new_hash = hash_password(pwd1)
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            "UPDATE users SET password_hash=?, must_change_password=0 WHERE id=?",
            (new_hash, current_user_id())
        )
        conn.commit()
        cur.close()
        conn.close()

        flash("Senha alterada com sucesso.", "success")
        return redirect(url_for("dashboard"))

    return render_template("change_password.html")

@app.route("/dashboard")
@login_required
def dashboard():
    uid = current_user_id()
    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT matricula, COALESCE(nome,'') FROM users WHERE id=?", (uid,))
    row = cur.fetchone()
    matricula = row[0] if row else ""
    nome = row[1] or "Nome não cadastrado"

    cur.execute(
        """
        SELECT id, referencia, file_path
        FROM payslips
        WHERE user_id=?
        ORDER BY (referencia IS NULL) ASC, referencia DESC, id DESC
        """,
        (uid,)
    )
    payslips = cur.fetchall()
    cur.close()
    conn.close()

    months = {
        "01": "Janeiro", "02": "Fevereiro", "03": "Março", "04": "Abril",
        "05": "Maio", "06": "Junho", "07": "Julho", "08": "Agosto",
        "09": "Setembro", "10": "Outubro", "11": "Novembro", "12": "Dezembro",
    }

    def format_ref(ref: str) -> str:
        ref = ref or ""
        if ref.startswith("IR-") and len(ref) >= 6:
            return f"IR {ref[3:]}"
        if ref.startswith("13-") and len(ref) >= 6:
            return f"13º {ref[3:]}"
        parts = ref.split("-")
        if len(parts) == 2 and parts[0].isdigit():
            year, month = parts
            return f"{months.get(month, month)} / {year}"
        return ref or "sem-ref"

    normalized = []
    total = 0
    total_ir = 0
    total_13 = 0
    for pid, ref, file_path in payslips:
        abs_path = _normalize_to_abs(file_path)
        total += 1
        if (ref or "").startswith("IR-"):
            total_ir += 1
        elif (ref or "").startswith("13-"):
            total_13 += 1
        normalized.append({
            "id": pid,
            "referencia": ref or "sem-ref",
            "label": format_ref(ref or ""),
            "abs_path": abs_path
        })

    return render_template("dashboard.html",
                           payslips=normalized,
                           total=total,
                           total_ir=total_ir,
                           total_13=total_13,
                           matricula=matricula,
                           nome=nome)

@app.route("/admin")
@admin_required
def admin():
    return render_template("admin.html", matricula=session.get("matricula", ""), nome=session.get("nome", ""))

def _user_owns_path(user_id: int, abs_path: str) -> bool:
    """
    Valida se o arquivo pertence ao usuário logado.
    A validação é feita pelo vínculo no DB (payslips.user_id + file_path).
    """
    try:
        target = Path((abs_path or "").replace("\\", "/")).resolve()
    except Exception:
        return False

    if not target.is_file():
        return False

    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("SELECT file_path FROM payslips WHERE user_id=?", (user_id,))
        for row in cur.fetchall():
            if _normalize_to_abs(row["file_path"]) == str(target):
                return True
        return False
    finally:
        cur.close()
        conn.close()

@app.route("/view")
@login_required
def view_pdf():
    pid = request.args.get("pid", type=int)
    abs_path = request.args.get("path", "")
    abs_path = _normalize_to_abs(abs_path)
    if not _user_owns_path(current_user_id(), abs_path):
        abort(403)
    if pid:
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            "UPDATE payslips SET viewed_at=COALESCE(viewed_at, ?) WHERE id=? AND user_id=?",
            (_now_iso(), pid, current_user_id())
        )
        conn.commit()
        cur.close()
        conn.close()
    return send_file(abs_path, mimetype="application/pdf", as_attachment=False,
                     download_name=os.path.basename(abs_path))

@app.route("/download")
@login_required
def download_pdf():
    pid = request.args.get("pid", type=int)
    abs_path = request.args.get("path", "")
    abs_path = _normalize_to_abs(abs_path)
    if not _user_owns_path(current_user_id(), abs_path):
        abort(403)
    if pid:
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            "UPDATE payslips SET downloaded_at=COALESCE(downloaded_at, ?) WHERE id=? AND user_id=?",
            (_now_iso(), pid, current_user_id())
        )
        conn.commit()
        cur.close()
        conn.close()
    return send_file(abs_path, mimetype="application/pdf", as_attachment=True,
                     download_name=os.path.basename(abs_path))

# =========================
# ADMIN API
# =========================
@app.route("/api/admin/employees", methods=["GET", "POST"])
@admin_required
def api_admin_employees():
    conn = get_db()
    cur = conn.cursor()
    try:
        if request.method == "GET":
            cur.execute(
                """
                SELECT
                    u.id,
                    u.matricula,
                    COALESCE(u.nome, '') AS nome,
                    COALESCE(u.cpf, '') AS cpf,
                    COALESCE(u.department, '') AS department,
                    COALESCE(u.position, '') AS position,
                    COALESCE(u.email, '') AS email,
                    COALESCE(u.complemento, '') AS complemento,
                    u.last_login_at,
                    (SELECT COUNT(*) FROM payslips p WHERE p.user_id=u.id) AS payslip_count,
                    (SELECT referencia FROM payslips p WHERE p.user_id=u.id ORDER BY referencia DESC, id DESC LIMIT 1) AS latest_payslip
                FROM users u
                WHERE COALESCE(u.is_admin, 0) = 0
                ORDER BY COALESCE(u.nome, u.matricula)
                """
            )
            rows = [dict(r) for r in cur.fetchall()]
            return jsonify(ok=True, employees=rows)

        data = request.get_json(silent=True) or request.form
        matricula = (data.get("matricula") or "").strip()
        nome = (data.get("nome") or "").strip()
        password = data.get("password") or ""
        cpf = (data.get("cpf") or "").strip() or None
        department = (data.get("department") or "").strip() or None
        position = (data.get("position") or "").strip() or None
        email = (data.get("email") or "").strip() or None
        complemento = _complemento_for_matricula(matricula)

        if not matricula or not nome or not password:
            return jsonify(ok=False, error="Matrícula, nome e senha são obrigatórios."), 400

        pwd_hash = hash_password(password)
        try:
            cur.execute(
                """
                INSERT INTO users (matricula, nome, cpf, department, position, email, complemento, password_hash, must_change_password, is_admin)
                VALUES (?,?,?,?,?,?,?,?,1,0)
                """,
                (matricula, nome, cpf, department, position, email, complemento, pwd_hash)
            )
            conn.commit()
        except sqlite3.IntegrityError:
            return jsonify(ok=False, error="Matrícula já cadastrada."), 409

        return jsonify(ok=True)
    finally:
        cur.close()
        conn.close()

@app.route("/api/admin/merge", methods=["POST"])
@admin_required
def api_admin_merge():
    data = request.get_json(silent=True) or request.form
    keep_id = data.get("keep_id")
    remove_id = data.get("remove_id")
    try:
        keep_id = int(keep_id)
        remove_id = int(remove_id)
    except (TypeError, ValueError):
        return jsonify(ok=False, error="IDs inválidos."), 400
    if keep_id == remove_id:
        return jsonify(ok=False, error="Escolha contas diferentes para mesclar."), 400

    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id, matricula, COALESCE(nome,'') AS nome, COALESCE(cpf,'') AS cpf,
                   COALESCE(department,'') AS department, COALESCE(position,'') AS position,
                   COALESCE(email,'') AS email, COALESCE(last_login_at,'') AS last_login_at,
                   COALESCE(complemento,'') AS complemento
            FROM users
            WHERE id=? AND COALESCE(is_admin,0)=0
            """,
            (keep_id,)
        )
        keep = cur.fetchone()
        cur.execute(
            """
            SELECT id, matricula, COALESCE(nome,'') AS nome, COALESCE(cpf,'') AS cpf,
                   COALESCE(department,'') AS department, COALESCE(position,'') AS position,
                   COALESCE(email,'') AS email, COALESCE(last_login_at,'') AS last_login_at,
                   COALESCE(complemento,'') AS complemento
            FROM users
            WHERE id=? AND COALESCE(is_admin,0)=0
            """,
            (remove_id,)
        )
        remove = cur.fetchone()

        if not keep or not remove:
            return jsonify(ok=False, error="Conta não encontrada."), 404

        moved = 0
        conflicts = 0
        cur.execute("SELECT id, referencia FROM payslips WHERE user_id=?", (remove_id,))
        for row in cur.fetchall():
            pid = row["id"]
            ref = row["referencia"]
            cur.execute(
                "SELECT id FROM payslips WHERE user_id=? AND referencia=?",
                (keep_id, ref)
            )
            if cur.fetchone():
                cur.execute("DELETE FROM payslips WHERE id=?", (pid,))
                conflicts += 1
            else:
                cur.execute("UPDATE payslips SET user_id=? WHERE id=?", (keep_id, pid))
                moved += 1

        def _pick(first: str, second: str) -> str:
            return first if (first or "").strip() else second

        def _cpf_candidate(row):
            cpf_val = (row["cpf"] or "").strip()
            if cpf_val:
                return cpf_val
            m = (row["matricula"] or "").strip()
            if m.isdigit() and len(m) == 11:
                return m
            return ""

        updates = {}
        updates["nome"] = _pick(keep["nome"], remove["nome"])
        updates["department"] = _pick(keep["department"], remove["department"])
        updates["position"] = _pick(keep["position"], remove["position"])
        updates["email"] = _pick(keep["email"], remove["email"])
        updates["complemento"] = _pick(keep["complemento"], remove["complemento"])

        keep_cpf = _cpf_candidate(keep)
        remove_cpf = _cpf_candidate(remove)
        updates["cpf"] = _pick(keep_cpf, remove_cpf)

        last_keep = (keep["last_login_at"] or "").strip()
        last_remove = (remove["last_login_at"] or "").strip()
        if last_keep and last_remove:
            updates["last_login_at"] = max(last_keep, last_remove)
        elif last_keep or last_remove:
            updates["last_login_at"] = last_keep or last_remove

        cur.execute(
            """
            UPDATE users
            SET nome=?, cpf=?, department=?, position=?, email=?, complemento=?, last_login_at=?
            WHERE id=?
            """,
            (
                updates.get("nome"),
                updates.get("cpf"),
                updates.get("department"),
                updates.get("position"),
                updates.get("email"),
                updates.get("complemento"),
                updates.get("last_login_at"),
                keep_id,
            )
        )

        cur.execute("DELETE FROM users WHERE id=?", (remove_id,))
        conn.commit()
        return jsonify(ok=True, moved=moved, conflicts=conflicts)
    finally:
        cur.close()
        conn.close()
@app.route("/api/admin/employees/<int:user_id>", methods=["PUT"])
@admin_required
def api_admin_employee_update(user_id):
    data = request.get_json(silent=True) or request.form
    nome = (data.get("nome") or "").strip()
    cpf = (data.get("cpf") or "").strip() or None
    department = (data.get("department") or "").strip() or None
    position = (data.get("position") or "").strip() or None
    email = (data.get("email") or "").strip() or None
    password = (data.get("password") or "").strip()

    if not nome:
        return jsonify(ok=False, error="Nome é obrigatório."), 400

    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE users
            SET nome=?, cpf=?, department=?, position=?, email=?
            WHERE id=? AND COALESCE(is_admin, 0)=0
            """,
            (nome, cpf, department, position, email, user_id)
        )
        if password:
            cur.execute(
                "UPDATE users SET password_hash=?, must_change_password=1 WHERE id=?",
                (hash_password(password), user_id)
            )
        conn.commit()
        return jsonify(ok=True)
    finally:
        cur.close()
        conn.close()

@app.route("/api/admin/employees/<int:user_id>/reset-password", methods=["POST"])
@admin_required
def api_admin_employee_reset_password(user_id):
    data = request.get_json(silent=True) or request.form
    password = (data.get("password") or "").strip()
    if len(password) < 6:
        return jsonify(ok=False, error="A nova senha deve ter pelo menos 6 caracteres."), 400

    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id FROM users WHERE id=? AND COALESCE(is_admin,0)=0", (user_id,))
        if not cur.fetchone():
            return jsonify(ok=False, error="Usuário não encontrado."), 404
        cur.execute(
            "UPDATE users SET password_hash=?, must_change_password=1 WHERE id=?",
            (hash_password(password), user_id)
        )
        conn.commit()
        return jsonify(ok=True)
    finally:
        cur.close()
        conn.close()

@app.route("/api/admin/duplicates", methods=["GET"])
@admin_required
def api_admin_duplicates():
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                u.id,
                u.matricula,
                COALESCE(u.nome, '') AS nome,
                COALESCE(u.cpf, '') AS cpf,
                COALESCE(u.last_login_at, '') AS last_login_at,
                (SELECT COUNT(*) FROM payslips p WHERE p.user_id=u.id) AS payslip_count
            FROM users u
            WHERE COALESCE(u.is_admin, 0) = 0
            """
        )
        rows = [dict(r) for r in cur.fetchall()]

        groups = {}
        for r in rows:
            key = _norm_name(r.get("nome") or "")
            if not key:
                continue
            groups.setdefault(key, []).append(r)

        results = []
        for key, users in groups.items():
            if len(users) < 2:
                continue
            users_sorted = sorted(
                users,
                key=lambda u: (
                    u.get("payslip_count") or 0,
                    u.get("last_login_at") or "",
                ),
                reverse=True
            )
            display_name = users_sorted[0].get("nome") or key.title()
            results.append({
                "name": display_name,
                "norm": key,
                "users": users_sorted
            })

        results.sort(key=lambda g: g["norm"])
        return jsonify(ok=True, groups=results)
    finally:
        cur.close()
        conn.close()

@app.route("/api/admin/payslips", methods=["GET"])
@admin_required
def api_admin_payslips():
    user_id = request.args.get("user_id", type=int)
    if not user_id:
        return jsonify(ok=False, error="user_id obrigatório."), 400

    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id, referencia, viewed_at, downloaded_at, issued_by_admin, COALESCE(file_name, '') AS file_name
            FROM payslips
            WHERE user_id=?
            ORDER BY referencia DESC, id DESC
            """,
            (user_id,)
        )
        rows = [dict(r) for r in cur.fetchall()]
        return jsonify(ok=True, payslips=rows)
    finally:
        cur.close()
        conn.close()

@app.route("/admin/payslips/download")
@admin_required
def admin_download_payslip():
    pid = request.args.get("pid", type=int)
    if not pid:
        abort(400)

    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT p.file_path, COALESCE(p.file_name, '') AS file_name
            FROM payslips p
            WHERE p.id=?
            """,
            (pid,)
        )
        row = cur.fetchone()
        if not row:
            abort(404)

        abs_path = _normalize_to_abs(row["file_path"])
        if not os.path.isfile(abs_path):
            abort(404)

        download_name = row["file_name"] or os.path.basename(abs_path)
        return send_file(abs_path, mimetype="application/pdf", as_attachment=True, download_name=download_name)
    finally:
        cur.close()
        conn.close()

def _zip_add_unique(zf: zipfile.ZipFile, filepath: str, arcname: str, used_names: set):
    name = arcname
    if name in used_names:
        base, ext = os.path.splitext(arcname)
        k = 2
        while True:
            name = f"{base}__{k}{ext}"
            if name not in used_names:
                break
            k += 1
    used_names.add(name)
    zf.write(filepath, arcname=name)

@app.route("/admin/payslips/download-zip", methods=["POST"])
@admin_required
def admin_download_payslips_zip():
    data = request.get_json(silent=True) or request.form
    try:
        user_id = int(data.get("user_id") or 0)
    except (TypeError, ValueError):
        user_id = 0

    ids = data.get("ids") or []
    if isinstance(ids, str):
        ids = [i for i in ids.split(",") if i.strip()]

    try:
        ids = [int(i) for i in ids]
    except (TypeError, ValueError):
        ids = []

    if not user_id or not ids:
        return jsonify(ok=False, error="Selecione os documentos."), 400

    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("SELECT matricula FROM users WHERE id=? AND COALESCE(is_admin,0)=0", (user_id,))
        user_row = cur.fetchone()
        if not user_row:
            return jsonify(ok=False, error="Usuário não encontrado."), 404

        placeholders = ",".join(["?"] * len(ids))
        cur.execute(
            f"""
            SELECT id, referencia, file_path, COALESCE(file_name, '') AS file_name
            FROM payslips
            WHERE user_id=? AND id IN ({placeholders})
            ORDER BY referencia DESC, id DESC
            """,
            (user_id, *ids)
        )
        rows = cur.fetchall()
        if not rows:
            return jsonify(ok=False, error="Nenhum documento encontrado."), 404

        mem = io.BytesIO()
        used = set()
        with zipfile.ZipFile(mem, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for r in rows:
                abs_path = _normalize_to_abs(r["file_path"])
                if not os.path.isfile(abs_path):
                    continue
                ref = (r["referencia"] or "").strip() or "documento"
                ext = os.path.splitext(abs_path)[1] or ".pdf"
                arc = r["file_name"] or f"{ref}{ext}"
                _zip_add_unique(zf, abs_path, arc, used)

        if not used:
            return jsonify(ok=False, error="Arquivos não encontrados no disco."), 404

        mem.seek(0)
        zip_name = f"contracheques_{user_row['matricula']}.zip"
        return send_file(mem, mimetype="application/zip", as_attachment=True, download_name=zip_name)
    finally:
        cur.close()
        conn.close()

@app.route("/admin/payslips/download-zip-bulk", methods=["POST"])
@admin_required
def admin_download_payslips_zip_bulk():
    data = request.get_json(silent=True) or request.form
    doc_type = (data.get("doc_type") or "monthly").strip().lower()
    ref_month = (data.get("ref_month") or "").strip()
    ref_year = (data.get("ref_year") or "").strip()

    if not ref_year.isdigit() or len(ref_year) != 4:
        return jsonify(ok=False, error="Ano inválido."), 400
    if doc_type not in ("monthly", "13", "ir"):
        return jsonify(ok=False, error="Tipo inválido."), 400

    referencia = ""
    if doc_type == "monthly":
        if not ref_month.isdigit() or not (1 <= int(ref_month) <= 12):
            return jsonify(ok=False, error="Mês inválido."), 400
        ref_month = ref_month.zfill(2)
        referencia = f"{ref_year}-{ref_month}"
    elif doc_type == "13":
        referencia = f"13-{ref_year}"
    elif doc_type == "ir":
        referencia = f"IR-{ref_year}"

    user_ids = data.get("user_ids") or data.get("ids") or []
    if isinstance(user_ids, str):
        user_ids = [i for i in user_ids.split(",") if i.strip()]
    try:
        user_ids = [int(i) for i in user_ids]
    except (TypeError, ValueError):
        user_ids = []

    if not user_ids:
        return jsonify(ok=False, error="Selecione os funcionários."), 400

    conn = get_db()
    cur = conn.cursor()
    try:
        placeholders = ",".join(["?"] * len(user_ids))
        cur.execute(
            f"""
            SELECT p.file_path,
                   p.referencia,
                   COALESCE(p.file_name, '') AS file_name,
                   u.matricula,
                   COALESCE(u.nome, '') AS nome
            FROM payslips p
            JOIN users u ON u.id = p.user_id
            WHERE p.referencia=? AND p.user_id IN ({placeholders})
            ORDER BY u.matricula
            """,
            (referencia, *user_ids)
        )
        rows = cur.fetchall()
        if not rows:
            return jsonify(ok=False, error="Nenhum documento encontrado."), 404

        mem = io.BytesIO()
        used = set()
        with zipfile.ZipFile(mem, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for r in rows:
                abs_path = _normalize_to_abs(r["file_path"])
                if not os.path.isfile(abs_path):
                    continue
                base_name = r["file_name"] or f"{r['referencia']}.pdf"
                arc = f"{r['matricula']}_{base_name}"
                _zip_add_unique(zf, abs_path, arc, used)

        if not used:
            return jsonify(ok=False, error="Arquivos não encontrados no disco."), 404

        mem.seek(0)
        zip_name = f"contracheques_{referencia}.zip"
        return send_file(mem, mimetype="application/zip", as_attachment=True, download_name=zip_name)
    finally:
        cur.close()
        conn.close()

@app.route("/api/admin/payslips/upload", methods=["POST"])
@admin_required
def api_admin_payslips_upload():
    try:
        user_id = int(request.form.get("user_id", ""))
    except ValueError:
        user_id = None
    doc_type = (request.form.get("doc_type") or "monthly").strip().lower()
    ref_month = (request.form.get("ref_month") or "").strip()
    ref_year = (request.form.get("ref_year") or "").strip()
    upload = request.files.get("file")

    if not user_id:
        return jsonify(ok=False, error="Informe o usuário."), 400
    if doc_type not in ("monthly", "13", "ir"):
        return jsonify(ok=False, error="Tipo inválido."), 400
    if not ref_year:
        return jsonify(ok=False, error="Informe o ano."), 400
    if not upload or not upload.filename:
        return jsonify(ok=False, error="Selecione um arquivo PDF."), 400

    filename = upload.filename
    if not filename.lower().endswith(".pdf"):
        return jsonify(ok=False, error="Somente arquivos PDF são permitidos."), 400

    if not ref_year.isdigit() or len(ref_year) != 4:
        return jsonify(ok=False, error="Ano inválido."), 400

    referencia = ""
    if doc_type == "monthly":
        if not ref_month.isdigit() or not (1 <= int(ref_month) <= 12):
            return jsonify(ok=False, error="Mês inválido."), 400
        ref_month = ref_month.zfill(2)
        referencia = f"{ref_year}-{ref_month}"
    elif doc_type == "13":
        referencia = f"13-{ref_year}"
    elif doc_type == "ir":
        referencia = f"IR-{ref_year}"

    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("SELECT matricula FROM users WHERE id=? AND COALESCE(is_admin,0)=0", (user_id,))
        row = cur.fetchone()
        if not row:
            return jsonify(ok=False, error="Usuário não encontrado."), 404

        matricula = str(row["matricula"])
        dest_dir = Path(STORAGE_DIR) / matricula
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_path = dest_dir / f"{referencia}.pdf"
        upload.save(str(dest_path))

        cur.execute(
            """
            INSERT INTO payslips (user_id, referencia, file_path, file_name, issued_by_admin)
            VALUES (?,?,?,?,1)
            ON CONFLICT(user_id, referencia) DO UPDATE SET
                file_path=excluded.file_path,
                file_name=excluded.file_name,
                issued_by_admin=1,
                viewed_at=NULL,
                downloaded_at=NULL
            """,
            (user_id, referencia, str(dest_path), filename)
        )
        conn.commit()
        return jsonify(ok=True)
    finally:
        cur.close()
        conn.close()

# =========================
# HUMANA DECLARATION
# =========================
_HUMANA_DATA: dict = {}
_HUMANA_ARQUIVO: str = ""
_MONTHS_HUMANA = [f"{str(i).zfill(2)}/2025" for i in range(1, 13)]
_HUMANA_HEADER = "img/cabecalho_agespisa.png"
_HUMANA_SIGNATURE = "img/assinatura_fabricio.png"


def _hum_normalize(value: str) -> str:
    value = str(value or "").strip().upper()
    return re.sub(r"\s+", " ", value)


def _hum_parse_money(value) -> float:
    if pd.isna(value):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(".", "").replace(",", ".")
    text = re.sub(r"[^\d\.-]", "", text)
    try:
        return float(text)
    except ValueError:
        return 0.0


def _hum_parse_date(value):
    if pd.isna(value):
        return None
    if isinstance(value, datetime):
        return value
    if hasattr(value, "to_pydatetime"):
        return value.to_pydatetime()
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    parsed = pd.to_datetime(value, dayfirst=True, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.to_pydatetime()


def _hum_format_money(value: float) -> str:
    return f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _hum_format_cpf(value: str) -> str:
    digits = re.sub(r"\D", "", str(value or ""))[:11]
    if len(digits) <= 3:
        return digits
    if len(digits) <= 6:
        return f"{digits[:3]}.{digits[3:]}"
    if len(digits) <= 9:
        return f"{digits[:3]}.{digits[3:6]}.{digits[6:]}"
    return f"{digits[:3]}.{digits[3:6]}.{digits[6:9]}-{digits[9:11]}"


def _hum_build_data(df: pd.DataFrame) -> dict:
    grouped: dict = {}
    for _, row in df.iterrows():
        try:
            name = row.iloc[1]
            amount_raw = row.iloc[6]
            date_raw = row.iloc[4]
        except IndexError:
            continue
        if pd.isna(name):
            continue
        name = str(name).strip()
        normalized = _hum_normalize(name)
        if not normalized:
            continue
        amount = _hum_parse_money(amount_raw)
        payment_date = _hum_parse_date(date_raw)
        if not payment_date:
            continue
        if payment_date.year != 2025:
            continue
        month_key = payment_date.strftime("%m/%Y")
        if month_key not in _MONTHS_HUMANA:
            continue
        if normalized not in grouped:
            grouped[normalized] = {"name": name, "payments": defaultdict(float)}
        grouped[normalized]["payments"][month_key] += amount

    result = {}
    for person in grouped.values():
        payments = {m: round(person["payments"].get(m, 0.0), 2) for m in _MONTHS_HUMANA}
        result[person["name"]] = {"name": person["name"], "payments": payments}
    return dict(sorted(result.items()))


def _hum_get_table(name: str):
    person = _HUMANA_DATA.get(name)
    if not person:
        return None, [], 0.0
    rows = []
    total = 0.0
    for month in _MONTHS_HUMANA:
        value = float(person["payments"].get(month, 0.0))
        if value > 0:
            total += value
            rows.append((month, _hum_format_money(value)))
    return person, rows, total


@app.route("/humana/upload", methods=["POST"])
@admin_required
def humana_upload():
    global _HUMANA_DATA, _HUMANA_ARQUIVO
    file = request.files.get("arquivo")
    if not file or not file.filename:
        return jsonify(ok=False, error="Selecione um arquivo Excel ou CSV."), 400
    filename = file.filename.lower()
    try:
        if filename.endswith(".csv"):
            df = pd.read_csv(file, header=None)
        else:
            df = pd.read_excel(file, header=None)
    except Exception as exc:
        return jsonify(ok=False, error=f"Erro ao ler a planilha: {exc}"), 400
    _HUMANA_DATA = _hum_build_data(df)
    _HUMANA_ARQUIVO = file.filename
    if not _HUMANA_DATA:
        return jsonify(ok=False, error="Nenhum registro válido de 2025 foi encontrado."), 400
    return jsonify(ok=True, names=list(_HUMANA_DATA.keys()), arquivo=file.filename)


@app.route("/humana/person", methods=["GET"])
@admin_required
def humana_person():
    name = request.args.get("nome", "").strip()
    person, table_rows, total = _hum_get_table(name)
    if not person:
        return jsonify(ok=False, error="Pessoa não encontrada."), 404
    return jsonify(ok=True, name=name, table_rows=table_rows, total=_hum_format_money(total))


@app.route("/humana/print", methods=["GET"])
@admin_required
def humana_print():
    name = request.args.get("nome", "").strip()
    cpf = _hum_format_cpf(request.args.get("cpf", "").strip())
    person, table_rows, _ = _hum_get_table(name)
    if not person:
        return "Pessoa não encontrada. Volte e selecione um nome válido.", 400
    return render_template(
        "humana_print.html",
        selected_name=name,
        cpf=cpf or "<cpf da pessoa>",
        table_rows=table_rows,
        header_url=url_for("static", filename=_HUMANA_HEADER, _external=True),
        signature_url=url_for("static", filename=_HUMANA_SIGNATURE, _external=True),
    )


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    init_db()
    os.makedirs(STORAGE_DIR, exist_ok=True)
    app.run(host="0.0.0.0", port=5000, debug=True)

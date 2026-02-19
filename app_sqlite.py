# app_sqlite.py
import os
import bcrypt
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from flask import (
    Flask, render_template, request, redirect, url_for,
    session, send_file, abort, flash, jsonify
)
from werkzeug.middleware.proxy_fix import ProxyFix
from dotenv import load_dotenv; load_dotenv()

# =========================
# CONFIGURAÇÕES
# =========================
DB_PATH = os.getenv("DB_PATH", os.path.abspath("contracheque.db"))
SECRET_KEY = os.getenv("SECRET_KEY", "troque-esta-secret-key-em-producao")
STORAGE_DIR = os.getenv("STORAGE_DIR", os.path.abspath("contracheques_split"))
ADMIN_MATRICULAS = os.getenv("ADMIN_MATRICULAS", "")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")

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
        bootstrap_admins(conn)
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
        "is_admin": "is_admin INTEGER NOT NULL DEFAULT 0",
    })
    ensure("payslips", {
        "file_name": "file_name TEXT",
        "issued_by_admin": "issued_by_admin INTEGER NOT NULL DEFAULT 0",
        "viewed_at": "viewed_at TEXT",
        "downloaded_at": "downloaded_at TEXT",
    })

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

    normalized = []
    for pid, ref, file_path in payslips:
        abs_path = _normalize_to_abs(file_path)
        normalized.append({
            "id": pid,
            "referencia": ref or "sem-ref",
            "abs_path": abs_path
        })

    return render_template("dashboard.html",
                           payslips=normalized,
                           matricula=matricula,
                           nome=nome)

@app.route("/admin")
@admin_required
def admin():
    return render_template("admin.html", matricula=session.get("matricula", ""), nome=session.get("nome", ""))

def _user_owns_path(user_id: int, abs_path: str) -> bool:
    """
    Valida se o arquivo solicitado pertence ao usuário logado.
    Regras:
      1) Caminho normalizado, absoluto e existente
      2) Debaixo de STORAGE_DIR/<matricula>/
    """
    # matrícula do usuário
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT matricula FROM users WHERE id=?", (user_id,))
    row = cur.fetchone()
    cur.close(); conn.close()
    if not row:
        return False
    matricula = str(row[0])

    # base do usuário e alvo normalizados
    base = (Path(STORAGE_DIR) / matricula).resolve()
    try:
        target = Path((abs_path or "").replace("\\", "/")).resolve()
    except Exception:
        return False

    if not target.is_file():
        return False

    # garante que target está dentro de base (sem traversal)
    try:
        target.relative_to(base)
    except ValueError:
        return False

    return True

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

        if not matricula or not nome or not password:
            return jsonify(ok=False, error="Matrícula, nome e senha são obrigatórios."), 400

        pwd_hash = hash_password(password)
        try:
            cur.execute(
                """
                INSERT INTO users (matricula, nome, cpf, department, position, email, password_hash, must_change_password, is_admin)
                VALUES (?,?,?,?,?,?,?,1,0)
                """,
                (matricula, nome, cpf, department, position, email, pwd_hash)
            )
            conn.commit()
        except sqlite3.IntegrityError:
            return jsonify(ok=False, error="Matrícula já cadastrada."), 409

        return jsonify(ok=True)
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
            SELECT id, referencia, viewed_at, downloaded_at, issued_by_admin
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

@app.route("/api/admin/payslips/upload", methods=["POST"])
@admin_required
def api_admin_payslips_upload():
    try:
        user_id = int(request.form.get("user_id", ""))
    except ValueError:
        user_id = None
    ref_month = (request.form.get("ref_month") or "").strip()
    ref_year = (request.form.get("ref_year") or "").strip()
    upload = request.files.get("file")

    if not user_id or not ref_month or not ref_year:
        return jsonify(ok=False, error="Informe usuário, mês e ano."), 400
    if not upload or not upload.filename:
        return jsonify(ok=False, error="Selecione um arquivo PDF."), 400

    filename = upload.filename
    if not filename.lower().endswith(".pdf"):
        return jsonify(ok=False, error="Somente arquivos PDF são permitidos."), 400

    if not ref_year.isdigit() or len(ref_year) != 4:
        return jsonify(ok=False, error="Ano inválido."), 400
    if not ref_month.isdigit() or not (1 <= int(ref_month) <= 12):
        return jsonify(ok=False, error="Mês inválido."), 400

    ref_month = ref_month.zfill(2)
    referencia = f"{ref_year}-{ref_month}"

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
# MAIN
# =========================
if __name__ == "__main__":
    init_db()
    os.makedirs(STORAGE_DIR, exist_ok=True)
    app.run(host="0.0.0.0", port=5000, debug=True)

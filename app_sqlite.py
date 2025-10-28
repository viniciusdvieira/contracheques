import os
import bcrypt
import sqlite3
from flask import (
    Flask, render_template, request, redirect, url_for,
    session, send_file, abort, flash
)
from werkzeug.middleware.proxy_fix import ProxyFix
from dotenv import load_dotenv; load_dotenv()

# =========================
# CONFIGURAÇÕES
# =========================
DB_PATH = os.getenv("DB_PATH", os.path.abspath("contracheque.db"))
SECRET_KEY = os.getenv("SECRET_KEY", "troque-esta-secret-key-em-producao")
# Pasta onde os PDFs foram gerados pelo seu script
STORAGE_DIR = os.getenv("STORAGE_DIR", os.path.abspath("contracheques_split"))

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
    UNIQUE(user_id, referencia),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);
"""

def get_db():
    # Uma conexão por request é suficiente aqui (app simples)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    # Garantias e performance
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True) if os.path.dirname(DB_PATH) else None
    conn = get_db()
    try:
        conn.executescript(DDL_USERS + DDL_PAYSLIPS)
        conn.commit()
    finally:
        conn.close()


# =========================
# HELPERS
# =========================
def build_matricula_candidates(user_input: str):
    """
    Gera candidatos para buscar no DB:
      - exatamente o que o usuário digitou (pode ser '00', '1234-5', etc.)
      - só dígitos
      - dígitos zero-padded para 6, 7 e 8 posições (cobre padrões de PDFs)
    """
    s = (user_input or "").strip()
    cands = {s}  # tenta exatamente como foi digitado (ex.: '00')
    digits = "".join(ch for ch in s if ch.isdigit())
    if digits:
        cands.add(digits)  # ex.: '12345'
        for width in (6, 7, 8):
            if len(digits) < width:
                cands.add(digits.zfill(width))  # ex.: '00012345'
    return list(cands)

# =========================
# BCRYPT
# =========================
def hash_password(plain: str) -> str:
    # bcrypt limita a 72 bytes
    return bcrypt.hashpw(plain.encode("utf-8")[:72], bcrypt.gensalt()).decode("utf-8")

def verify_password(plain: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(plain.encode("utf-8")[:72], hashed.encode("utf-8"))
    except Exception:
        return False

# =========================
# AUTH HELPERS
# =========================
def current_user_id():
    return session.get("user_id")

def login_required(fn):
    from functools import wraps
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not current_user_id():
            return redirect(url_for("login"))
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
                SELECT id, matricula, password_hash, must_change_password, COALESCE(nome, '') AS nome
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

        uid, matricula_db, pwd_hash, must_change, nome = row
        if verify_password(senha, pwd_hash):
            session["user_id"] = uid
            session["matricula"] = matricula_db
            session["nome"] = nome
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

    # pega nome e matrícula (exibir no topo)
    cur.execute("SELECT matricula, COALESCE(nome,'') FROM users WHERE id=?", (uid,))
    row = cur.fetchone()
    matricula = row[0] if row else ""
    nome = row[1] or "Nome não cadastrado"

    # Em SQLite não há NULLS LAST. Estratégia:
    # 1) ordenar por (referencia IS NULL) ASC (False=0 vem antes de True=1)
    # 2) depois por referencia DESC
    # 3) fallback por id DESC
    cur.execute(
        """
        SELECT referencia, file_path
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
    for ref, file_path in payslips:
        abs_path = os.path.abspath(file_path) if os.path.isabs(file_path) \
                   else os.path.abspath(os.path.join(os.getcwd(), file_path))
        normalized.append({
            "referencia": ref or "sem-ref",
            "abs_path": abs_path
        })

    return render_template("dashboard.html",
                           payslips=normalized,
                           matricula=matricula,
                           nome=nome)

def _user_owns_path(user_id: int, abs_path: str) -> bool:
    """
    Valida se o arquivo solicitado pertence ao usuário logado.
    Regra: o path do arquivo deve estar dentro de STORAGE_DIR/<matricula>/
    """
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT matricula FROM users WHERE id=?", (user_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row:
        return False
    matricula = row[0]

    user_base = os.path.join(STORAGE_DIR, str(matricula))
    user_base = os.path.abspath(user_base)
    try:
        target = os.path.abspath(abs_path)
    except Exception:
        return False

    # dentro da pasta do usuário + arquivo existe
    return os.path.commonpath([target, user_base]) == user_base and os.path.exists(target)

@app.route("/view")
@login_required
def view_pdf():
    abs_path = request.args.get("path", "")
    if not _user_owns_path(current_user_id(), abs_path):
        abort(403)
    return send_file(abs_path, mimetype="application/pdf", as_attachment=False,
                     download_name=os.path.basename(abs_path))

@app.route("/download")
@login_required
def download_pdf():
    abs_path = request.args.get("path", "")
    if not _user_owns_path(current_user_id(), abs_path):
        abort(403)
    return send_file(abs_path, mimetype="application/pdf", as_attachment=True,
                     download_name=os.path.basename(abs_path))

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    # debug local
    init_db()
    os.makedirs(STORAGE_DIR, exist_ok=True)
    app.run(host="0.0.0.0", port=5000, debug=True)

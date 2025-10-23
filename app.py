import os
import sqlite3

import bcrypt
from flask import (
    Flask, render_template, request, redirect, url_for,
    session, send_file, abort, flash
)
from werkzeug.middleware.proxy_fix import ProxyFix
from dotenv import load_dotenv; load_dotenv()


# =========================
# CONFIGURAÇÕES
# =========================
# Pegue do ambiente (recomendado) ou use valor padrão local
def _resolve_sqlite_target(raw: str):
    """Converte DATABASE_URL em um caminho aceitável pelo sqlite3."""
    if not raw:
        raw = os.path.abspath("contracheques.db")

    if raw.startswith("sqlite:///"):
        raw = raw.replace("sqlite:///", "", 1)

    if raw.startswith("file:"):
        return raw, True

    if not os.path.isabs(raw):
        raw = os.path.abspath(raw)

    return raw, False


DATABASE_URL = os.getenv("DATABASE_URL", os.path.abspath("contracheques.db"))
DATABASE_PATH, DATABASE_IS_URI = _resolve_sqlite_target(DATABASE_URL)
SECRET_KEY = os.getenv("SECRET_KEY", "troque-esta-secret-key-em-producao")
# Pasta onde os PDFs foram gerados pelo seu script
STORAGE_DIR = os.getenv("STORAGE_DIR", os.path.abspath("contracheques_split"))

# =========================
# APP
# =========================
app = Flask(__name__, template_folder="templates", static_folder="static")
app.secret_key = SECRET_KEY

# Se colocar atrás de nginx/traefik, ajuda a corrigir scheme/host
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_port=1, x_host=1)

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

def get_db():
    # Uma conexão por request é suficiente aqui (simples)
    conn = sqlite3.connect(
        DATABASE_PATH,
        detect_types=sqlite3.PARSE_DECLTYPES,
        uri=DATABASE_IS_URI,
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

# =========================
# BCRYPT HELPERS
# =========================
def hash_password(plain: str) -> str:
    # bcrypt limita a 72 bytes — aqui é curto, mas garantimos.
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
        # Tenta casar com qualquer um dos candidatos
        placeholders = ",".join(["?"] * len(candidates))
        query = f"""
            SELECT id, matricula, password_hash, must_change_password, COALESCE(nome, '') AS nome
            FROM users
            WHERE matricula IN ({placeholders})
            LIMIT 1
        """
        cur.execute(query, candidates)
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
            if must_change:
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

    cur.execute(
        """
        SELECT referencia, file_path
        FROM payslips
        WHERE user_id=?
        ORDER BY referencia IS NULL, referencia DESC, id DESC
        """,
        (uid,)
    )
    payslips = cur.fetchall()
    cur.close()
    conn.close()

    normalized = []
    for ref, file_path in payslips:
        if not os.path.isabs(file_path):
            abs_path = os.path.abspath(os.path.join(os.getcwd(), file_path))
        else:
            abs_path = file_path
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
    # Descobre a matrícula do usuário
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT matricula FROM users WHERE id=?", (user_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row:
        return False
    matricula = row[0]

    # Base esperada
    user_base = os.path.join(STORAGE_DIR, str(matricula))
    user_base = os.path.abspath(user_base)
    try:
        target = os.path.abspath(abs_path)
    except Exception:
        return False

    # Verifica se 'target' está sob 'user_base'
    # (impede path traversal e acesso fora da pasta do usuário)
    return os.path.commonpath([target, user_base]) == user_base and os.path.exists(target)

@app.route("/view")
@login_required
def view_pdf():
    abs_path = request.args.get("path", "")
    if not _user_owns_path(current_user_id(), abs_path):
        abort(403)
    # envia inline
    return send_file(abs_path, mimetype="application/pdf", as_attachment=False, download_name=os.path.basename(abs_path))

@app.route("/download")
@login_required
def download_pdf():
    abs_path = request.args.get("path", "")
    if not _user_owns_path(current_user_id(), abs_path):
        abort(403)
    # envia como download
    return send_file(abs_path, mimetype="application/pdf", as_attachment=True, download_name=os.path.basename(abs_path))

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    # debug local
    app.run(host="0.0.0.0", port=5000, debug=True)

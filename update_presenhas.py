import os
import re
import sqlite3

import bcrypt
from dotenv import load_dotenv

load_dotenv()

DB_TARGET = os.getenv("DATABASE_URL", os.path.abspath("contracheques.db"))


def _resolve_sqlite_target(raw: str):
    if not raw:
        raw = os.path.abspath("contracheques.db")
    if raw.startswith("sqlite:///"):
        raw = raw.replace("sqlite:///", "", 1)
    if raw.startswith("file:"):
        return raw, True
    if not os.path.isabs(raw):
        raw = os.path.abspath(raw)
    return raw, False


DB_PATH, DB_URI = _resolve_sqlite_target(DB_TARGET)

def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode("utf-8")[:72], bcrypt.gensalt()).decode("utf-8")

def strip_leading_zeros(matricula: str) -> str:
    if not matricula:
        return ""
    # Remove apenas zeros à esquerda, mantendo zeros internos (ex: 1002 -> 1002)
    s = matricula.lstrip("0")
    return s if s else "0"

def main():
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES, uri=DB_URI)
    conn.execute("PRAGMA foreign_keys = ON;")
    cur = conn.cursor()
    cur.execute("SELECT id, matricula FROM users ORDER BY id;")
    rows = cur.fetchall()

    total = 0
    for uid, matricula in rows:
        nova_mat = strip_leading_zeros(re.sub(r"\D", "", matricula)) or matricula
        nova_senha = f"agespisa{nova_mat}"
        hash_ = hash_password(nova_senha)
        cur.execute(
            "UPDATE users SET password_hash=?, must_change_password=1 WHERE id=?;",
            (hash_, uid)
        )
        total += 1
        print(f"Atualizado ID={uid} | Matricula={matricula} -> Senha: agespisa{nova_mat}")

    conn.commit()
    cur.close()
    conn.close()
    print(f"\n✅ Total de {total} senhas reinicializadas com sucesso.")

if __name__ == "__main__":
    main()

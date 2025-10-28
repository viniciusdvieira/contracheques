import os
import re
import sqlite3
import bcrypt

# Caminho do banco SQLite (mesmo do app)
DB_PATH = os.getenv("DB_PATH", os.path.abspath("contracheque.db"))

def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode("utf-8")[:72], bcrypt.gensalt()).decode("utf-8")

def strip_leading_zeros(matricula: str) -> str:
    if not matricula:
        return ""
    # Remove apenas zeros à esquerda, mantendo zeros internos (ex: 1002 -> 1002)
    s = matricula.lstrip("0")
    return s if s else "0"

def main():
    if not os.path.exists(DB_PATH):
        print(f"❌ Banco de dados não encontrado: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # busca todos os usuários
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
        print(f"Atualizado ID={uid} | Matrícula={matricula} -> Senha: agespisa{nova_mat}")

    conn.commit()
    cur.close()
    conn.close()

    print(f"\n✅ Total de {total} senhas reinicializadas com sucesso.")

if __name__ == "__main__":
    main()

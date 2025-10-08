import re
import psycopg2
import bcrypt

DB_DSN = "postgresql://contracheque_admin:admin_pass_alterar@localhost:5432/contracheque_db"

def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode("utf-8")[:72], bcrypt.gensalt()).decode("utf-8")

def strip_leading_zeros(matricula: str) -> str:
    if not matricula:
        return ""
    # Remove apenas zeros à esquerda, mantendo zeros internos (ex: 1002 -> 1002)
    s = matricula.lstrip("0")
    return s if s else "0"

def main():
    conn = psycopg2.connect(DB_DSN)
    cur = conn.cursor()
    cur.execute("SELECT id, matricula FROM users ORDER BY id;")
    rows = cur.fetchall()

    total = 0
    for uid, matricula in rows:
        nova_mat = strip_leading_zeros(re.sub(r"\D", "", matricula)) or matricula
        nova_senha = f"agespisa{nova_mat}"
        hash_ = hash_password(nova_senha)
        cur.execute(
            "UPDATE users SET password_hash=%s, must_change_password=true WHERE id=%s;",
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

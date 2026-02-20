import os
import shutil
import sqlite3

OUTPUT_DIR = r"contracheques_split"
DB_PATH    = r"contracheque.db"

def is_digits(s: str) -> bool:
    return s.isdigit()

def is_cpf_folder(name: str) -> bool:
    return is_digits(name) and len(name) == 11

def is_matricula_folder(name: str) -> bool:
    # matrícula "curta" ou já padronizada (até 7)
    return is_digits(name) and 1 <= len(name) <= 7

def canonical_login(name: str) -> str:
    # CPF fica igual; matrícula vira 7 dígitos
    if is_cpf_folder(name):
        return name
    if is_matricula_folder(name):
        return name.zfill(7)
    return name

def move_merge_folder(src: str, dst: str):
    os.makedirs(dst, exist_ok=True)

    for root, dirs, files in os.walk(src):
        rel = os.path.relpath(root, src)
        dst_root = dst if rel == "." else os.path.join(dst, rel)
        os.makedirs(dst_root, exist_ok=True)

        for fn in files:
            s = os.path.join(root, fn)
            d = os.path.join(dst_root, fn)

            # Se já existir no destino, tenta resolver conflito
            if os.path.exists(d):
                base, ext = os.path.splitext(fn)
                k = 1
                while True:
                    d2 = os.path.join(dst_root, f"{base}__dup{k}{ext}")
                    if not os.path.exists(d2):
                        d = d2
                        break
                    k += 1

            shutil.move(s, d)

    # Se sobrou vazio, remove
    try:
        shutil.rmtree(src)
    except Exception:
        pass

def update_db_paths(conn: sqlite3.Connection, old_prefix: str, new_prefix: str):
    # old_prefix/new_prefix: por exemplo "contracheques_split\\10143\\"
    cur = conn.cursor()

    # Atualiza só onde começa com old_prefix
    cur.execute(
        """
        UPDATE payslips
        SET file_path = REPLACE(file_path, ?, ?)
        WHERE file_path LIKE ?
        """,
        (old_prefix, new_prefix, old_prefix + "%")
    )
    return cur.rowcount

def main():
    if not os.path.isdir(OUTPUT_DIR):
        raise SystemExit(f"Diretório não encontrado: {OUTPUT_DIR}")
    if not os.path.exists(DB_PATH):
        raise SystemExit(f"DB não encontrado: {DB_PATH}")

    # Mapeia: pasta atual -> pasta canônica
    folders = [
        d for d in os.listdir(OUTPUT_DIR)
        if os.path.isdir(os.path.join(OUTPUT_DIR, d))
    ]

    mappings = []
    for name in folders:
        if not is_digits(name):
            continue
        canon = canonical_login(name)
        if canon != name:
            mappings.append((name, canon))

    if not mappings:
        print("[OK] Nada para unificar.")
        return

    # Conecta DB
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")

    moved = 0
    updated_rows = 0

    try:
        # Importante: processar em ordem estável, p/ evitar efeitos colaterais
        for src_name, dst_name in sorted(mappings, key=lambda x: (len(x[0]), x[0])):
            src = os.path.join(OUTPUT_DIR, src_name)
            dst = os.path.join(OUTPUT_DIR, dst_name)

            if not os.path.exists(src):
                continue

            print(f"[MOVE] {src_name} -> {dst_name}")
            move_merge_folder(src, dst)
            moved += 1

            # Atualiza DB paths (suporta \ e /)
            # Windows: geralmente fica com \, mas alguns scripts podem gravar /
            old_prefix_win = OUTPUT_DIR + "\\" + src_name + "\\"
            new_prefix_win = OUTPUT_DIR + "\\" + dst_name + "\\"
            old_prefix_unx = OUTPUT_DIR + "/" + src_name + "/"
            new_prefix_unx = OUTPUT_DIR + "/" + dst_name + "/"

            updated_rows += update_db_paths(conn, old_prefix_win, new_prefix_win)
            updated_rows += update_db_paths(conn, old_prefix_unx, new_prefix_unx)

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(f"\nFinalizado.")
    print(f"Pastas migradas/mescladas: {moved}")
    print(f"Registros de paths atualizados no DB: {updated_rows}")

if __name__ == "__main__":
    main()
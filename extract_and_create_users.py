# extract_pages_simple.py
import os, re, sys
import pdfplumber
from PyPDF2 import PdfReader, PdfWriter
import psycopg2
from passlib.hash import bcrypt

# === CONFIG ===
PDF_PATH   = r"contracheques_allago.pdf"   # <- aponte para o seu PDF geral
OUTPUT_DIR = r"contracheques_split"
DB_DSN     = "postgresql://contracheque_app:app_pass_alterar@localhost:5432/contracheque_db"

# Se todos são de setembro/2025, pode fixar aqui:
REF_OVERRIDE = "2025-08"  # ou None para auto-detectar

# Auto-detecção "SETEMBRO/2025" -> "2025-09"
MESES = {"JANEIRO":"01","FEVEREIRO":"02","MARCO":"03","MARÇO":"03","ABRIL":"04","MAIO":"05",
         "JUNHO":"06","JULHO":"07","AGOSTO":"08","SETEMBRO":"09","OUTUBRO":"10","NOVEMBRO":"11","DEZEMBRO":"12"}
REF_WORD_REGEX = re.compile(r"\b([A-ZÇÃÉÊÍÓÚÂÔÜ]+)\s*/\s*(\d{4})", re.IGNORECASE)

def ensure_dir(p): os.makedirs(p, exist_ok=True)

def detect_ref(text: str):
    if REF_OVERRIDE: 
        return REF_OVERRIDE
    m = REF_WORD_REGEX.search(text or "")
    if not m: 
        return None
    mes = (m.group(1) or "").upper().replace("Ç","C").replace("Ã","A").replace("É","E").replace("Ê","E").replace("Í","I").replace("Ó","O").replace("Ú","U").replace("Â","A").replace("Ô","O").replace("Ü","U")
    mm  = MESES.get(mes)
    return f"{m.group(2)}-{mm}" if mm else None

def detect_matricula(text: str):
    # Procura 7 dígitos nas primeiras linhas
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    for ln in lines[:5]:
        m = re.search(r"\b(\d{7})\b", ln)
        if m: 
            return m.group(1)
    # fallback: procura em toda a página
    m = re.search(r"\b(\d{7})\b", text or "")
    return m.group(1) if m else None

def upsert_user(cur, matricula, nome=None):
    cur.execute("SELECT id FROM users WHERE matricula=%s", (matricula,))
    r = cur.fetchone()
    if r: 
        return r[0]
    password_hash = bcrypt.hash(f"agespisa{matricula}")
    cur.execute(
        "INSERT INTO users (matricula, nome, password_hash, must_change_password) "
        "VALUES (%s,%s,%s,TRUE) RETURNING id",
        (matricula, nome, password_hash),
    )
    return cur.fetchone()[0]

def write_single_page(reader, page_idx, out_file):
    w = PdfWriter()
    w.add_page(reader.pages[page_idx])
    ensure_dir(os.path.dirname(out_file))
    with open(out_file, "wb") as f:
        w.write(f)

def main():
    if not os.path.exists(PDF_PATH):
        print(f"ERRO: arquivo não encontrado: {PDF_PATH}")
        sys.exit(1)

    reader = PdfReader(PDF_PATH)
    with pdfplumber.open(PDF_PATH) as pdf:
        conn = psycopg2.connect(DB_DSN)
        conn.autocommit = False
        cur = conn.cursor()

        total = len(pdf.pages)
        ok, falhas = 0, 0

        for i in range(total):
            text = (pdf.pages[i].extract_text() or "")
            matricula = detect_matricula(text)
            referencia = detect_ref(text) or "sem-ref"

            if not matricula:
                falhas += 1
                print(f"[WARN] Página {i+1}: matrícula não encontrada.")
                continue

            # caminho: contracheques_split/<matricula>/<referencia>.pdf
            out_dir  = os.path.join(OUTPUT_DIR, matricula)
            out_file = os.path.join(out_dir, f"{referencia}.pdf")
            write_single_page(reader, i, out_file)

            uid = upsert_user(cur, matricula, nome=None)
            cur.execute(
                "INSERT INTO payslips (user_id, referencia, file_path) "
                "VALUES (%s,%s,%s) "
                "ON CONFLICT (user_id, referencia) DO UPDATE SET file_path=EXCLUDED.file_path",
                (uid, referencia, out_file)
            )
            ok += 1
            print(f"OK p{i+1:03d}: {matricula} | {referencia} -> {out_file}")

        conn.commit()
        cur.close()
        conn.close()
        print(f"\nFinalizado. Sucesso: {ok} | Sem matrícula: {falhas} | Total páginas: {total}")

if __name__ == "__main__":
    main()

-- Esquema do sistema de contracheques (SQLite)

PRAGMA foreign_keys = ON;

-- Tabela de usuários (login por matrícula)
CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  matricula TEXT NOT NULL UNIQUE,
  nome TEXT,
  email TEXT,
  password_hash TEXT NOT NULL,
  must_change_password INTEGER NOT NULL DEFAULT 1,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de contracheques
CREATE TABLE IF NOT EXISTS payslips (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  referencia TEXT,                        -- ex.: 06/2024 ou 2024-06
  file_path TEXT NOT NULL,                -- ex.: contracheques_split/12345/2024-06.pdf
  pages_from INTEGER,                     -- opcional: página inicial no PDF grande
  pages_to INTEGER,                       -- opcional: página final no PDF grande
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT uq_payslip_user_ref UNIQUE (user_id, referencia)
);

-- Índices úteis
CREATE INDEX IF NOT EXISTS idx_users_matricula ON users(matricula);
CREATE INDEX IF NOT EXISTS idx_payslips_user ON payslips(user_id);
CREATE INDEX IF NOT EXISTS idx_payslips_ref ON payslips(referencia);

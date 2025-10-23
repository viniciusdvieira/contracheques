# Contracheques

Aplicação Flask para disponibilizar contracheques em PDF. Esta versão foi atualizada para utilizar SQLite, o que simplifica o deploy em ambientes pequenos (por exemplo, uma VM Ubuntu Server) sem necessidade de Postgres ou Docker.

## Requisitos

- Python 3.10 ou superior
- Dependências Python instaladas com `pip install -r requirements.txt`
- Biblioteca do sistema `libpq` não é mais necessária; basta o SQLite que já acompanha o Python

## Configuração local

1. Crie o ambiente virtual e instale as dependências:

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. Ajuste o arquivo `.env` conforme necessário. Valores padrão:

   ```env
   DATABASE_URL=contracheques.db
   SECRET_KEY=sua-chave-secreta-aleatoria
   STORAGE_DIR=contracheques_split
   ```

   > `DATABASE_URL` pode ser um caminho relativo ou absoluto para o arquivo SQLite. Ele será criado automaticamente quando a aplicação escrever dados.

3. Crie o banco de dados e o esquema inicial:

   ```bash
   sqlite3 contracheques.db < schema.sql
   ```

4. Gere os PDFs individuais e cadastre usuários/contracheques (opcional):

   ```bash
   python extract_and_create_users.py
   ```

5. Execute a aplicação localmente:

   ```bash
   flask --app app run --host 0.0.0.0 --port 5000
   ```

## Atualização em massa de senhas

Para redefinir as senhas seguindo o padrão `agespisa<matricula>`, execute:

```bash
python update_presenhas.py
```

## Deploy em uma VM Ubuntu Server (resumo)

1. **Dependências do sistema**

   ```bash
   sudo apt update
   sudo apt install python3 python3-venv python3-pip git nginx
   ```

2. **Clonar o projeto e preparar ambiente**

   ```bash
   git clone https://seu-repositorio.git contracheques
   cd contracheques
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   cp .env .env.production  # opcional; ajuste as variáveis
   sqlite3 contracheques.db < schema.sql
   ```

3. **Carregar dados (opcional)**

   ```bash
   python extract_and_create_users.py
   ```

4. **Executar com Gunicorn**

   ```bash
   .venv/bin/gunicorn --bind 0.0.0.0:8000 app:app
   ```

5. **(Opcional) systemd**: crie `/etc/systemd/system/contracheques.service` apontando para o Gunicorn e habilite com `sudo systemctl enable --now contracheques`.

6. **(Opcional) Nginx**: configure um virtual host que faça proxy para `http://127.0.0.1:8000`.

Os passos 4 a 6 estão descritos de forma resumida; ajuste conforme sua topologia de rede e políticas de segurança.

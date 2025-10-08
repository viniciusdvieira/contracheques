# Imagem base leve
FROM python:3.11-slim

# Não gerar .pyc e ativar stdout sem buffer
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Dependências nativas para psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Diretório de trabalho
WORKDIR /app

# Instalar deps Python
COPY requirements.txt /app/
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copiar o app
COPY . /app

# Diretório padrão dos PDFs (você vai montar como volume)
ENV STORAGE_DIR=/app/contracheques_split

# Expõe a porta do Gunicorn
EXPOSE 5000

# Sobe o Flask com Gunicorn em produção
CMD ["gunicorn", "-b", "0.0.0.0:5000", "app:app"]

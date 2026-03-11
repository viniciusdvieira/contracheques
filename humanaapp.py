from flask import Flask, request, render_template_string, send_file, url_for
import io
import re
from collections import defaultdict
from datetime import datetime

import pandas as pd

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024  # 16 MB

MONTHS_2025 = [f"{str(i).zfill(2)}/2025" for i in range(1, 13)]
PEOPLE_DATA = {}
HEADER_IMAGE = "img/cabecalho_agespisa.png"
SIGNATURE_IMAGE = "img/assinatura_fabricio.png"

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Declaração Humana 2025</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            background: #f4f6f8;
            margin: 0;
            padding: 24px;
            color: #222;
        }
        .container {
            max-width: 1280px;
            margin: 0 auto;
            display: grid;
            grid-template-columns: 380px 1fr;
            gap: 24px;
        }
        .card {
            background: #fff;
            border-radius: 16px;
            box-shadow: 0 4px 20px rgba(0,0,0,0.06);
            padding: 20px;
        }
        h1, h2, h3 { margin-top: 0; }
        label {
            display: block;
            font-weight: bold;
            margin-bottom: 8px;
        }
        input[type="text"], input[type="file"], select {
            width: 100%;
            padding: 12px;
            border: 1px solid #cfd6dd;
            border-radius: 10px;
            box-sizing: border-box;
            font-size: 14px;
        }
        button {
            width: 100%;
            padding: 12px 16px;
            border: 0;
            border-radius: 10px;
            background: #0f172a;
            color: #fff;
            font-size: 14px;
            cursor: pointer;
            margin-top: 12px;
        }
        button:hover { opacity: 0.94; }
        .muted {
            color: #667085;
            font-size: 13px;
        }
        .field {
            margin-bottom: 16px;
        }
        .preview {
            background: #fff;
            min-height: 1123px;
            padding: 56px;
            box-sizing: border-box;
        }
        .doc-header {
            text-align: center;
            margin-bottom: 26px;
        }
        .doc-header img {
            max-width: 100%;
            height: auto;
        }
        .doc-title {
            text-align: center;
            font-family: Arial, sans-serif;
            font-size: 22px;
            letter-spacing: 0.5px;
            margin: 18px 0 26px 0;
        }
        .signature-block {
            margin-top: 56px;
            text-align: center;
        }
        .signature-block img {
            width: 280px;
            max-width: 100%;
            height: auto;
            display: block;
            margin: 0 auto 4px auto;
        }
        .signature-line {
            width: 320px;
            max-width: 100%;
            border-top: 1px solid #000;
            margin: 0 auto 8px auto;
        }
        .signature-role {
            font-family: Arial, sans-serif;
            font-size: 16px;
        }
        .declaracao {
            font-family: "Times New Roman", serif;
            font-size: 18px;
            line-height: 1.7;
            color: #000;
        }
        .declaracao p {
            text-align: justify;
            margin: 0 0 22px 0;
        }
        table {
            border-collapse: collapse;
            margin: 0 auto 24px auto;
            min-width: 360px;
            font-size: 18px;
        }
        th, td {
            border: 1px solid #000;
            padding: 4px 18px;
            text-align: center;
        }
        .top-info {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 18px;
        }
        .pill {
            background: #e2e8f0;
            padding: 6px 10px;
            border-radius: 999px;
            font-size: 12px;
        }
        .stats {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 12px;
            margin-top: 10px;
        }
        .stat {
            background: #f8fafc;
            border: 1px solid #e2e8f0;
            padding: 12px;
            border-radius: 12px;
        }
        .actions {
            display: flex;
            gap: 10px;
            margin-top: 12px;
        }
        .actions form {
            flex: 1;
        }
        .actions button {
            margin-top: 0;
        }
        .error {
            background: #fee2e2;
            color: #991b1b;
            padding: 10px;
            border-radius: 10px;
            margin-bottom: 12px;
            font-size: 14px;
        }
        @media print {
            body { background: white; padding: 0; }
            .sidebar, .top-info { display: none !important; }
            .container { display: block; max-width: 100%; }
            .card { box-shadow: none; padding: 0; }
            .preview { padding: 40px; min-height: auto; }
        }
        @media (max-width: 960px) {
            .container { grid-template-columns: 1fr; }
            .preview { padding: 24px; }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="card sidebar">
            <h2>Declaração Humana 2025</h2>

            {% if error %}
                <div class="error">{{ error }}</div>
            {% endif %}

            <form method="post" enctype="multipart/form-data" action="/upload">
                <div class="field">
                    <label>Planilha base</label>
                    <input type="file" name="arquivo" accept=".xlsx,.xls,.csv" required>
                    <div class="muted" style="margin-top:8px;">
                        Coluna B = nome | G = valor pago | H = data do pagamento
                    </div>
                    {% if arquivo_nome %}
                        <div class="muted" style="margin-top:6px;"><strong>Arquivo:</strong> {{ arquivo_nome }}</div>
                    {% endif %}
                </div>
                <button type="submit">Carregar planilha</button>
            </form>

            <hr style="margin: 20px 0; border: 0; border-top: 1px solid #e5e7eb;">

            <form method="get" action="/">
                <div class="field">
                    <label>Nome da pessoa</label>
                    <input list="nomes" name="nome" value="{{ selected_name }}" placeholder="Digite ou selecione o nome" {% if not people_names %}disabled{% endif %}>
                    <datalist id="nomes">
                        {% for nome in people_names %}
                            <option value="{{ nome }}"></option>
                        {% endfor %}
                    </datalist>
                </div>

                <div class="field">
                    <label>CPF</label>
                    <input type="text" name="cpf" value="{{ cpf }}" maxlength="14" placeholder="000.000.000-00">
                </div>

                <button type="submit">Atualizar declaração</button>
            </form>

            <div class="stats">
                <div class="stat">
                    <div class="muted">Pessoa selecionada</div>
                    <div><strong>{{ selected_name if selected_name else 'Nenhuma' }}</strong></div>
                </div>
                <div class="stat">
                    <div class="muted">Total em 2025</div>
                    <div><strong>{{ total_fmt }}</strong></div>
                </div>
            </div>

            <div class="actions">
                <form method="get" action="/print" target="_blank">
                    <input type="hidden" name="nome" value="{{ selected_name }}">
                    <input type="hidden" name="cpf" value="{{ cpf }}">
                    <button type="submit" {% if not selected_person %}disabled{% endif %}>Imprimir</button>
                </form>
            </div>
        </div>

        <div class="card preview">
            <div class="top-info">
                <div>
                    <h3 style="margin-bottom: 4px;">Pré-visualização da declaração</h3>
                    <div class="muted">Documento em formato A4</div>
                </div>
                <span class="pill">Ano-base 2025</span>
            </div>

            <div class="declaracao">
                <div class="doc-header">
                    <img src="{{ header_url }}" alt="Cabeçalho AGESPISA">
                </div>

                <div class="doc-title">DECLARAÇÃO</div>
                <p>
                    Declaramos para os devidos fins que <strong>{{ selected_name if selected_name else '<nome da pessoa>' }}</strong>
                    com o CPF <strong>{{ cpf if cpf else '<cpf da pessoa>' }}</strong>, ex-empregada desta empresa,
                    é beneficiária do Plano de Saúde Humana Assistência Médica Ltda. Pagou mensalmente no ano de 2025:
                </p>

                {% if table_rows %}
                <table>
                    <thead>
                        <tr>
                            <th>COMPETÊNCIA</th>
                            <th>VALOR (R$)</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for month, value in table_rows %}
                        <tr>
                            <td>{{ month }}</td>
                            <td>{{ value }}</td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
                {% endif %}

                <p>
                    Referente à beneficiária supracitada, conforme contrato celebrado entre a AGESPISA
                    e a HUMANA ASSISTÊNCIA MÉDICA LTDA para prestação de serviço de assistência
                    à saúde.
                </p>

                <div class="signature-block">
                    <img src="{{ signature_url }}" alt="Assinatura digital">
                    <div class="signature-line"></div>
                    <div class="signature-role">Diretor Financeiro</div>
                </div>
            </div>
        </div>
    </div>
</body>
</html>
"""

PRINT_TEMPLATE = """
<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <title>Declaração</title>
    <style>
        body {
            margin: 0;
            background: white;
            font-family: "Times New Roman", serif;
            font-size: 18px;
            line-height: 1.7;
            color: #000;
        }
        .page {
            width: 794px;
            margin: 0 auto;
            padding: 56px;
            box-sizing: border-box;
        }
        .doc-header {
            text-align: center;
            margin-bottom: 26px;
        }
        .doc-header img {
            max-width: 100%;
            height: auto;
        }
        .doc-title {
            text-align: center;
            font-family: Arial, sans-serif;
            font-size: 22px;
            letter-spacing: 0.5px;
            margin: 18px 0 26px 0;
        }
        .signature-block {
            margin-top: 56px;
            text-align: center;
        }
        .signature-block img {
            width: 280px;
            max-width: 100%;
            height: auto;
            display: block;
            margin: 0 auto 4px auto;
        }
        .signature-line {
            width: 320px;
            max-width: 100%;
            border-top: 1px solid #000;
            margin: 0 auto 8px auto;
        }
        .signature-role {
            font-family: Arial, sans-serif;
            font-size: 16px;
        }
        p { text-align: justify; margin: 0 0 22px 0; }
        table {
            border-collapse: collapse;
            margin: 0 auto 24px auto;
            min-width: 360px;
            font-size: 18px;
        }
        th, td {
            border: 1px solid #000;
            padding: 4px 18px;
            text-align: center;
        }
        @media print {
            .page { width: 100%; margin: 0; padding: 40px; }
        }
    </style>
</head>
<body onload="window.print()">
    <div class="page">
        <div class="doc-header">
            <img src="{{ header_url }}" alt="Cabeçalho AGESPISA">
        </div>

        <div class="doc-title">DECLARAÇÃO</div>
        <p>
            Declaramos para os devidos fins que <strong>{{ selected_name }}</strong>
            com o CPF <strong>{{ cpf }}</strong>, ex-empregada desta empresa,
            é beneficiária do Plano de Saúde Humana Assistência Médica Ltda. Pagou mensalmente no ano de 2025:
        </p>

        {% if table_rows %}
        <table>
            <thead>
                <tr>
                    <th>COMPETÊNCIA</th>
                    <th>VALOR (R$)</th>
                </tr>
            </thead>
            <tbody>
                {% for month, value in table_rows %}
                <tr>
                    <td>{{ month }}</td>
                    <td>{{ value }}</td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
        {% endif %}

        <p>
            Referente à beneficiária supracitada, conforme contrato celebrado entre a AGESPISA
            e a HUMANA ASSISTÊNCIA MÉDICA LTDA para prestação de serviço de assistência
            à saúde.
        </p>

        <div class="signature-block">
            <img src="{{ signature_url }}" alt="Assinatura digital">
            <div class="signature-line"></div>
            <div class="signature-role">Diretor Financeiro</div>
        </div>
    </div>
</body>
</html>
"""


def normalize_name(value: str) -> str:
    value = str(value or "").strip().upper()
    value = re.sub(r"\s+", " ", value)
    return value


def only_digits(value: str) -> str:
    return re.sub(r"\D", "", str(value or ""))


def format_cpf(value: str) -> str:
    digits = only_digits(value)[:11]
    if len(digits) <= 3:
        return digits
    if len(digits) <= 6:
        return f"{digits[:3]}.{digits[3:]}"
    if len(digits) <= 9:
        return f"{digits[:3]}.{digits[3:6]}.{digits[6:]}"
    return f"{digits[:3]}.{digits[3:6]}.{digits[6:9]}-{digits[9:11]}"


def parse_money(value) -> float:
    if pd.isna(value):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    text = text.replace(".", "").replace(",", ".")
    text = re.sub(r"[^\d\.-]", "", text)
    try:
        return float(text)
    except ValueError:
        return 0.0


def parse_date(value):
    if pd.isna(value):
        return None
    if isinstance(value, datetime):
        return value
    if hasattr(value, "to_pydatetime"):
        return value.to_pydatetime()

    text = str(value).strip()
    if not text:
        return None

    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass

    parsed = pd.to_datetime(value, dayfirst=True, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.to_pydatetime()


def format_money_br(value: float) -> str:
    return f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def build_people_data(df: pd.DataFrame):
    grouped = {}

    for _, row in df.iterrows():
        try:
            name = row.iloc[1]   # coluna B
            amount_raw = row.iloc[6]  # coluna G
            date_raw = row.iloc[4]    # coluna H
        except IndexError:
            continue

        if pd.isna(name):
            continue

        name = str(name).strip()
        normalized = normalize_name(name)
        if not normalized:
            continue

        amount = parse_money(amount_raw)
        payment_date = parse_date(date_raw)
        if not payment_date:
            continue
        if payment_date.year != 2025:
            continue

        month_key = payment_date.strftime("%m/%Y")
        if month_key not in MONTHS_2025:
            continue

        if normalized not in grouped:
            grouped[normalized] = {
                "name": name,
                "payments": defaultdict(float),
            }

        grouped[normalized]["payments"][month_key] += amount

    normalized_map = {}
    for person in grouped.values():
        payments = {month: round(person["payments"].get(month, 0.0), 2) for month in MONTHS_2025}
        normalized_map[person["name"]] = {
            "name": person["name"],
            "payments": payments,
        }

    return dict(sorted(normalized_map.items(), key=lambda item: item[0]))


def get_person_table(selected_name: str):
    person = PEOPLE_DATA.get(selected_name)
    if not person:
        return None, [], 0.0

    table_rows = []
    total = 0.0
    for month in MONTHS_2025:
        value = float(person["payments"].get(month, 0.0))
        if value > 0:
            total += value
            table_rows.append((month, format_money_br(value)))
    return person, table_rows, total


def render_main_page(selected_name="", cpf="", error=""):
    people_names = list(PEOPLE_DATA.keys())
    selected_person, table_rows, total = get_person_table(selected_name)
    return render_template_string(
        HTML_TEMPLATE,
        people_names=people_names,
        selected_name=selected_name,
        cpf=format_cpf(cpf),
        selected_person=selected_person,
        table_rows=table_rows,
        total_fmt=format_money_br(total),
        arquivo_nome=app.config.get("ARQUIVO_NOME", ""),
        error=error,
        header_url=url_for("static", filename=HEADER_IMAGE),
        signature_url=url_for("static", filename=SIGNATURE_IMAGE),
    )


@app.route("/", methods=["GET"])
def index():
    selected_name = request.args.get("nome", "").strip()
    cpf = request.args.get("cpf", "").strip()
    return render_main_page(selected_name=selected_name, cpf=cpf)


@app.route("/upload", methods=["POST"])
def upload():
    global PEOPLE_DATA

    file = request.files.get("arquivo")
    if not file or not file.filename:
        return render_main_page(error="Selecione um arquivo Excel ou CSV.")

    filename = file.filename.lower()

    try:
        if filename.endswith(".csv"):
            df = pd.read_csv(file, header=None)
        else:
            df = pd.read_excel(file, header=None)
    except Exception as exc:
        return render_main_page(error=f"Erro ao ler a planilha: {exc}")

    PEOPLE_DATA = build_people_data(df)
    app.config["ARQUIVO_NOME"] = file.filename

    if not PEOPLE_DATA:
        return render_main_page(error="Nenhum registro válido de 2025 foi encontrado na planilha.")

    return render_main_page()


@app.route("/print", methods=["GET"])
def print_view():
    selected_name = request.args.get("nome", "").strip()
    cpf = format_cpf(request.args.get("cpf", "").strip())

    selected_person, table_rows, _ = get_person_table(selected_name)
    if not selected_person:
        return "Pessoa não encontrada. Volte e selecione um nome válido.", 400

    return render_template_string(
        PRINT_TEMPLATE,
        selected_name=selected_name,
        cpf=cpf or "<cpf da pessoa>",
        table_rows=table_rows,
        header_url=url_for("static", filename=HEADER_IMAGE, _external=True),
        signature_url=url_for("static", filename=SIGNATURE_IMAGE, _external=True),
    )


@app.route("/pdf", methods=["GET"])
def pdf_view():
    selected_name = request.args.get("nome", "").strip()
    cpf = format_cpf(request.args.get("cpf", "").strip())

    selected_person, table_rows, _ = get_person_table(selected_name)
    if not selected_person:
        return "Pessoa não encontrada. Volte e selecione um nome válido.", 400

    html = render_template_string(
        PRINT_TEMPLATE,
        selected_name=selected_name,
        cpf=cpf or "<cpf da pessoa>",
        table_rows=table_rows,
        header_url=url_for("static", filename=HEADER_IMAGE, _external=True),
        signature_url=url_for("static", filename=SIGNATURE_IMAGE, _external=True),
    )

    try:
        from weasyprint import HTML
    except ImportError:
        return (
            "WeasyPrint não está instalado. Rode: pip install weasyprint. "
            "Enquanto isso, use o botão Imprimir e salve em PDF pelo navegador.",
            500,
        )

    pdf_bytes = HTML(string=html).write_pdf()
    file_buffer = io.BytesIO(pdf_bytes)
    safe_name = re.sub(r"[^A-Za-z0-9_-]+", "_", selected_name.upper()).strip("_") or "DECLARACAO"

    return send_file(
        file_buffer,
        mimetype="application/pdf",
        as_attachment=True,
        download_name=f"DECLARACAO_{safe_name}.pdf",
    )


if __name__ == "__main__":
    app.run(debug=True)

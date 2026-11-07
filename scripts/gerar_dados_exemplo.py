"""Gera dados 100% FICTÍCIOS no mesmo formato das exportações reais (WebDiet e LiveClin).

Nenhum dado real de paciente está neste repositório. Os nomes são combinações aleatórias,
os telefones usam o prefixo fictício 21 90000-xxxx, os e-mails usam o domínio exemplo.com
e os CPFs são propositalmente inválidos (o pipeline os descarta na validação).

Os arquivos imitam os problemas reais que o pipeline resolve: cadastros repetidos,
telefones em formatos diferentes, nomes com acento/caixa diferente entre os sistemas,
pacientes que existem só em um dos sistemas e consultas duplicadas no mesmo dia.

Uso:  python scripts/gerar_dados_exemplo.py   (grava em dados_exemplo/)
"""
import random
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

random.seed(42)
SAIDA = Path(__file__).resolve().parent.parent / "dados_exemplo"
DATA_REF = date(2026, 11, 6)
N = 320

PRIMEIROS_F = ["Ana", "Beatriz", "Camila", "Daniela", "Fernanda", "Gabriela", "Helena", "Isabela",
               "Juliana", "Larissa", "Mariana", "Natália", "Patrícia", "Renata", "Sofia", "Tatiana"]
PRIMEIROS_M = ["André", "Bruno", "Carlos", "Diego", "Eduardo", "Felipe", "Gustavo", "Henrique",
               "Igor", "Lucas", "Marcelo", "Pedro", "Rafael", "Rodrigo", "Thiago", "Vinícius"]
SOBRENOMES = ["Almeida", "Barbosa", "Cardoso", "Costa", "Dias", "Ferreira", "Gomes", "Lima",
              "Martins", "Moreira", "Nunes", "Oliveira", "Pereira", "Ribeiro", "Rocha", "Santos",
              "Silva", "Souza", "Teixeira", "Vieira"]
# sazonalidade: mais procura no início do ano e antes do verão
PESO_MES = [1.6, 1.5, 1.3, 1.0, 0.9, 0.7, 0.8, 1.1, 1.0, 1.1, 1.2, 0.8]
PLANOS = {"Start": 30, "Keep": 90, "Excellence": 180}


def data_cadastro():
    while True:
        ano = random.choice([2024, 2025, 2026])
        mes = random.choices(range(1, 13), weights=PESO_MES)[0]
        d = date(ano, mes, random.randint(1, 28))
        if d <= DATA_REF:
            return d


def telefone(i):
    numero = f"90000{i:04d}"
    formatos = [f"(21) {numero[:5]}-{numero[5:]}", f"21{numero}", f"+55 21 {numero}",
                f"21 {numero[1:]}", numero]          # com/sem DDD, com 55, sem o 9
    return random.choices(formatos, weights=[5, 3, 2, 1, 1])[0]


pacientes = []
for i in range(N):
    fem = random.random() < 0.72
    nome = f"{random.choice(PRIMEIROS_F if fem else PRIMEIROS_M)} {random.choice(SOBRENOMES)} {random.choice(SOBRENOMES)}"
    pacientes.append({
        "i": i, "nome": nome, "fem": fem,
        "nasc": date(random.randint(1960, 2008), random.randint(1, 12), random.randint(1, 28)),
        "tel": telefone(i), "email": f"paciente{i:03d}@exemplo.com" if random.random() < 0.8 else None,
        "cadastro": data_cadastro(),
        "no_webdiet": random.random() < 0.92, "no_liveclin": random.random() < 0.85,
    })

# ---------- WebDiet: cadastro de pacientes (xlsx) ----------
linhas_wd = []
for p in pacientes:
    if not p["no_webdiet"]:
        continue
    copias = 2 if random.random() < 0.06 else 1           # cadastros repetidos
    for k in range(copias):
        criado = datetime.combine(p["cadastro"] + timedelta(days=30 * k), datetime.min.time()) + timedelta(hours=random.randint(8, 19))
        nome = p["nome"].upper() if random.random() < 0.15 else p["nome"]
        linhas_wd.append({
            "Nome do paciente": nome,
            "Gênero": "Feminino" if p["fem"] else "Masculino",
            "Data de nascimento": p["nasc"].strftime("%d/%m/%Y"),
            "Telefone": p["tel"],
            "Email": p["email"] or "",
            "CPF": "111.111.111-11" if random.random() < 0.1 else "",
            "Criado em": criado.strftime("%Y-%m-%d %H:%M:%S"),
            "Modificado em": (criado + timedelta(days=random.randint(0, 200))).strftime("%d/%m/%Y - %H:%M:%S"),
            "Observacao": "",
        })

# ---------- LiveClin: planos e acompanhamento (csv) ----------
linhas_lc, planos_por_paciente = [], {}
for p in pacientes:
    if not p["no_liveclin"]:
        continue
    plano = random.choices(list(PLANOS), weights=[3, 4, 2])[0]
    duracao = PLANOS[plano]
    inicio = p["cadastro"] + timedelta(days=random.randint(0, 20))
    renovacoes = random.choices([0, 1, 2, 3], weights=[4, 3, 2, 1])[0]
    inicio += timedelta(days=duracao * renovacoes)
    if plano == "Start" and inicio >= date(2026, 6, 1) and random.random() < 0.5:
        duracao = 45                                       # erro conhecido do sistema
    fim = inicio + timedelta(days=duracao)
    if fim > DATA_REF:
        status, restantes = "active", (fim - DATA_REF).days
    else:
        status, restantes = random.choices(["finished", "inactive"], weights=[3, 1])[0], 0
    enviados = random.randint(0, 12)
    respostas = random.randint(0, enviados)
    nome = p["nome"]
    if random.random() < 0.2:                              # grafia diferente entre sistemas
        nome = nome.replace("í", "i").replace("é", "e").replace("á", "a").lower()
    planos_por_paciente[p["i"]] = (plano, inicio, fim)
    linhas_lc.append({
        "patientReport.headers.id": 1000 + p["i"],
        "patientReport.headers.full_name": nome,
        "patientReport.headers.gender": "Feminine" if p["fem"] else "Masculine",
        "patientReport.headers.birth_date": p["nasc"].isoformat() if random.random() < 0.9 else "",
        "patientReport.headers.phone_number": p["tel"],
        "patientReport.headers.email": p["email"] or "",
        "patientReport.headers.is_whatsapp_validated": random.choice(["Yes", "No"]),
        "patientReport.headers.last_service_provided": plano,
        "patientReport.headers.customer_status": status,
        "patientReport.headers.service_end_date": fim.isoformat(),
        "patientReport.headers.service_days_remaining": restantes,
        "patientReport.headers.service_duration_days": duracao,
        "patientReport.headers.questionnaires_sent": enviados,
        "patientReport.headers.total_responses": respostas,
        "patientReport.headers.response_rate_percentage": round(100 * respostas / enviados, 1) if enviados else 0,
        "patientReport.headers.last_response_date": (fim - timedelta(days=random.randint(0, 20))).isoformat() if respostas else "",
    })

# ---------- WebDiet: relatório de consultas (xlsx) ----------
linhas_c = []
for p in pacientes:
    if not p["no_webdiet"]:
        continue
    d = p["cadastro"]
    for _ in range(random.randint(1, 7)):
        if d > DATA_REF:
            break
        linhas_c.append({"Nome do paciente": p["nome"], "Data": d.strftime("%d/%m/%Y"), "Observacao": ""})
        if random.random() < 0.05:                         # linha duplicada no mesmo dia
            linhas_c.append({"Nome do paciente": p["nome"], "Data": d.strftime("%d/%m/%Y"), "Observacao": ""})
        d += timedelta(days=random.randint(30, 60))

SAIDA.mkdir(exist_ok=True)
pd.DataFrame(linhas_wd).to_excel(SAIDA / "pacientes_webdiet.xlsx", index=False)
pd.DataFrame(linhas_lc).to_csv(SAIDA / "pacientes_liveclin.csv", index=False, encoding="utf-8-sig")
pd.DataFrame(linhas_c).to_excel(SAIDA / "relatorio_consultas_webdiet.xlsx", index=False)
print(f"WebDiet: {len(linhas_wd)} cadastros | LiveClin: {len(linhas_lc)} | consultas: {len(linhas_c)}")

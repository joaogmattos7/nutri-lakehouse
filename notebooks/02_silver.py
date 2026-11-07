# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02 · Silver + Gold de pacientes
# MAGIC Limpa os dois sistemas, corrige o plano Start, unifica os pacientes, grava as tabelas `silver_*` e gera a `gold_pacientes`.
# MAGIC Autocontido: pode rodar do zero com **Run all**, sem depender de outras células.

# COMMAND ----------

# 1. Funções de limpeza
import re, hashlib, unicodedata
import pandas as pd

PARTICULAS = {"de", "da", "do", "das", "dos", "e"}
MAPA_GENERO = {"feminino": "F", "feminine": "F", "f": "F",
               "masculino": "M", "masculine": "M", "m": "M"}

def so_digitos(v):
    return "" if pd.isna(v) else re.sub(r"\D", "", str(v))

def normalizar_telefone(v):
    """Devolve (telefone_normalizado, qualidade). Brasil: DDD+número, sem o 55.
    Internacional: código do país + número. Não inventa DDD."""
    if pd.isna(v):
        return None, "ausente"
    texto = str(v).strip()
    # internacional só quando vem com '+' e outro código de país; "(21) 9..." é DDD, não país
    if texto.startswith("+") and not so_digitos(texto).startswith("55"):
        return so_digitos(texto), "internacional"
    d = so_digitos(texto)
    if not d:
        return None, "ausente"
    if len(d) >= 12 and d.startswith("55"):
        d = d[2:]
    if len(d) == 11 and d[2] == "9":
        return d, "ok"
    if len(d) == 10 and d[2] in "6789":
        return d[:2] + "9" + d[2:], "ok_9_adicionado"
    if len(d) == 10:
        return d, "ok_fixo"
    return None, "incompleto"

def chave_telefone(bruto, normalizado):
    """Últimos 9 dígitos: casa o mesmo número mesmo sem DDD ou com código de país."""
    d = normalizado or so_digitos(bruto)
    return d[-9:] if len(d) >= 8 else None

def normalizar_texto(v):
    if pd.isna(v):
        return ""
    s = unicodedata.normalize("NFKD", str(v)).encode("ascii", "ignore").decode()
    return re.sub(r"\s+", " ", s.lower().strip())

def chave_nome(v):
    return " ".join(t for t in normalizar_texto(v).split() if t not in PARTICULAS)

def normalizar_email(v):
    if pd.isna(v):
        return None
    s = str(v).strip().lower()
    return s if "@" in s else None

def cpf_valido(cpf):
    if len(cpf) != 11 or cpf == cpf[0] * 11:
        return False
    for i in (9, 10):
        soma = sum(int(cpf[n]) * (i + 1 - n) for n in range(i))
        if (soma * 10 % 11) % 10 != int(cpf[i]):
            return False
    return True

def normalizar_cpf(v):
    d = so_digitos(v)
    if not d:
        return None
    d = d.zfill(11)
    return d if cpf_valido(d) else None

def normalizar_genero(v):
    return None if pd.isna(v) else MAPA_GENERO.get(normalizar_texto(v))

def hash_estavel(texto):
    return int(hashlib.sha1(texto.encode()).hexdigest()[:12], 16)

def para_spark(df):
    """pandas -> Spark: troca NaN/NaT por None (nulo de verdade)."""
    return df.astype(object).where(df.notna(), None)

# COMMAND ----------

# 2. Transformações de cada sistema
def silver_webdiet(b):
    df = pd.DataFrame()
    df["nome"] = b["nome_do_paciente"].str.strip().str.title()
    df["nome_chave"] = b["nome_do_paciente"].map(chave_nome)
    df["genero"] = b["genero"].map(normalizar_genero)
    df["data_nascimento"] = pd.to_datetime(b["data_de_nascimento"], format="%d/%m/%Y", errors="coerce").dt.date
    tel = b["telefone"].map(normalizar_telefone)
    df["telefone"] = tel.map(lambda t: t[0])
    df["telefone_qualidade"] = tel.map(lambda t: t[1])
    df["telefone_chave"] = [chave_telefone(r, t[0]) for r, t in zip(b["telefone"], tel)]
    df["email"] = b["email"].map(normalizar_email)
    df["cpf"] = b["cpf"].map(normalizar_cpf)
    df["criado_em"] = pd.to_datetime(b["criado_em"], errors="coerce")
    df["modificado_em"] = pd.to_datetime(b["modificado_em"], format="%d/%m/%Y - %H:%M:%S", errors="coerce")
    # o WebDiet não exporta ID: cria uma chave determinística (nome + nascimento)
    df["id_webdiet"] = [f"WD-{hash_estavel(f'{n}|{d}') % 10**8:08d}"
                        for n, d in zip(df["nome_chave"], df["data_nascimento"])]
    return df

def deduplicar_webdiet(df):
    """Une cadastros repetidos do mesmo paciente (mesmo nome + nascimento).
    Mantém o 1º cadastro (criado_em mais antigo) e a modificação mais recente."""
    df = df.sort_values("criado_em")
    g = df.groupby("id_webdiet", sort=False)
    unico = g.first()
    unico["criado_em"] = g["criado_em"].min()
    unico["modificado_em"] = g["modificado_em"].max()
    unico["qtd_cadastros_webdiet"] = g.size()
    return unico.reset_index()[list(df.columns) + ["qtd_cadastros_webdiet"]]

def silver_liveclin(b):
    num = lambda c: pd.to_numeric(b[c], errors="coerce")
    df = pd.DataFrame()
    df["id_liveclin"] = num("id").astype("int64")
    df["nome"] = b["full_name"].str.strip().str.title()
    df["nome_chave"] = b["full_name"].map(chave_nome)
    df["genero"] = b["gender"].map(normalizar_genero)
    df["data_nascimento"] = pd.to_datetime(b["birth_date"], errors="coerce").dt.date
    tel = b["phone_number"].map(normalizar_telefone)
    df["telefone"] = tel.map(lambda t: t[0])
    df["telefone_qualidade"] = tel.map(lambda t: t[1])
    df["telefone_chave"] = [chave_telefone(r, t[0]) for r, t in zip(b["phone_number"], tel)]
    df["email"] = b["email"].map(normalizar_email)
    df["whatsapp_validado"] = b["is_whatsapp_validated"].eq("Yes")
    df["plano"] = b["last_service_provided"]
    df["status_cliente"] = b["customer_status"]
    df["data_fim_plano"] = pd.to_datetime(b["service_end_date"], errors="coerce").dt.date
    df["dias_restantes"] = num("service_days_remaining").astype("int64")
    df["duracao_dias"] = num("service_duration_days").astype("int64")
    df["questionarios_enviados"] = num("questionnaires_sent").astype("int64")
    df["respostas"] = num("total_responses").astype("int64")
    df["taxa_resposta"] = num("response_rate_percentage").astype(float)
    df["ultima_resposta"] = pd.to_datetime(b["last_response_date"], errors="coerce").dt.date
    df["data_referencia"] = pd.to_datetime(b["_data_referencia"], errors="coerce").dt.date
    return df

# COMMAND ----------

# 3. Correção do plano Start (30 dias)
from datetime import date

def corrigir_start(lc, a_partir_de=date(2026, 6, 1)):
    """O plano Start passou de 45 para 30 dias, mas o LiveClin continuou registrando 45
    em alguns pacientes novos. Para Start de 45 dias iniciados a partir de `a_partir_de`,
    recalcula a data de fim com 30 dias. Guarda os valores originais para auditoria."""
    lc = lc.copy()
    lc["duracao_dias_original"] = lc["duracao_dias"]
    lc["data_fim_plano_original"] = lc["data_fim_plano"]
    fim = pd.to_datetime(lc["data_fim_plano"])
    inicio = fim - pd.to_timedelta(lc["duracao_dias"], unit="D")
    m = (lc["plano"] == "Start") & (lc["duracao_dias"] == 45) & (inicio >= pd.Timestamp(a_partir_de))
    novo_fim = inicio[m] + pd.Timedelta(days=30)
    ref = pd.to_datetime(lc["data_referencia"])
    lc.loc[m, "data_fim_plano"] = novo_fim.dt.date
    lc.loc[m, "duracao_dias"] = 30
    lc.loc[m, "dias_restantes"] = (novo_fim - ref[m]).dt.days.clip(lower=0)
    vencido = m & (lc["dias_restantes"] == 0) & (lc["status_cliente"] == "active")
    lc.loc[vencido, "status_cliente"] = "finished"      # venceu pela regra nova
    lc["ajuste_start_30d"] = m
    return lc

# COMMAND ----------

# 4. Pareamento entre os sistemas
from difflib import SequenceMatcher

LIMITE_NOME_FORTE = 0.80

def preenchido(x):
    return pd.notna(x) and x != ""

def similaridade_nome(a, b):
    """0..1 entre duas chaves de nome; a ordem das palavras não importa."""
    if not a or not b:
        return 0.0
    ta, tb = " ".join(sorted(a.split())), " ".join(sorted(b.split()))
    return max(SequenceMatcher(None, a, b).ratio(), SequenceMatcher(None, ta, tb).ratio())

def gerar_candidatos(wd, lc):
    """Todos os pares (WebDiet x LiveClin) que dividem telefone, e-mail ou nome,
    cada um com os 'sinais' que batem e uma nota de confiança."""
    wd = wd.reset_index(drop=True).assign(_iw=lambda d: d.index)
    lc = lc.reset_index(drop=True).assign(_il=lambda d: d.index)
    blocos = []
    for chave in ("telefone_chave", "email", "nome_chave"):
        a = wd.loc[wd[chave].map(preenchido), ["_iw", chave]]
        b = lc.loc[lc[chave].map(preenchido), ["_il", chave]]
        blocos.append(a.merge(b, on=chave)[["_iw", "_il"]])
    cand = pd.concat(blocos).drop_duplicates().reset_index(drop=True)

    w, l = wd.set_index("_iw"), lc.set_index("_il")
    linhas = []
    for iw, il in zip(cand["_iw"], cand["_il"]):
        rw, rl = w.loc[iw], l.loc[il]
        sim = similaridade_nome(rw["nome_chave"], rl["nome_chave"])
        s_tel = preenchido(rw["telefone_chave"]) and rw["telefone_chave"] == rl["telefone_chave"]
        s_email = preenchido(rw["email"]) and rw["email"] == rl["email"]
        s_nasc = preenchido(rw["data_nascimento"]) and rw["data_nascimento"] == rl["data_nascimento"]
        s_nome = sim >= LIMITE_NOME_FORTE
        s_primeiro = rw["nome_chave"].split()[:1] == rl["nome_chave"].split()[:1]
        sinais = int(s_tel) + int(s_email) + int(s_nasc) + int(s_nome)
        if sinais >= 2:
            conf = "alta"
        elif sinais == 1 and (s_tel or s_email) and s_primeiro:
            conf = "media"
        elif sinais == 1 and s_nome:
            conf = "media"
        else:
            conf = "revisar"      # ex.: só o telefone bate (família), nomes diferentes
        linhas.append({"iw": iw, "il": il, "sim_nome": round(sim, 3), "s_tel": bool(s_tel),
                       "s_email": bool(s_email), "s_nasc": bool(s_nasc), "s_nome": bool(s_nome),
                       "sinais": sinais, "confianca": conf, "score": sinais + sim})
    return pd.DataFrame(linhas)

def melhores_pares(cand):
    """Escolhe os melhores pares, sem repetir ninguém (1 para 1). Ignora 'revisar'."""
    aceitos = cand[cand["confianca"].isin(["alta", "media"])].sort_values("score", ascending=False)
    usados_w, usados_l, escolhidos = set(), set(), []
    for _, r in aceitos.iterrows():
        if r["iw"] in usados_w or r["il"] in usados_l:
            continue
        usados_w.add(r["iw"]); usados_l.add(r["il"]); escolhidos.append(r)
    return pd.DataFrame(escolhidos)

def unificar(wd, lc):
    wd, lc = wd.reset_index(drop=True), lc.reset_index(drop=True)
    cand = gerar_candidatos(wd, lc)
    esc = melhores_pares(cand)
    par_w = {int(r["iw"]): r for _, r in esc.iterrows()}
    par_l = {int(r["il"]) for _, r in esc.iterrows()}

    campos = ["nome", "nome_chave", "genero", "data_nascimento", "telefone", "telefone_chave", "email", "cpf"]
    registros = []
    for iw, rw in wd.iterrows():                      # 1) todos do WebDiet
        rec = {c: rw[c] for c in campos}
        rec.update(id_webdiet=rw["id_webdiet"], id_liveclin=None, data_cadastro_webdiet=rw["criado_em"],
                   match_confianca=None, genero_divergente=False, nascimento_divergente=False,
                   qtd_cadastros_webdiet=int(rw["qtd_cadastros_webdiet"]))
        if iw in par_w:
            il = int(par_w[iw]["il"]); rl = lc.loc[il]
            rec["id_liveclin"] = int(rl["id_liveclin"])
            rec["match_confianca"] = par_w[iw]["confianca"]
            for c in ("telefone", "telefone_chave", "email", "cpf", "data_nascimento"):
                if not preenchido(rec[c]) and c in lc.columns:
                    rec[c] = rl[c]                    # preenche lacunas com o outro sistema
            rec["genero_divergente"] = bool(preenchido(rw["genero"]) and preenchido(rl["genero"])
                                            and rw["genero"] != rl["genero"])
            rec["nascimento_divergente"] = bool(preenchido(rw["data_nascimento"]) and preenchido(rl["data_nascimento"])
                                                and rw["data_nascimento"] != rl["data_nascimento"])
        registros.append(rec)
    for il, rl in lc.iterrows():                      # 2) quem só existe no LiveClin
        if il in par_l:
            continue
        rec = {c: (rl[c] if c in lc.columns else None) for c in campos}
        rec.update(id_webdiet=None, id_liveclin=int(rl["id_liveclin"]), data_cadastro_webdiet=pd.NaT,
                   match_confianca=None, genero_divergente=False, nascimento_divergente=False,
                   qtd_cadastros_webdiet=0)
        registros.append(rec)

    dim = pd.DataFrame(registros)
    dim["id_liveclin"] = dim["id_liveclin"].astype("Int64")
    tem_wd, tem_lc = dim["id_webdiet"].notna(), dim["id_liveclin"].notna()
    dim["origem"] = "liveclin"
    dim.loc[tem_wd & ~tem_lc, "origem"] = "webdiet"
    dim.loc[tem_wd & tem_lc, "origem"] = "ambos"
    # ID numerado pela ordem de cadastro (e não pelo nome): não revela nada sobre a pessoa
    dim = dim.sort_values(["data_cadastro_webdiet", "id_liveclin", "id_webdiet"],
                          na_position="last", kind="stable").reset_index(drop=True)
    dim.insert(0, "id_paciente", [f"P{i + 1:04d}" for i in range(len(dim))])

    rev = cand[cand["confianca"] == "revisar"].copy()
    rev["id_webdiet"] = rev["iw"].map(wd["id_webdiet"])
    rev["nome_webdiet"] = rev["iw"].map(wd["nome"])
    rev["id_liveclin"] = rev["il"].map(lc["id_liveclin"]).astype("int64")
    rev["nome_liveclin"] = rev["il"].map(lc["nome"])
    rev = rev[["id_webdiet", "nome_webdiet", "id_liveclin", "nome_liveclin",
               "sim_nome", "s_tel", "s_email", "s_nasc"]].reset_index(drop=True)
    return dim, rev

# COMMAND ----------

# 5. Executar e gravar
# Lê a bronze, limpa, corrige, unifica e grava as tabelas silver ------------------
b_wd = spark.table("workspace.nutri.bronze_webdiet_pacientes").toPandas()
b_lc = spark.table("workspace.nutri.bronze_liveclin_pacientes").toPandas()

wd = deduplicar_webdiet(silver_webdiet(b_wd))
lc = corrigir_start(silver_liveclin(b_lc))

dim, rev = unificar(wd, lc)

# fato de planos: os campos do LiveClin ligados ao ID único do paciente
mapa = dim.dropna(subset=["id_liveclin"]).set_index("id_liveclin")["id_paciente"]
campos_plano = ["plano", "status_cliente", "data_fim_plano", "dias_restantes", "duracao_dias",
                "questionarios_enviados", "respostas", "taxa_resposta", "ultima_resposta",
                "whatsapp_validado", "data_referencia",
                "duracao_dias_original", "data_fim_plano_original", "ajuste_start_30d"]
planos = lc[["id_liveclin"] + campos_plano].copy()
planos.insert(0, "id_paciente", planos["id_liveclin"].map(mapa))

def gravar(df, nome, schema=None):
    sdf = spark.createDataFrame(para_spark(df), schema=schema) if schema else spark.createDataFrame(para_spark(df))
    sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"workspace.nutri.{nome}")
    print(f"{nome:28s} {len(df):4d} linhas")

gravar(wd, "silver_webdiet_pacientes")
gravar(lc, "silver_liveclin_pacientes")
gravar(dim, "silver_pacientes")
gravar(planos, "silver_planos")
gravar(rev, "silver_pares_revisao",
       schema="id_webdiet string, nome_webdiet string, id_liveclin long, nome_liveclin string, "
              "sim_nome double, s_tel boolean, s_email boolean, s_nasc boolean")

print("\nplanos Start corrigidos:", int(lc["ajuste_start_30d"].sum()))
print("pacientes por origem:", dim["origem"].value_counts().to_dict())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Gold: situação de cada paciente

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.nutri.gold_pacientes AS
# MAGIC WITH
# MAGIC params AS (          -- limites de negócio: ajuste aqui, em um só lugar
# MAGIC   SELECT 30  AS dias_renovacao,    -- plano ativo que vence em até X dias
# MAGIC          60  AS dias_recente,      -- plano encerrado há até X dias
# MAGIC          180 AS dias_risco         -- encerrado há até X dias = em risco; acima = inativo
# MAGIC ),
# MAGIC ref AS (SELECT MAX(data_referencia) AS data_ref FROM workspace.nutri.silver_planos),
# MAGIC base AS (
# MAGIC   SELECT
# MAGIC     p.id_paciente,
# MAGIC     p.genero,
# MAGIC     p.origem,
# MAGIC     date_format(p.data_cadastro_webdiet, 'yyyy-MM')                       AS mes_cadastro,
# MAGIC     CAST(floor(datediff(r.data_ref, p.data_nascimento) / 365.25) AS INT)  AS idade,
# MAGIC     pl.plano,
# MAGIC     pl.status_cliente,
# MAGIC     pl.data_fim_plano,
# MAGIC     pl.dias_restantes,
# MAGIC     datediff(r.data_ref, pl.data_fim_plano)                               AS dias_desde_fim,
# MAGIC     pl.duracao_dias,
# MAGIC     pl.questionarios_enviados,
# MAGIC     pl.respostas,
# MAGIC     pl.taxa_resposta,
# MAGIC     pl.ultima_resposta,
# MAGIC     r.data_ref
# MAGIC   FROM workspace.nutri.silver_pacientes p
# MAGIC   CROSS JOIN ref r
# MAGIC   LEFT JOIN workspace.nutri.silver_planos pl ON pl.id_paciente = p.id_paciente
# MAGIC )
# MAGIC SELECT
# MAGIC   b.*,
# MAGIC   CASE
# MAGIC     WHEN idade IS NULL THEN 'sem data'
# MAGIC     WHEN idade < 18    THEN '< 18'
# MAGIC     WHEN idade < 30    THEN '18-29'
# MAGIC     WHEN idade < 45    THEN '30-44'
# MAGIC     WHEN idade < 60    THEN '45-59'
# MAGIC     ELSE '60+'
# MAGIC   END AS faixa_etaria,
# MAGIC   CASE
# MAGIC     WHEN b.status_cliente IS NULL                                  THEN '7. Sem plano no LiveClin'
# MAGIC     WHEN b.status_cliente = 'active'
# MAGIC          AND b.dias_restantes <= pr.dias_renovacao                 THEN '2. Ativo - renovação próxima'
# MAGIC     WHEN b.status_cliente = 'active'                               THEN '1. Ativo'
# MAGIC     WHEN b.data_fim_plano IS NULL                                  THEN '6. Plano nunca iniciado'
# MAGIC     WHEN b.dias_desde_fim <= pr.dias_recente                       THEN '3. Encerrado recente'
# MAGIC     WHEN b.dias_desde_fim <= pr.dias_risco
# MAGIC          AND b.status_cliente <> 'inactive'                        THEN '4. Em risco'
# MAGIC     ELSE '5. Inativo'
# MAGIC   END AS situacao
# MAGIC FROM base b CROSS JOIN params pr;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT situacao, COUNT(*) AS pacientes
# MAGIC FROM workspace.nutri.gold_pacientes GROUP BY 1 ORDER BY 1;

# COMMAND ----------

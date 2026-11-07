# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03 · Consultas (bronze + silver)
# MAGIC Carrega o relatório de consultas do WebDiet e liga cada consulta ao `id_paciente`.
# MAGIC Antes de rodar: suba o arquivo `relatorio_consultas_webdiet.xlsx` no volume `workspace.nutri.raw`.
# MAGIC Precisa que o notebook `02_silver` já tenha rodado (usa a tabela `silver_pacientes`).

# COMMAND ----------

# MAGIC %pip install openpyxl -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# 1. Bronze: copia o arquivo como veio (só nomes de coluna padronizados)
import re, unicodedata
import pandas as pd
from datetime import datetime, timezone

VOL = "/Volumes/workspace/nutri/raw"
ARQ = "relatorio_consultas_webdiet.xlsx"
DATA_REF = "2026-11-06"          # data em que o relatório foi exportado
carga = datetime.now(timezone.utc).isoformat(timespec="seconds")

def snake(c):
    c = unicodedata.normalize("NFKD", c.split(".")[-1]).encode("ascii", "ignore").decode()
    return re.sub(r"\W+", "_", c.lower()).strip("_")

raw = pd.read_excel(f"{VOL}/{ARQ}", dtype=str)
raw.columns = [snake(c) for c in raw.columns]
raw = raw.dropna(axis=1, how="all")                       # colunas totalmente vazias (observacao)
raw = raw.astype(object).where(raw.notna(), None)
raw["_arquivo_origem"], raw["_data_referencia"], raw["_carregado_em"] = ARQ, DATA_REF, carga
(spark.createDataFrame(raw).write.mode("overwrite").option("overwriteSchema", "true")
      .saveAsTable("workspace.nutri.bronze_webdiet_consultas"))
print("bronze_webdiet_consultas", len(raw), "linhas |", len(raw.columns), "colunas")

# COMMAND ----------

# 2. Silver: datas de verdade, sem duplicadas, ligadas ao id_paciente
import re, unicodedata
import pandas as pd

PARTICULAS = {"de", "da", "do", "das", "dos", "e"}
def normalizar_texto(v):
    if pd.isna(v):
        return ""
    s = unicodedata.normalize("NFKD", str(v)).encode("ascii", "ignore").decode()
    return re.sub(r"\s+", " ", s.lower().strip())
def chave_nome(v):
    return " ".join(t for t in normalizar_texto(v).split() if t not in PARTICULAS)
def para_spark(df):
    return df.astype(object).where(df.notna(), None)

b = spark.table("workspace.nutri.bronze_webdiet_consultas").toPandas()
pac = spark.table("workspace.nutri.silver_pacientes").select("id_paciente", "nome_chave").toPandas()

c = pd.DataFrame()
c["nome_relatorio"] = b["nome_do_paciente"].str.strip()
c["nome_chave"] = b["nome_do_paciente"].map(chave_nome)
c["data_consulta"] = pd.to_datetime(b["data"], format="%d/%m/%Y", errors="coerce")
invalidas = int(c["data_consulta"].isna().sum())
c = c.dropna(subset=["data_consulta"])
repetidas = int(c.duplicated(["nome_chave", "data_consulta"]).sum())
c = c.drop_duplicates(["nome_chave", "data_consulta"])      # mesma pessoa, mesmo dia = 1 consulta

# ligação ao paciente: só pelo nome (o relatório não traz ID nem telefone)
ambiguos = set(pac.loc[pac["nome_chave"].duplicated(keep=False), "nome_chave"])
mapa = pac.drop_duplicates("nome_chave", keep=False).set_index("nome_chave")["id_paciente"]
c["id_paciente"] = c["nome_chave"].map(mapa)
c["motivo_sem_id"] = None
c.loc[c["id_paciente"].isna() & c["nome_chave"].isin(ambiguos), "motivo_sem_id"] = "nome repetido na base"
c.loc[c["id_paciente"].isna() & c["motivo_sem_id"].isna(), "motivo_sem_id"] = "nome não encontrado"

c = c.sort_values(["nome_chave", "data_consulta"]).reset_index(drop=True)
c["numero_consulta_no_relatorio"] = c.groupby("nome_chave").cumcount() + 1
c["dias_desde_anterior"] = c.groupby("nome_chave")["data_consulta"].diff().dt.days.astype("Int64")
c["data_consulta"] = c["data_consulta"].dt.date

ok = c[c["id_paciente"].notna()].drop(columns=["motivo_sem_id"])
sem = c[c["id_paciente"].isna()][["nome_relatorio", "data_consulta", "motivo_sem_id"]]

def gravar(df, nome, schema=None):
    sdf = spark.createDataFrame(para_spark(df), schema=schema) if schema else spark.createDataFrame(para_spark(df))
    sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"workspace.nutri.{nome}")
    print(f"{nome:34s} {len(df):4d} linhas")

gravar(ok, "silver_consultas")
gravar(sem, "silver_consultas_sem_paciente",
       schema="nome_relatorio string, data_consulta date, motivo_sem_id string")
print("\ndatas inválidas:", invalidas, "| linhas repetidas removidas:", repetidas)
print("pacientes distintos com consulta:", ok["id_paciente"].nunique())
print("período:", ok["data_consulta"].min(), "a", ok["data_consulta"].max())

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.nutri.gold_consultas_pacientes AS
# MAGIC WITH
# MAGIC params AS (
# MAGIC   SELECT 1  AS cota_start,         -- consultas REGISTRADAS por plano (a online dos 7 dias do Keep não é registrada)
# MAGIC          2  AS cota_keep,
# MAGIC          6  AS cota_excellence,
# MAGIC          45 AS espaco_keep,        -- dias típicos entre consultas (mediana observada)
# MAGIC          36 AS espaco_excellence,
# MAGIC          3  AS tolerancia_dias,    -- folga nas bordas da janela do plano
# MAGIC          90 AS horizonte_dias
# MAGIC ),
# MAGIC planos AS (
# MAGIC   SELECT g.id_paciente, g.plano, g.status_cliente, g.data_ref, g.data_fim_plano, g.dias_restantes,
# MAGIC            date_sub(g.data_fim_plano, CAST(g.duracao_dias AS INT)) AS inicio_plano
# MAGIC   FROM workspace.nutri.gold_pacientes g
# MAGIC   WHERE g.plano IS NOT NULL AND g.data_fim_plano IS NOT NULL
# MAGIC ),
# MAGIC no_plano AS (
# MAGIC   SELECT p.id_paciente,
# MAGIC          COUNT(c.data_consulta) AS consultas_no_plano,
# MAGIC          MAX(c.data_consulta)   AS ultima_no_plano
# MAGIC   FROM planos p CROSS JOIN params pr
# MAGIC   LEFT JOIN workspace.nutri.silver_consultas c
# MAGIC     ON c.id_paciente = p.id_paciente
# MAGIC    AND c.data_consulta BETWEEN date_sub(p.inicio_plano, pr.tolerancia_dias)
# MAGIC                            AND date_add(p.data_fim_plano, pr.tolerancia_dias)
# MAGIC   GROUP BY p.id_paciente
# MAGIC ),
# MAGIC geral AS (
# MAGIC   SELECT id_paciente,
# MAGIC          COUNT(*)                                  AS consultas_total,
# MAGIC          MIN(data_consulta)                        AS primeira_consulta,
# MAGIC          MAX(data_consulta)                        AS ultima_consulta,
# MAGIC          percentile_approx(dias_desde_anterior, 0.5) AS intervalo_mediano_dias
# MAGIC   FROM workspace.nutri.silver_consultas
# MAGIC   GROUP BY id_paciente
# MAGIC ),
# MAGIC calc AS (
# MAGIC   SELECT
# MAGIC     g.id_paciente, g.situacao, g.plano, g.status_cliente, g.data_ref,
# MAGIC     gr.primeira_consulta, gr.ultima_consulta,
# MAGIC     COALESCE(gr.consultas_total, 0) AS consultas_total,
# MAGIC     datediff(g.data_ref, gr.ultima_consulta) AS dias_desde_ultima,
# MAGIC     gr.intervalo_mediano_dias,
# MAGIC     p.inicio_plano, p.data_fim_plano,
# MAGIC     COALESCE(np.consultas_no_plano, 0) AS consultas_no_plano,
# MAGIC     np.ultima_no_plano,
# MAGIC     CASE g.plano WHEN 'Start' THEN pr.cota_start WHEN 'Keep' THEN pr.cota_keep
# MAGIC                  WHEN 'Excellence' THEN pr.cota_excellence END                AS cota_registrada,
# MAGIC     CASE g.plano WHEN 'Keep' THEN pr.espaco_keep WHEN 'Excellence' THEN pr.espaco_excellence
# MAGIC                  ELSE 1 END                                                   AS espaco_dias,
# MAGIC     g.dias_restantes, pr.horizonte_dias
# MAGIC   FROM workspace.nutri.gold_pacientes g
# MAGIC   CROSS JOIN params pr
# MAGIC   LEFT JOIN planos p    ON p.id_paciente  = g.id_paciente
# MAGIC   LEFT JOIN no_plano np ON np.id_paciente = g.id_paciente
# MAGIC   LEFT JOIN geral gr    ON gr.id_paciente = g.id_paciente
# MAGIC ),
# MAGIC ativos AS (
# MAGIC   SELECT *,
# MAGIC          GREATEST(cota_registrada - consultas_no_plano, 0) AS consultas_restantes,
# MAGIC          CASE WHEN consultas_no_plano = 0 THEN inicio_plano
# MAGIC               ELSE date_add(ultima_no_plano, espaco_dias) END AS data_devida
# MAGIC   FROM calc
# MAGIC   WHERE status_cliente = 'active' AND cota_registrada IS NOT NULL
# MAGIC )
# MAGIC SELECT
# MAGIC   c.id_paciente, c.situacao, c.plano, c.status_cliente, c.data_ref,
# MAGIC   c.consultas_total, c.primeira_consulta, c.ultima_consulta, c.dias_desde_ultima, c.intervalo_mediano_dias,
# MAGIC   c.consultas_no_plano, c.cota_registrada,
# MAGIC   a.consultas_restantes,
# MAGIC   CASE WHEN a.consultas_restantes > 0
# MAGIC        THEN LEAST(GREATEST(a.data_devida, c.data_ref), c.data_fim_plano) END                AS proxima_consulta_prevista,
# MAGIC   CASE WHEN a.id_paciente IS NULL            THEN NULL
# MAGIC        WHEN a.consultas_restantes = 0         THEN 'Cota usada'
# MAGIC        WHEN a.data_devida < c.data_ref        THEN 'Atrasada'
# MAGIC        ELSE 'Em dia' END                                                                    AS situacao_consulta,
# MAGIC   CASE WHEN a.id_paciente IS NOT NULL
# MAGIC        THEN ROUND(LEAST(a.consultas_restantes,
# MAGIC                         LEAST(c.dias_restantes, c.horizonte_dias) / c.espaco_dias), 1) END AS consultas_esperadas_90d
# MAGIC FROM calc c
# MAGIC LEFT JOIN ativos a ON a.id_paciente = c.id_paciente;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS pacientes,
# MAGIC        SUM(CASE WHEN consultas_total > 0 THEN 1 ELSE 0 END) AS com_consulta
# MAGIC FROM workspace.nutri.gold_consultas_pacientes;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW workspace.nutri.silver_lista_agendar AS
# MAGIC SELECT
# MAGIC   CASE c.situacao_consulta WHEN 'Atrasada' THEN 'A. Atrasada' ELSE 'B. Em dia' END AS prioridade,
# MAGIC   c.id_paciente,
# MAGIC   p.nome,
# MAGIC   p.telefone,
# MAGIC   CASE WHEN length(p.telefone) = 11 AND COALESCE(l.telefone_qualidade, '') <> 'internacional'
# MAGIC        THEN concat('https://wa.me/55', p.telefone) END AS link_whatsapp,
# MAGIC   c.plano,
# MAGIC   c.consultas_no_plano,
# MAGIC   c.cota_registrada,
# MAGIC   c.consultas_restantes,
# MAGIC   c.ultima_consulta,
# MAGIC   c.proxima_consulta_prevista,
# MAGIC   datediff(c.proxima_consulta_prevista, c.data_ref) AS dias_ate_proxima,
# MAGIC   g.data_fim_plano,
# MAGIC   g.dias_restantes AS dias_ate_fim_do_plano
# MAGIC FROM workspace.nutri.gold_consultas_pacientes c
# MAGIC JOIN workspace.nutri.gold_pacientes g          ON g.id_paciente = c.id_paciente
# MAGIC JOIN workspace.nutri.silver_pacientes p        ON p.id_paciente = c.id_paciente
# MAGIC LEFT JOIN workspace.nutri.silver_liveclin_pacientes l ON l.id_liveclin = p.id_liveclin
# MAGIC WHERE c.situacao_consulta IN ('Atrasada', 'Em dia');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.nutri.silver_lista_agendar
# MAGIC ORDER BY prioridade, proxima_consulta_prevista;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.nutri.gold_consultas_pacientes ORDER BY id_paciente;

# COMMAND ----------

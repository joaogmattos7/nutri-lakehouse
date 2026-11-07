# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze
# MAGIC Carrega as exportações dos dois sistemas **como vieram**: só padroniza os nomes das colunas e
# MAGIC acrescenta metadados de linhagem (`_arquivo_origem`, `_data_referencia`, `_carregado_em`).
# MAGIC
# MAGIC Antes de rodar: suba os arquivos no volume `workspace.nutri.raw`
# MAGIC (para testar, use os arquivos fictícios da pasta `dados_exemplo/` do repositório).

# COMMAND ----------

# MAGIC %pip install openpyxl -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import re, unicodedata
import pandas as pd
from datetime import datetime, timezone

VOL = "/Volumes/workspace/nutri/raw"
DATA_REF = "2026-11-06"          # data em que os relatórios foram exportados
ARQ_WEBDIET = "pacientes_webdiet.xlsx"
ARQ_LIVECLIN = "pacientes_liveclin.csv"
carga = datetime.now(timezone.utc).isoformat(timespec="seconds")

def snake(c):
    c = c.split(".")[-1]                      # remove o prefixo 'patientReport.headers.' do LiveClin
    c = unicodedata.normalize("NFKD", c).encode("ascii", "ignore").decode()
    return re.sub(r"\W+", "_", c.lower()).strip("_")

def gravar_bronze(df, tabela, arquivo, data_ref):
    df.columns = [snake(c) for c in df.columns]
    df = df.dropna(axis=1, how="all")         # colunas totalmente vazias
    df = df.astype(object).where(df.notna(), None)
    df["_arquivo_origem"], df["_data_referencia"], df["_carregado_em"] = arquivo, data_ref, carga
    (spark.createDataFrame(df).write.mode("overwrite")
          .option("overwriteSchema", "true").saveAsTable(f"workspace.nutri.{tabela}"))
    print(tabela, len(df), "linhas |", len(df.columns), "colunas")

gravar_bronze(pd.read_excel(f"{VOL}/{ARQ_WEBDIET}", dtype=str),
              "bronze_webdiet_pacientes", ARQ_WEBDIET, DATA_REF)

gravar_bronze(pd.read_csv(f"{VOL}/{ARQ_LIVECLIN}", dtype=str, encoding="utf-8-sig"),
              "bronze_liveclin_pacientes", ARQ_LIVECLIN, DATA_REF)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT table_name, COUNT(*) AS colunas
# MAGIC FROM workspace.information_schema.columns
# MAGIC WHERE table_schema = 'nutri' AND table_name LIKE 'bronze%'
# MAGIC GROUP BY table_name;

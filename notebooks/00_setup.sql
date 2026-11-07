-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 · Setup
-- MAGIC Cria o schema e o volume onde os arquivos exportados dos sistemas (WebDiet e LiveClin) são carregados.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS workspace.nutri;
CREATE VOLUME IF NOT EXISTS workspace.nutri.raw;

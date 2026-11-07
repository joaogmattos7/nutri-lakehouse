# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Lista de contato e análises
# MAGIC Transforma a `gold_pacientes` em ação: quem contatar primeiro para **renovar** ou **reativar**,
# MAGIC e as análises de sazonalidade, retenção e engajamento.
# MAGIC Precisa que `02_silver` já tenha rodado.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW workspace.nutri.silver_lista_contato AS
# MAGIC WITH params AS (
# MAGIC   SELECT 15 AS dias_antecedencia,  -- contatar X dias antes do plano acabar
# MAGIC          30 AS dias_recente_p2     -- encerrados há até X dias têm prioridade alta
# MAGIC )
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN g.situacao = '2. Ativo - renovação próxima' AND g.dias_restantes <= pr.dias_antecedencia THEN 'P1'
# MAGIC     WHEN g.situacao = '3. Encerrado recente'         AND g.dias_desde_fim <= pr.dias_recente_p2  THEN 'P2'
# MAGIC     WHEN g.situacao = '2. Ativo - renovação próxima'                                             THEN 'P3'
# MAGIC     WHEN g.situacao = '3. Encerrado recente'                                                     THEN 'P4'
# MAGIC     WHEN g.situacao = '4. Em risco'                                                              THEN 'P5'
# MAGIC   END AS prioridade,
# MAGIC   CASE
# MAGIC     WHEN g.status_cliente = 'active'         THEN 'Renovar o plano antes de acabar'
# MAGIC     WHEN g.situacao = '3. Encerrado recente' THEN 'Plano acabou há pouco: oferecer continuidade'
# MAGIC     ELSE 'Sem retorno há meses: tentar reativar'
# MAGIC   END AS motivo,
# MAGIC   g.id_paciente,
# MAGIC   p.nome,
# MAGIC   p.telefone,
# MAGIC   CASE WHEN length(p.telefone) = 11 AND COALESCE(l.telefone_qualidade, '') <> 'internacional'
# MAGIC        THEN concat('https://wa.me/55', p.telefone) END AS link_whatsapp,
# MAGIC   g.plano,
# MAGIC   g.data_fim_plano,
# MAGIC   g.dias_restantes,
# MAGIC   g.dias_desde_fim,
# MAGIC   g.taxa_resposta,
# MAGIC   g.ultima_resposta,
# MAGIC   CASE WHEN g.status_cliente = 'active'
# MAGIC        THEN date_sub(g.data_fim_plano, pr.dias_antecedencia)
# MAGIC        ELSE g.data_ref END AS contatar_a_partir_de
# MAGIC FROM workspace.nutri.gold_pacientes g
# MAGIC JOIN workspace.nutri.silver_pacientes p ON p.id_paciente = g.id_paciente
# MAGIC LEFT JOIN workspace.nutri.silver_liveclin_pacientes l ON l.id_liveclin = p.id_liveclin
# MAGIC CROSS JOIN params pr
# MAGIC WHERE g.situacao IN ('2. Ativo - renovação próxima', '3. Encerrado recente', '4. Em risco');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quantos pacientes em cada prioridade

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT prioridade, motivo, COUNT(*) AS pacientes
# MAGIC FROM workspace.nutri.silver_lista_contato
# MAGIC GROUP BY 1, 2 ORDER BY 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Situação da carteira

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT situacao, COUNT(*) AS pacientes,
# MAGIC        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
# MAGIC FROM workspace.nutri.gold_pacientes
# MAGIC GROUP BY situacao ORDER BY situacao;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sazonalidade: pacientes novos por mês, comparando os anos

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CAST(substr(mes_cadastro, 6, 2) AS INT) AS mes,
# MAGIC        SUM(CASE WHEN substr(mes_cadastro, 1, 4) = '2024' THEN 1 ELSE 0 END) AS a2024,
# MAGIC        SUM(CASE WHEN substr(mes_cadastro, 1, 4) = '2025' THEN 1 ELSE 0 END) AS a2025,
# MAGIC        SUM(CASE WHEN substr(mes_cadastro, 1, 4) = '2026' THEN 1 ELSE 0 END) AS a2026,
# MAGIC        COUNT(*) AS total
# MAGIC FROM workspace.nutri.gold_pacientes
# MAGIC WHERE mes_cadastro IS NOT NULL
# MAGIC GROUP BY 1 ORDER BY 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Engajamento (resposta aos questionários) por situação

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT situacao, COUNT(*) AS pacientes,
# MAGIC        ROUND(AVG(taxa_resposta), 1)          AS taxa_resposta_media,
# MAGIC        ROUND(AVG(questionarios_enviados), 1) AS questionarios_enviados_medio
# MAGIC FROM workspace.nutri.gold_pacientes
# MAGIC WHERE questionarios_enviados > 0
# MAGIC GROUP BY situacao ORDER BY situacao;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Qualidade do pareamento entre os sistemas

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT origem, match_confianca, COUNT(*) AS pacientes
# MAGIC FROM workspace.nutri.silver_pacientes
# MAGIC GROUP BY 1, 2 ORDER BY 1, 2;

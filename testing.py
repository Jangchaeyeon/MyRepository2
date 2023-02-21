# Databricks notebook source


# COMMAND ----------

import sys
print("\n".join(sys.path))

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from sample import n_to_mth
n_to_mth(3, 4)

# COMMAND ----------

import sample

# COMMAND ----------

from sample import n_to_mth

# COMMAND ----------

n_to_mth(3, 4)

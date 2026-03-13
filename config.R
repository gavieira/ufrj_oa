# 1. Definição de variáveis do projeto

anos_para_baixar <- 2001:2025  # Defina o intervalo analisado aqui
dir_dados <- "dados"
dir_dados_raw   <- file.path(dir_dados, "raw")
dados_processados <- file.path(dir_dados, "ufrj_oplx.rds")
dados_processados_csv <- file.path(dir_dados, "ufrj_data_zenodo.csv")
dados_processados_parquet <- file.path(dir_dados, "ufrj_data_completo.parquet")
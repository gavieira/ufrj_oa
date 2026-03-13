library(httr2)
library(jsonlite)
library(tidyverse)
library(arrow)
source("config.R")

##### Funcao para extrair dados da openalex via api #####

extrair_ufrj_por_ano <- function(id_institucional, ano, email_contato) {
  base_url <- "https://api.openalex.org/works"
  
  req_base <- request(base_url) %>%
    req_headers("User-Agent" = paste0("mailto:", email_contato)) %>%
    req_url_query(
      filter = paste0("authorships.institutions.lineage:", id_institucional, 
                      ",publication_year:", ano),
      select = "id,title,doi,publication_year,cited_by_count,counts_by_year,fwci,countries_distinct_count,institutions_distinct_count,language,locations_count,topics,type,open_access",
      `per-page` = 200
    )
  
  # Aqui guardaremos os DATAFRAMES de cada página
  paginas_df <- list()
  cursor_atual <- "*"
  
  repeat {
    message(paste("Coletando ano", ano, "- Cursor:", substr(cursor_atual, 1, 10), "..."))
    
    resp <- req_base %>%
      req_url_query(cursor = cursor_atual) %>%
      req_perform()
    
    # Pegamos o conteúdo como texto para o jsonlite processar o achatamento (flatten)
    corpo_texto <- resp %>% resp_body_string()
    dados_pagina <- fromJSON(corpo_texto, flatten = TRUE)
    
    # Se não houver mais resultados, paramos
    if (length(dados_pagina$results) == 0) break
    
    # Guardamos o dataframe desta página na nossa lista
    paginas_df[[length(paginas_df) + 1]] <- as.data.frame(dados_pagina$results)
    
    proximo_cursor <- dados_pagina$meta$next_cursor
    if (is.null(proximo_cursor) || cursor_atual == proximo_cursor) break
    
    cursor_atual <- proximo_cursor
  }
  
  # Agora sim: rbind_pages em uma lista de dataframes
  df_final <- jsonlite::rbind_pages(paginas_df)
  return(as_tibble(df_final))
}

##### Baixando e salvando os dados #####
 
# 1. Definição dos parâmetros de captura importados de config.R no topo do script

# Criar a pasta se não existir
if (!dir.exists(dir_dados_raw)) dir.create(dir_dados_raw, recursive = TRUE)

# 2. Loop de execução
for (ano in anos_para_baixar) {
  
  arquivo_nome <- file.path(dir_dados_raw, paste0("openalex_ufrj_", ano, ".rds"))
  
  # Verificação opcional: não baixar se o arquivo já existir (útil se a net cair)
  if (file.exists(arquivo_nome)) {
    message(paste("Pulando ano", ano, "- Arquivo já existe."))
    next
  }
  
  message(paste("--- Iniciando captura da UFRJ - Ano:", ano, "---"))
  
  # Tenta baixar os dados
  tryCatch({
    dados_ano <- extrair_ufrj_por_ano(id_ufrj, ano, email)
    
    # Salva o arquivo .rds (preserva tipos de dados do R)
    saveRDS(dados_ano, file = arquivo_nome)
    
    message(paste("Sucesso! Salvo em:", arquivo_nome))
    
    # Pausa breve para ser "polite" com a API, mesmo no Polite Pool
    Sys.sleep(1) 
    
  }, error = function(e) {
    message(paste("Erro ao baixar o ano", ano, ":", e$message))
  })
}



##### Fundindo registros em dataframe único #####

# 1. Listar todos os caminhos dos arquivos .rds na pasta raw
arquivos <- list.files(dir_dados_raw, pattern = "\\.rds$", full.names = TRUE)

# 2. Ler todos e empilhar em um único tibble
# O map_dfr do purrr (ou bind_rows + lapply) é excelente aqui
df_ufrj_consolidado <- arquivos %>%
  map(readRDS) %>%
  bind_rows()

# 3. Verificar o resultado
message(paste("Total de registros consolidados:", nrow(df_ufrj_consolidado)))
glimpse(df_ufrj_consolidado)


##### Limpando o dataframe #####

df_ufrj_final <- df_ufrj_consolidado %>%
  #Extraindo a informacao sobre os dominios
  mutate(
    # Criamos uma coluna que contém vetores de strings
    dominios_nomes = map(topics, function(df_interno) {
      if (is.null(df_interno) || !is.data.frame(df_interno) || nrow(df_interno) == 0) {
        return(NA_character_) # Retorna NA se não houver dados
      }
      
      # Extraímos apenas a coluna do nome do domínio
      # unique() garante que se 3 tópicos forem "Life Sciences", 
      # guardaremos o nome apenas uma vez.
      df_interno %>%
        pull(domain.display_name) %>% 
        unique()
    }) 
  ) %>%
  # 1. Renomear as colunas de Open Access # A sintaxe é: novo_nome = nome_antigo
  rename(
    is_oa = open_access.is_oa,
    oa_status = open_access.oa_status
  ) %>%
  # 2. Remover o que não é mais necessário
  select(
    -starts_with("open_access."), # Remove todos os outros campos (oa_url, etc)
    -topics                        # Remove a coluna complexa original
  )  %>%
  # 3. Filtrar apenas os 'articles' e 'reviews'
  filter(
    type %in% c('article', 'review')
  )

saveRDS(df_ufrj_final, file = dados_processados, compress = "xz")


######## Geracao arquivos do zenodo (csv e parquet) ########


#### CSV

# Versão "segura" da função toJSON que retorna NA se falhar
safe_json <- possibly(function(x) {
  if (is.null(x) || (is.logical(x) && all(is.na(x))) || !is.data.frame(x) || nrow(x) == 0) {
    return(NA_character_)
  }
  return(as.character(toJSON(x)))
}, otherwise = NA_character_)

ufrj_csv_completo <- ufrj_data %>%
  mutate(
    # Agora usamos a função segura para o histórico
    counts_by_year = map_chr(counts_by_year, safe_json),
    
    # Tratamento para domínios (simplificado e seguro)
    dominios_nomes = map_chr(dominios_nomes, ~ {
      # Verificamos se é uma lista válida de strings
      val <- unlist(.x)
      if (is.null(val) || all(is.na(val)) || length(val) == 0) {
        return(NA_character_)
      }
      paste(unique(trimws(as.character(val))), collapse = "; ")
    })
  )

# 2. Exportação

write.table(ufrj_csv_completo, dados_processados_csv, 
            sep = ";", 
            dec = ",", 
            row.names = FALSE, 
            quote = TRUE,
            qmethod = "double",
            fileEncoding = "UTF-8")


#### PARQUET

# 1. Normalizando a estrutura para o Arrow
ufrj_data_parquet <- ufrj_data %>%
  mutate(
    counts_by_year = map(counts_by_year, ~ {
      # Se for nulo ou não for um data.frame, cria um df vazio com as colunas corretas
      if (is.null(.x) || !is.data.frame(.x) || nrow(.x) == 0) {
        return(data.frame(year = integer(), cited_by_count = integer()))
      }
      # Garante que as colunas sejam exatamente o que o Arrow espera
      as.data.frame(.x) %>% select(year, cited_by_count)
    })
  )

# 2. Agora o write_parquet deve funcionar perfeitamente
write_parquet(ufrj_data_parquet, dados_processados_parquet)

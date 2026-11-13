source('config.R')
library(tidyverse)

# Modelos exploratorios: relacao entre status de acesso aberto e impacto (FWCI),
# controlando por colaboracao, dominio, tipo de documento, idioma e ano.

ufrj_data <- readRDS(dados_processados)

df_modelo <- ufrj_data %>%
  filter(!is.na(fwci), publication_year <= 2023) %>%  # exclui anos com janela de citacao muito curta
  mutate(
    dominio = map_chr(dominios_nomes, ~ {
      val <- unlist(.x)
      if (length(val) == 0 || is.na(val[1])) NA_character_ else val[1]
    }),
    colaboracao = case_when(
      countries_distinct_count > 1 ~ "internacional",
      institutions_distinct_count > 1 ~ "nacional",
      TRUE ~ "ufrj"
    ),
    oa_status = relevel(factor(oa_status), ref = "closed"),
    colaboracao = relevel(factor(colaboracao), ref = "ufrj"),
    ingles = as.integer(language %in% "en"),
    fwci_zero = as.integer(fwci == 0),
    log_fwci = log1p(fwci)
  ) %>%
  filter(!is.na(dominio))

formula_controles <- ~ oa_status + colaboracao + dominio + type + ingles + factor(publication_year)

# 1. Sem controles (apenas ano): efeito "bruto" do status de acesso
m_bruto <- lm(log_fwci ~ oa_status + factor(publication_year), data = df_modelo)

# 2. Com controles: log(1 + FWCI)
m_log <- lm(update(formula_controles, log_fwci ~ .), data = df_modelo)

# 3. Com controles: probabilidade de FWCI = 0 (logit)
m_zero <- glm(update(formula_controles, fwci_zero ~ .), data = df_modelo, family = binomial)

extrair <- function(m) {
  broom_like <- summary(m)$coefficients
  as_tibble(broom_like, rownames = "termo") %>%
    filter(str_detect(termo, "oa_status|colaboracao|ingles|type"))
}

list(bruto = m_bruto, log_fwci = m_log, fwci_zero = m_zero) %>%
  map(extrair) %>%
  print()

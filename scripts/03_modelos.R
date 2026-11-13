source('config.R')
library(tidyverse)
library(sandwich)
library(lmtest)

# Modelos: relacao entre rota de acesso aberto e impacto (FWCI),
# com e sem controles de idioma, colaboracao, dominio, tipo de documento e ano.
# Erros-padrao robustos a heteroscedasticidade (HC1).

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

controles <- "oa_status + colaboracao + dominio + type + ingles + factor(publication_year)"

# Modelo A: apenas rota de acesso e ano (associacao "bruta")
m_a <- lm(log_fwci ~ oa_status + factor(publication_year), data = df_modelo)

# Modelo B: com controles, log(1 + FWCI)
m_b <- lm(as.formula(paste("log_fwci ~", controles)), data = df_modelo)

# Modelo C: com controles, probabilidade de FWCI = 0 (logit)
m_c <- glm(as.formula(paste("fwci_zero ~", controles)), data = df_modelo, family = binomial)

# Coeficientes com erros-padrao robustos (sem os efeitos fixos de ano)
coef_robusto <- function(modelo, nome) {
  ct <- coeftest(modelo, vcov. = vcovHC(modelo, type = "HC1"))
  tibble(
    modelo = nome,
    termo = rownames(ct),
    estimativa = ct[, 1],
    erro_padrao = ct[, 2],
    p_valor = ct[, 4]
  ) %>%
    filter(!str_detect(termo, "publication_year|Intercept"))
}

tabela_modelos <- bind_rows(
  coef_robusto(m_a, "A"),
  coef_robusto(m_b, "B"),
  coef_robusto(m_c, "C")
)

ajuste <- tibble(
  modelo = c("A", "B", "C"),
  n = c(nobs(m_a), nobs(m_b), nobs(m_c)),
  r2_ajustado = c(summary(m_a)$adj.r.squared, summary(m_b)$adj.r.squared, NA),
  pseudo_r2_mcfadden = c(NA, NA, 1 - m_c$deviance / m_c$null.deviance)
)

print(tabela_modelos, n = Inf)
print(ajuste)

if (!dir.exists("resultados")) dir.create("resultados")
write_csv(tabela_modelos, "resultados/tabela_modelos.csv")
write_csv(ajuste, "resultados/ajuste_modelos.csv")

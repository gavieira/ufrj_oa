source("config.R")
library(tidyverse)
library(sandwich)
library(lmtest)

# Regression models of the association between OA route and citation impact (FWCI),
# with and without controls for language, collaboration, domain, document type and year.
# Heteroscedasticity-robust (HC1) standard errors.

ufrj_data <- readRDS(processed_data)

model_data <- ufrj_data %>%
  filter(!is.na(fwci), publication_year <= 2023) %>%  # drop years with a very short citation window
  mutate(
    # Domain of the first listed topic
    domain = map_chr(dominios_nomes, ~ {
      val <- unlist(.x)
      if (length(val) == 0 || is.na(val[1])) NA_character_ else val[1]
    }),
    collaboration = case_when(
      countries_distinct_count > 1 ~ "international",
      institutions_distinct_count > 1 ~ "national",
      TRUE ~ "ufrj_only"
    ),
    oa_status = relevel(factor(oa_status), ref = "closed"),
    collaboration = relevel(factor(collaboration), ref = "ufrj_only"),
    english = as.integer(language %in% "en"),
    zero_fwci = as.integer(fwci == 0),
    log_fwci = log1p(fwci)
  ) %>%
  filter(!is.na(domain))

controls <- "oa_status + collaboration + domain + type + english + factor(publication_year)"

# Model A: OA route and year only (raw association)
model_a <- lm(log_fwci ~ oa_status + factor(publication_year), data = model_data)

# Model B: with controls, log(1 + FWCI)
model_b <- lm(as.formula(paste("log_fwci ~", controls)), data = model_data)

# Model C: with controls, probability that FWCI = 0 (logit)
model_c <- glm(as.formula(paste("zero_fwci ~", controls)), data = model_data, family = binomial)

# Coefficients with robust standard errors (year fixed effects and intercept omitted)
robust_coefs <- function(model, name) {
  ct <- coeftest(model, vcov. = vcovHC(model, type = "HC1"))
  tibble(
    model = name,
    term = rownames(ct),
    estimate = ct[, 1],
    std_error = ct[, 2],
    p_value = ct[, 4]
  ) %>%
    filter(!str_detect(term, "publication_year|Intercept"))
}

model_coefficients <- bind_rows(
  robust_coefs(model_a, "A"),
  robust_coefs(model_b, "B"),
  robust_coefs(model_c, "C")
)

model_fit <- tibble(
  model = c("A", "B", "C"),
  n = c(nobs(model_a), nobs(model_b), nobs(model_c)),
  adj_r2 = c(summary(model_a)$adj.r.squared, summary(model_b)$adj.r.squared, NA),
  mcfadden_pseudo_r2 = c(NA, NA, 1 - model_c$deviance / model_c$null.deviance)
)

print(model_coefficients, n = Inf)
print(model_fit)

if (!dir.exists(results_dir)) dir.create(results_dir)
write_csv(model_coefficients, file.path(results_dir, "model_coefficients.csv"))
write_csv(model_fit, file.path(results_dir, "model_fit.csv"))

#### Robustness checks by language ####

# R1: Model B for English-language articles only
model_b_en <- lm(log_fwci ~ oa_status + collaboration + domain + type + factor(publication_year),
                 data = filter(model_data, english == 1))

# R2: Model B for Portuguese-language articles only
model_b_pt <- lm(log_fwci ~ oa_status + collaboration + domain + type + factor(publication_year),
                 data = filter(model_data, language %in% "pt"))

robustness <- bind_rows(
  robust_coefs(model_b_en, "B_english"),
  robust_coefs(model_b_pt, "B_portuguese")
) %>%
  filter(str_detect(term, "oa_status")) %>%
  mutate(n = if_else(model == "B_english", nobs(model_b_en), nobs(model_b_pt)))

print(robustness, n = Inf)
write_csv(robustness, file.path(results_dir, "robustness_by_language.csv"))

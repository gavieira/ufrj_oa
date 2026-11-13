source('config.R')
library(tidyverse)
library(patchwork)
library(scales)

# Figuras em ingles para o artigo da JSciRes (Figuras 2 a 7).
# Salva PNG (300 dpi) em figuras/. Requer resultados/tabela_modelos.csv (scripts/03_modelos.R).

ufrj_data <- readRDS(dados_processados)
dir_fig <- "figuras"
if (!dir.exists(dir_fig)) dir.create(dir_fig)

salvar <- function(plot, nome, largura = 10, altura = 6) {
  ggsave(file.path(dir_fig, paste0(nome, ".png")), plot,
         width = largura, height = altura, dpi = 300, bg = "white")
}

# Paleta unica para as rotas de acesso, usada em todas as figuras
rotas <- c("diamond", "gold", "hybrid", "green", "bronze", "closed")
rotulos_rotas <- c(diamond = "Diamond", gold = "Gold", hybrid = "Hybrid",
                   green = "Green", bronze = "Bronze", closed = "Closed")
cores_rotas <- c(diamond = "#0072B2", gold = "#E69F00", hybrid = "#CC79A7",
                 green = "#009E73", bronze = "#A6761D", closed = "#999999")
cores_acesso <- c("Open access" = "#0072B2", "Closed access" = "#999999")

tema <- theme_minimal(base_size = 13) +
  theme(panel.grid.minor = element_blank(), legend.position = "bottom")

dados <- ufrj_data %>%
  mutate(
    oa_status = factor(oa_status, levels = rotas),
    acesso = if_else(is_oa, "Open access", "Closed access")
  )

#### Figura 2: volume anual por rota de acesso ####

fig2 <- dados %>%
  count(publication_year, oa_status) %>%
  ggplot(aes(publication_year, n, fill = oa_status)) +
  geom_area(color = "white", linewidth = 0.2) +
  scale_fill_manual(values = cores_rotas, labels = rotulos_rotas, name = "Access route") +
  scale_x_continuous(breaks = seq(2001, 2025, 2), expand = c(0.01, 0)) +
  scale_y_continuous(labels = label_comma(), expand = expansion(mult = c(0, 0.05))) +
  labs(x = "Publication year", y = "Number of articles") +
  tema

salvar(fig2, "fig2_volume_by_route")

#### Figura 3: rota de acesso por dominio ####

rotulos_dominio <- c("Health Sciences" = "Health Sciences", "Life Sciences" = "Life Sciences",
                     "Physical Sciences" = "Physical Sciences", "Social Sciences" = "Social Sciences")

fig3 <- dados %>%
  unnest(dominios_nomes) %>%
  filter(dominios_nomes %in% names(rotulos_dominio)) %>%
  count(dominios_nomes, oa_status) %>%
  group_by(dominios_nomes) %>%
  mutate(pct = n / sum(n),
         rotulo = paste0(label_comma()(n), "\n(", percent(pct, accuracy = 0.1), ")")) %>%
  ungroup() %>%
  ggplot(aes(oa_status, n, fill = oa_status)) +
  geom_col(show.legend = FALSE) +
  geom_text(aes(label = rotulo), vjust = -0.2, size = 3, lineheight = 0.85) +
  facet_wrap(~dominios_nomes, ncol = 2, scales = "free_y") +
  scale_fill_manual(values = cores_rotas) +
  scale_x_discrete(labels = rotulos_rotas) +
  scale_y_continuous(labels = label_comma(), expand = expansion(mult = c(0, 0.25))) +
  labs(x = "Access route", y = "Number of articles") +
  tema +
  theme(strip.text = element_text(face = "bold", size = 12))

salvar(fig3, "fig3_route_by_domain", altura = 7)

#### Figura 4: distribuicao do FWCI por ano e acesso ####

df_fwci <- dados %>% filter(!is.na(fwci))

fig4 <- ggplot(df_fwci, aes(publication_year, fwci, fill = acesso, color = acesso,
                            group = interaction(publication_year, acesso))) +
  geom_jitter(alpha = 0.08, size = 0.4,
              position = position_jitterdodge(jitter.width = 0.2, dodge.width = 0.75)) +
  geom_boxplot(outlier.shape = NA, color = "black", linewidth = 0.3, alpha = 0.5,
               width = 0.7, position = position_dodge(width = 0.75)) +
  stat_summary(geom = "crossbar", fun = median, fun.max = median, fun.min = median,
               width = 0.7, color = "red", linewidth = 0.5,
               position = position_dodge(width = 0.75), show.legend = FALSE) +
  scale_y_continuous(trans = "pseudo_log", breaks = c(0, 0.5, 1, 2, 5, 10, 50, 100)) +
  coord_cartesian(ylim = c(0, 100)) +
  scale_x_continuous(breaks = seq(2001, 2025, 2)) +
  scale_fill_manual(values = cores_acesso, name = NULL) +
  scale_color_manual(values = cores_acesso, name = NULL) +
  labs(x = "Publication year", y = "FWCI (pseudo-log scale)") +
  tema

salvar(fig4, "fig4_fwci_distribution", largura = 12)

#### Figura 5: proporcao de artigos com FWCI = 0 ####

df_zero <- df_fwci %>%
  group_by(publication_year, acesso) %>%
  summarise(total = n(), zero = sum(fwci == 0), pct = zero / total, .groups = "drop")

fig5 <- ggplot(df_zero, aes(publication_year, pct, color = acesso)) +
  geom_line(linewidth = 1) +
  geom_point(size = 2) +
  scale_y_continuous(labels = label_percent(), limits = c(0, NA)) +
  scale_x_continuous(breaks = seq(2001, 2025, 2)) +
  scale_color_manual(values = cores_acesso, name = NULL) +
  labs(x = "Publication year", y = "Articles with FWCI = 0") +
  tema

salvar(fig5, "fig5_zero_fwci")

#### Figura 6: quartil superior de FWCI por ano e rota ####

df_topo <- df_fwci %>%
  group_by(publication_year) %>%
  filter(fwci >= quantile(fwci, 0.75)) %>%
  ungroup()

fig6 <- df_topo %>%
  count(publication_year, oa_status) %>%
  group_by(publication_year) %>%
  mutate(pct = n / sum(n)) %>%
  ungroup() %>%
  ggplot(aes(publication_year, pct, fill = oa_status)) +
  geom_area(color = "white", linewidth = 0.2) +
  scale_fill_manual(values = cores_rotas, labels = rotulos_rotas, name = "Access route") +
  scale_y_continuous(labels = label_percent(), expand = c(0, 0)) +
  scale_x_continuous(breaks = seq(2001, 2025, 2), expand = c(0.01, 0)) +
  labs(x = "Publication year", y = "Share of the year's top-quartile articles") +
  tema

salvar(fig6, "fig6_top_quartile_by_route")

#### Figura 7: coeficientes das rotas antes e depois dos controles ####

coefs <- read_csv("resultados/tabela_modelos.csv", show_col_types = FALSE) %>%
  filter(modelo %in% c("A", "B"), str_detect(termo, "oa_status")) %>%
  mutate(
    rota = str_remove(termo, "oa_status"),
    rota = factor(rotulos_rotas[rota], levels = rev(rotulos_rotas[rotas[-6]])),
    modelo = if_else(modelo == "A", "Year only (Model A)", "With controls (Model B)"),
    li = estimativa - 1.96 * erro_padrao,
    ls = estimativa + 1.96 * erro_padrao
  )

fig7 <- ggplot(coefs, aes(estimativa, rota)) +
  geom_vline(xintercept = 0, color = "grey40", linetype = "dashed") +
  geom_line(aes(group = rota), color = "grey70", linewidth = 1) +
  geom_errorbarh(aes(xmin = li, xmax = ls, color = modelo), height = 0.15) +
  geom_point(aes(color = modelo), size = 3) +
  scale_color_manual(values = c("Year only (Model A)" = "#999999",
                                "With controls (Model B)" = "#0072B2"), name = NULL) +
  labs(x = "Coefficient on log(1 + FWCI), relative to closed access (95% CI)", y = NULL) +
  tema

salvar(fig7, "fig7_coefficients", altura = 4.5)

message("Figuras salvas em ", dir_fig)

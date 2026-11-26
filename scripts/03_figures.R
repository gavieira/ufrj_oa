source("config.R")
library(tidyverse)
library(scales)

# Figures 2 to 6 of the JSciRes article.
# Saves PNG files (300 dpi) to figures/.

ufrj_data <- readRDS(processed_data)
if (!dir.exists(figures_dir)) dir.create(figures_dir)

save_figure <- function(plot, name, width = 10, height = 6) {
  ggsave(file.path(figures_dir, paste0(name, ".png")), plot,
         width = width, height = height, dpi = 300, bg = "white")
}

# One palette for the access routes, used in every figure
routes <- c("diamond", "gold", "hybrid", "green", "bronze", "closed")
route_labels <- c(diamond = "Diamond", gold = "Gold", hybrid = "Hybrid",
                  green = "Green", bronze = "Bronze", closed = "Closed")
route_colours <- c(diamond = "#0072B2", gold = "#E69F00", hybrid = "#CC79A7",
                   green = "#009E73", bronze = "#A6761D", closed = "#999999")
access_colours <- c("Open access" = "#0072B2", "Closed access" = "#999999")

base_theme <- theme_minimal(base_size = 13) +
  theme(panel.grid.minor = element_blank(), legend.position = "bottom")

plot_data <- ufrj_data %>%
  mutate(
    oa_status = factor(oa_status, levels = routes),
    access = if_else(is_oa, "Open access", "Closed access")
  )

#### Figure 2: yearly volume by access route ####

fig2 <- plot_data %>%
  count(publication_year, oa_status) %>%
  ggplot(aes(publication_year, n, fill = oa_status)) +
  geom_area(color = "white", linewidth = 0.2) +
  scale_fill_manual(values = route_colours, labels = route_labels, name = "Access route") +
  scale_x_continuous(breaks = seq(2001, 2025, 2), expand = c(0.01, 0)) +
  scale_y_continuous(labels = label_comma(), expand = expansion(mult = c(0, 0.05))) +
  labs(x = "Publication year", y = "Number of articles") +
  base_theme

save_figure(fig2, "fig2_volume_by_route")

#### Figure 3: access route by scientific domain ####

domains <- c("Health Sciences", "Life Sciences", "Physical Sciences", "Social Sciences")

fig3 <- plot_data %>%
  unnest(dominios_nomes) %>%
  filter(dominios_nomes %in% domains) %>%
  count(dominios_nomes, oa_status) %>%
  group_by(dominios_nomes) %>%
  mutate(pct = n / sum(n),
         label = paste0(label_comma()(n), "\n(", percent(pct, accuracy = 0.1), ")")) %>%
  ungroup() %>%
  ggplot(aes(oa_status, n, fill = oa_status)) +
  geom_col(show.legend = FALSE) +
  geom_text(aes(label = label), vjust = -0.2, size = 3, lineheight = 0.85) +
  facet_wrap(~dominios_nomes, ncol = 2, scales = "free_y") +
  scale_fill_manual(values = route_colours) +
  scale_x_discrete(labels = route_labels) +
  scale_y_continuous(labels = label_comma(), expand = expansion(mult = c(0, 0.25))) +
  labs(x = "Access route", y = "Number of articles") +
  base_theme +
  theme(strip.text = element_text(face = "bold", size = 12))

save_figure(fig3, "fig3_route_by_domain", height = 7)

#### Figure 4: FWCI distribution by year and access status ####

fwci_data <- plot_data %>% filter(!is.na(fwci))

fig4 <- ggplot(fwci_data, aes(publication_year, fwci, fill = access, color = access,
                              group = interaction(publication_year, access))) +
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
  scale_fill_manual(values = access_colours, name = NULL) +
  scale_color_manual(values = access_colours, name = NULL) +
  labs(x = "Publication year", y = "FWCI (pseudo-log scale)") +
  base_theme

save_figure(fig4, "fig4_fwci_distribution", width = 12)

#### Figure 5: share of articles with FWCI = 0 ####

zero_data <- fwci_data %>%
  group_by(publication_year, access) %>%
  summarise(total = n(), zero = sum(fwci == 0), pct = zero / total, .groups = "drop")

fig5 <- ggplot(zero_data, aes(publication_year, pct, color = access)) +
  geom_line(linewidth = 1) +
  geom_point(size = 2) +
  scale_y_continuous(labels = label_percent(), limits = c(0, NA)) +
  scale_x_continuous(breaks = seq(2001, 2025, 2)) +
  scale_color_manual(values = access_colours, name = NULL) +
  labs(x = "Publication year", y = "Articles with FWCI = 0") +
  base_theme

save_figure(fig5, "fig5_zero_fwci")

#### Figure 6: top FWCI quartile by year and access route ####

top_quartile <- fwci_data %>%
  group_by(publication_year) %>%
  filter(fwci >= quantile(fwci, 0.75)) %>%
  ungroup()

fig6 <- top_quartile %>%
  count(publication_year, oa_status) %>%
  group_by(publication_year) %>%
  mutate(pct = n / sum(n)) %>%
  ungroup() %>%
  ggplot(aes(publication_year, pct, fill = oa_status)) +
  geom_area(color = "white", linewidth = 0.2) +
  scale_fill_manual(values = route_colours, labels = route_labels, name = "Access route") +
  scale_y_continuous(labels = label_percent(), expand = c(0, 0)) +
  scale_x_continuous(breaks = seq(2001, 2025, 2), expand = c(0.01, 0)) +
  labs(x = "Publication year", y = "Share of the year's top-quartile articles") +
  base_theme

save_figure(fig6, "fig6_top_quartile_by_route")

message("Figures saved to ", figures_dir)

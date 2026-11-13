library(tidyverse)

# Figura 1 do manuscrito: diagrama conceitual (sem dados).
# Mostra o tipo de acesso e o FWCI ligados por confundidores e pela auto-selecao.

dir_fig <- "figuras"
if (!dir.exists(dir_fig)) dir.create(dir_fig)

caixas <- tribble(
  ~id,       ~x,  ~y,   ~largura, ~altura, ~texto,                                                                  ~cor,
  "oa",      1.2, 2.0,  1.9,      0.9,     "Open access route\n(diamond, gold, hybrid,\ngreen, bronze, closed)",   "#0072B2",
  "fwci",    6.8, 2.0,  1.9,      0.9,     "Citation impact\n(FWCI)",                                              "#0072B2",
  "conf",    4.0, 3.75, 3.6,      1.0,     "Confounders\nPublication language; journal circuit (national/international);\ncollaboration type; field; document type", "#555555",
  "quality", 4.0, 0.25, 3.2,      0.7,     "Paper quality / author choice (self-selection)",                       "#999999"
)

setas <- tribble(
  ~x,   ~y,   ~xend, ~yend, ~tipo,
  3.0,  3.25, 1.6,   2.5,   "confounding",
  5.0,  3.25, 6.4,   2.5,   "confounding",
  2.2,  2.0,  5.8,   2.0,   "effect",
  2.6,  0.45, 1.4,   1.5,   "selection",
  5.4,  0.45, 6.6,   1.5,   "selection"
)

rotulos <- tribble(
  ~x,  ~y,   ~texto,
  1.7, 3.0,  "confounding",
  6.3, 3.0,  "confounding",
  1.4, 0.85, "self-selection",
  6.6, 0.85, "self-selection",
  4.0, 2.25, "Access effect (what we want to estimate)",
  4.0, 1.75, "Compared in Model A (raw) and Model B (adjusted for confounders)"
)

fig1 <- ggplot() +
  geom_segment(data = filter(setas, tipo == "confounding"),
               aes(x, y, xend = xend, yend = yend),
               arrow = arrow(length = unit(0.25, "cm"), type = "closed"),
               color = "#555555", linewidth = 0.8) +
  geom_segment(data = filter(setas, tipo == "effect"),
               aes(x, y, xend = xend, yend = yend),
               arrow = arrow(length = unit(0.3, "cm"), type = "closed"),
               color = "#0072B2", linewidth = 1.1, linetype = "dashed") +
  geom_segment(data = filter(setas, tipo == "selection"),
               aes(x, y, xend = xend, yend = yend),
               arrow = arrow(length = unit(0.25, "cm"), type = "closed"),
               color = "#999999", linewidth = 0.7, linetype = "dotted") +
  geom_tile(data = caixas, aes(x, y, width = largura, height = altura),
            fill = "white", color = caixas$cor, linewidth = 0.9) +
  geom_text(data = caixas, aes(x, y, label = texto), size = 3.6, lineheight = 0.95) +
  geom_text(data = rotulos[1:4, ], aes(x, y, label = texto), size = 3.1, color = "#777777", fontface = "italic") +
  geom_text(data = rotulos[5, ], aes(x, y, label = texto), size = 3.4,
            color = "#0072B2", fontface = "bold") +
  geom_text(data = rotulos[6, ], aes(x, y, label = texto), size = 3.1, color = "#555555") +
  coord_cartesian(xlim = c(0.1, 7.9), ylim = c(-0.2, 4.35), expand = FALSE) +
  theme_void() +
  theme(plot.background = element_rect(fill = "white", color = NA))

ggsave(file.path(dir_fig, "fig1_conceptual_framework.png"), fig1,
       width = 10, height = 5.5, dpi = 300, bg = "white")

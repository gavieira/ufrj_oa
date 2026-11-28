source("config.R")
library(tidyverse)

# Figure 1 of the JSciRes article: conceptual framework diagram (no data).
# Access route (explanatory variable) and FWCI (outcome) are both influenced by
# measured confounders (adjusted for in Model B) and unmeasured confounders (not adjusted for).

if (!dir.exists(figures_dir)) dir.create(figures_dir)

blue <- "#0072B2"
dark <- "#333333"
grey <- "#8C8C8C"

boxes <- tribble(
  ~id,        ~x,  ~y,   ~width, ~height, ~colour, ~linetype,
  "oa",       1.5, 3.5,  2.7,    1.35,    blue,    "solid",
  "fwci",     8.5, 3.5,  2.7,    1.35,    blue,    "solid",
  "measured", 5.0, 6.0,  7.6,    1.25,    dark,    "solid",
  "unmeas",   5.0, 1.0,  7.6,    1.25,    grey,    "dashed"
)

# Box titles (bold) and role labels (italic, grey)
titles <- tribble(
  ~x,  ~y,   ~text,
  1.5, 3.8,  "Open access route",
  8.5, 3.8,  "Citation impact\n(FWCI)",
  5.0, 6.22, "Language; collaboration; document type; domain; year",
  5.0, 1.22, "Journal circuit (national/international);\npaper quality / author choice"
)

roles <- tribble(
  ~x,  ~y,   ~text,
  1.5, 3.2,  "Explanatory variable:\nwhat we compare",
  8.5, 3.1,  "Outcome:\nwhat we measure",
  5.0, 5.75, "Measured confounders: influence both; adjusted for in Model B",
  5.0, 0.72, "Unmeasured confounders: influence both; not adjusted for"
)

arrows <- tribble(
  ~x,  ~y,   ~xend, ~yend, ~type,
  3.4, 5.35, 2.0,   4.2,   "measured",
  6.6, 5.35, 8.0,   4.2,   "measured",
  3.4, 1.65, 2.0,   2.8,   "unmeasured",
  6.6, 1.65, 8.0,   2.8,   "unmeasured",
  2.9, 3.5,  7.1,   3.5,   "effect"
)

fig1 <- ggplot() +
  geom_segment(data = filter(arrows, type == "measured"),
               aes(x, y, xend = xend, yend = yend),
               arrow = arrow(length = unit(0.25, "cm"), type = "closed"),
               color = dark, linewidth = 0.8) +
  geom_segment(data = filter(arrows, type == "unmeasured"),
               aes(x, y, xend = xend, yend = yend),
               arrow = arrow(length = unit(0.25, "cm"), type = "closed"),
               color = grey, linewidth = 0.8, linetype = "dotted") +
  geom_segment(data = filter(arrows, type == "effect"),
               aes(x, y, xend = xend, yend = yend),
               arrow = arrow(length = unit(0.3, "cm"), type = "closed"),
               color = blue, linewidth = 1.1, linetype = "dashed") +
  geom_tile(data = boxes, aes(x, y, width = width, height = height),
            fill = "white", color = boxes$colour, linetype = boxes$linetype, linewidth = 0.9) +
  geom_text(data = titles, aes(x, y, label = text), size = 4, fontface = "bold",
            color = dark, lineheight = 0.95) +
  geom_text(data = roles, aes(x, y, label = text), size = 3.4, fontface = "italic",
            color = grey, lineheight = 0.95) +
  annotate("text", x = 5, y = 3.78, label = "Access effect (what we want to estimate)",
           size = 3.6, color = blue, fontface = "bold") +
  coord_cartesian(xlim = c(0, 10), ylim = c(0.2, 6.8), expand = FALSE) +
  theme_void() +
  theme(plot.background = element_rect(fill = "white", color = NA))

ggsave(file.path(figures_dir, "fig1_conceptual_framework.png"), fig1,
       width = 10, height = 6.2, dpi = 300, bg = "white")

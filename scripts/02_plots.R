source('config.R')
library(tidyverse)
library(patchwork)
library(gt)
library(scales)

#Importando dados
ufrj_data = readRDS(dados_processados)

head(ufrj_data)

### Totais de artigos (aberto/fechado) ###

ufrj_data %>%
  # 1. Transforma o TRUE/FALSE e calcula totais e porcentagens
  mutate(tipo_acesso = if_else(is_oa, "Acesso Aberto", "Acesso Fechado")) %>% 
  count(tipo_acesso) %>% 
  mutate(
    pct = n / sum(n),
    label_completa = paste0(n, "\n(", percent(pct, accuracy = 0.1), ")")
  ) %>% 
  
  # 2. Geramos o gráfico
  ggplot(aes(x = tipo_acesso, y = n, fill = tipo_acesso)) +
  geom_col(show.legend = FALSE) +
  # geom_text modificado: centralizado, maior e com cor contrastante
  geom_text(
    aes(label = label_completa), 
    position = position_stack(vjust = 0.5), # Centraliza dentro da barra
    size = 6,                              # Aumenta o tamanho da fonte
    color = "white",                       # Cor branca para ler melhor sobre o preenchimento
    fontface = "bold"
  ) +
  # Customização de cores (usando fill em vez de color)
  scale_fill_manual(values = c("Acesso Aberto" = "#56B4E9", "Acesso Fechado" = "#D55E00")) +
  labs(
    x = "Status de Acesso",
    y = "Total de Documentos"
  ) +
  theme_minimal(base_size = 14) +
  theme(
    axis.title.x = element_text(size = 18), # Tamanho do nome do eixo X
    axis.text.x = element_text(size = 16)                # Tamanho das legendas das categorias
  )

### Producao de artigos (aberto/fechado) ####

df_p_vol <- ufrj_data %>%
  filter(!is.na(is_oa)) %>%
  count(publication_year, is_oa) %>%
  mutate(status_label = ifelse(is_oa, "Acesso Aberto", "Acesso Fechado"))

p_volume <- ggplot(df_p_vol, aes(x = publication_year, y = n, color = status_label)) +
  geom_line(linewidth = 1.2) +
  geom_point(size = 2) +
  scale_color_manual(values = c("Acesso Aberto" = "#56B4E9", "Acesso Fechado" = "#D55E00")) +
  scale_x_continuous(
    breaks = seq(2001, 2025, by = 2), 
    limits = c(2001, 2025),
    expand = c(0.02, 0) # Pequena margem para o ponto não ficar colado no eixo Y
  ) +
  labs(title = "Figura 1: Evolução Temporal da Produção Científica na UFRJ",
       subtitle = "Volume total de artigos (2001-2025)",
       y = "Total de Documentos", x = "Ano", color = "Status") +
  theme_minimal(base_size = 14) +
  theme(legend.position = "bottom")

# Visualizar Volume isolado
p_volume


###Panorama de Impacto###

# --- 1. PREPARAÇÃO DOS DADOS PARA O QUARTIL (P2) ---
# Filtrando os Top 25% artigos com os maiores FWCI 
df_elite_base <- ufrj_data %>%
  filter(!is.na(fwci), !is.na(is_oa)) %>%
  group_by(publication_year) %>%
  filter(fwci >= quantile(fwci, 0.75, na.rm = TRUE)) %>%
  group_by(publication_year, is_oa) %>%
  summarise(n = n(), .groups = "drop_last") %>%
  mutate(perc_no_topo = (n / sum(n)) * 100,
         status_label = ifelse(is_oa, "Acesso Aberto", "Acesso Fechado")) %>%
  ungroup()

ano_min <- min(df_elite_base$publication_year)
ano_max <- max(df_elite_base$publication_year)

# Expansão de 0.5 ano para as laterais (para a cor preencher o fundo do texto)
df_elite_ext <- bind_rows(
  df_elite_base %>% filter(publication_year == ano_min) %>% mutate(publication_year = ano_min - 0.5),
  df_elite_base,
  df_elite_base %>% filter(publication_year == ano_max) %>% mutate(publication_year = ano_max + 0.5)
)

# --- 2. P1: DISTRIBUIÇÃO DE FWCI (BOXPLOT) ---
df_p2 <- ufrj_data %>%
  # Mantemos apenas registros com FWCI e Status de OA definidos
  filter(!is.na(fwci), !is.na(is_oa)) %>%
  
  # Filtro de tipo para manter a consistência acadêmica
  filter(type %in% c("article", "review")) %>% 
  
  mutate(
    # Criamos os labels para a legenda
    status_label = ifelse(is_oa, "Acesso Aberto", "Acesso Fechado"),
    
    # O boxplot no eixo X precisa que o ano seja um 'Fator' (categórico)
    ano_fator = factor(publication_year)
  )


# --- P1: DISTRIBUIÇÃO COM MEDIANA EM VERMELHO E VALOR NUMÉRICO ---
p1_distribuicao <- ggplot(df_p2, 
                          aes(x = publication_year, # Usamos o ano numérico aqui
                              y = fwci, 
                              fill = status_label, 
                              color = status_label,
                              group = interaction(publication_year, status_label))) + # Agrupa por ano E status
  
  # 1. Pontinhos (Jitter)
  geom_jitter(alpha = 0.15, size = 0.6, 
              position = position_jitterdodge(jitter.width = 0.2, dodge.width = 0.75)) +
  
  # 2. Boxplot
  geom_boxplot(outlier.shape = NA, 
               color = "black", 
               linewidth = 0.4, 
               alpha = 0.6, 
               width = 0.7, # Define uma largura fixa para não ocupar o gráfico todo
               position = position_dodge(width = 0.75)) + 
  
  # 3. Linhas das Medianas em Vermelho
  stat_summary(geom = "crossbar", 
               fun = median, fun.max = median, fun.min = median,
               width = 0.7, 
               color = "red", 
               linewidth = 0.7,
               position = position_dodge(width = 0.75),
               show.legend = FALSE) +
  
  # 4. Labels com Valores (Individuais e Alinhados)
  stat_summary(geom = "label", 
               fun = median, 
               aes(label = sprintf("%.2f", after_stat(y))), 
               position = position_dodge(width = 0.75),
               angle = 90,
               vjust = 0.5,
               hjust = -0.2, 
               size = 3, 
               color = "white",
               fill = "red",
               fontface = "bold",
               label.padding = unit(0.08, "lines"),
               show.legend = FALSE) +
  
  # Ajuste das escalas para manter o visual anterior
  scale_y_log10(trans = "pseudo_log", 
                breaks = c(0.1, 1, 10, 100, 1000),
                labels = c("0.1", "1", "10", "100", "1000")) +
  
  # Forçamos o eixo X a mostrar os anos corretamente
  scale_x_continuous(breaks = seq(2001, 2025, by = 2)) +
  
  scale_fill_manual(values = c("Acesso Aberto" = "#56B4E9", "Acesso Fechado" = "#D55E00")) +
  scale_color_manual(values = c("Acesso Aberto" = "#0072B2", "Acesso Fechado" = "#B04100")) +
  
  coord_cartesian(ylim = c(0.1, 100)) +
  
  labs(title = "A)",
       y = "FWCI (Log)", x = NULL) +
  
  theme_minimal(base_size = 16) + 
  theme(legend.position = "none", 
        axis.text.x = element_blank(),
        panel.grid.minor = element_blank(),
        plot.title = element_text(size = 20, face = "bold"))

# --- 3. P2: COMPOSIÇÃO DOS ARTIGOS MAIS CITADOS (QUARTIL 1) ---
teto_maximo <- df_elite_base %>% 
  group_by(publication_year) %>% 
  summarise(total_ano = sum(n)) %>% 
  pull(total_ano) %>% 
  max()

p2_elite <- ggplot(df_elite_ext, aes(x = publication_year, y = n, fill = status_label)) +
  geom_area(alpha = 0.8, color = "white", linewidth = 0.2) +
  geom_text(data = df_elite_base, 
            aes(label = paste0(round(perc_no_topo, 0), "%")),
            position = position_stack(vjust = 0.5), 
            size = 3.2, color = "white", fontface = "bold") +
  # ylim(0, max) garante que o gráfico comece no zero e ignore a linha lá embaixo
  coord_cartesian(ylim = c(0, teto_maximo * 1.1)) +
  scale_x_continuous(breaks = seq(ano_min, ano_max, by = 2),
                     limits = c(ano_min - 0.5, ano_max + 0.5), 
                     expand = c(0,0)) +
  scale_fill_manual(values = c("Acesso Aberto" = "#56B4E9", "Acesso Fechado" = "#D55E00")) +
  # Define o tipo de linha e o nome na legenda
  scale_linetype_manual(name = NULL, values = "dashed") +
  labs(title = "B)",
       #subtitle = "Proporção de documentos com maior impacto por ano",
       y = "Nº de Documentos", x = "Ano de Publicação", fill = "Status") +
  theme_minimal(16) + 
  theme(legend.position = "bottom",
        #axis.text.x = element_text(angle = 45, hjust = 1),
        # Tamanho do título do gráfico
        plot.title = element_text(size = 20, face = "bold"),
        
        # Tamanho do subtítulo
        plot.subtitle = element_text(size = 14),
        
        # Tamanho dos títulos dos eixos (X e Y)
        axis.title = element_text(size = 14),
        
        # Tamanho dos números nos eixos
        axis.text = element_text(size = 12),
        
        # Tamanho do texto da legenda
        legend.text = element_text(size = 12),
        legend.title = element_text(size = 13, face = "bold") 
        ) +
  guides(fill = guide_legend(override.aes = list(alpha = 1, color = NA)),
         linetype = guide_legend(override.aes = list(color = "red")),
         )

# --- 4. UNIÃO FINAL DO PAINEL ---
painel_final <- wrap_plots(p1_distribuicao, p2_elite, ncol = 1, heights = c(1.5, 1)) +
  plot_annotation(
    #title = "Impacto Científico UFRJ: Da Distribuição à Elite",
    theme = theme(plot.title = element_text(size = 18, face = "bold", hjust = 0.5))
  )

# Renderizar
painel_final


##PANORAMA MULTIDIMENSIONAL DE ACESSO ABERTO - UFRJ###

# --- 1. CONFIGURAÇÕES DE TRADUÇÃO E CORES ---
#labels_oa <- c(
#  "diamond" = "Diamante", "gold" = "Dourado", "hybrid" = "Híbrido", 
#  "green"   = "Verde",    "bronze" = "Bronze",  "closed"  = "Fechado"
#)
#
#cores_oa <- c(
#  "diamond" = "#00B0F0", "gold" = "#FFD700", "hybrid" = "#FF8C00", 
#  "green"   = "#00B050", "bronze" = "#CD7F32", "closed" = "#7F7F7F"
#)
#status_levels <- names(cores_oa)
#
## --- 2. GRÁFICO A: TIPOS DE DOCUMENTO (Mantendo os 4 tipos) ---
#p1 <- ufrj_data %>%
#  filter(type %in% c("article", "review", "book-chapter", "preprint")) %>%
#  group_by(type) %>%
#  count(oa_status) %>%
#  mutate(
#    prop = n / sum(n) * 100,
#    label_completo = paste0(n, "\n(", round(prop, 1), "%)"),
#    oa_status = factor(oa_status, levels = status_levels)
#  ) %>%
#  ggplot(aes(x = oa_status, y = n, fill = oa_status)) +
#  geom_col(color = "white", linewidth = 0.1) +
#  geom_text(aes(label = label_completo), vjust = -0.2, size = 4, fontface = "bold", lineheight = 0.8) +
#  facet_wrap(~type, ncol = 4, scales = "free_y", 
#             labeller = as_labeller(c(
#               "article" = "Artigos", 
#               "review" = "Revisões",
#               "book-chapter" = "Capítulos de Livro",
#               "preprint" = "Preprints"
#             ))) +
#  scale_fill_manual(values = cores_oa, labels = labels_oa, name = "Status de Acesso") +
#  scale_y_continuous(expand = expansion(mult = c(0, 0.25))) +
#  labs(title = "Distribuição por Tipo de Documento", y = "Total de Obras", x = NULL) +
#  theme_minimal() +
#  theme(legend.position = "none", 
#        axis.text.x = element_blank(), 
#        strip.background = element_rect(fill = "#0072B2"),
#        strip.text = element_text(color = "white", face = "bold"))
#
## --- 3. GRÁFICO B: DOMÍNIOS ---
#p2 <- ufrj_data %>%
#  unnest(dominios_nomes) %>%
#  filter(!is.na(dominios_nomes), dominios_nomes != "NA") %>%
#  filter(dominios_nomes %in% c("Health Sciences", "Life Sciences", "Physical Sciences", "Social Sciences")) %>%
#  group_by(dominios_nomes) %>%
#  count(oa_status) %>%
#  mutate(
#    prop = n / sum(n) * 100,
#    label_completo = paste0(n, "\n(", round(prop, 1), "%)"),
#    oa_status = factor(oa_status, levels = status_levels)
#  ) %>%
#  ggplot(aes(x = oa_status, y = n, fill = oa_status)) +
#  geom_col(color = "white", linewidth = 0.1) +
#  geom_text(aes(label = label_completo), vjust = -0.2, size = 4, fontface = "bold", lineheight = 0.8) +
#  facet_wrap(~dominios_nomes, ncol = 4, scales = "free_y",
#             labeller = as_labeller(c(
#               "Health Sciences" = "Ciências da Saúde",
#               "Life Sciences" = "Ciências da Vida",
#               "Physical Sciences" = "Ciências Exatas",
#               "Social Sciences" = "Ciências Sociais"
#             ))) +
#  # Tradução da legenda e do eixo X aqui
#  scale_fill_manual(values = cores_oa, labels = labels_oa, name = "Status de Acesso") +
#  scale_x_discrete(labels = labels_oa) +
#  scale_y_continuous(expand = expansion(mult = c(0, 0.25))) +
#  labs(title = "Distribuição por Domínio Científico", y = "Total de Obras", x = "Status de Acesso Aberto") +
#  theme_minimal() +
#  theme(legend.position = "bottom",
#        axis.text.x = element_text(angle = 45, hjust = 1, size = 8, face = "bold"),
#        strip.background = element_rect(fill = "#D55E00"),
#        strip.text = element_text(color = "white", face = "bold"))
#
## --- 4. JUNÇÃO ---
#painel_final <- p1 / p2 + 
#  plot_annotation(
#    title = 'Panorama Multidimensional de Acesso Aberto - UFRJ',
#    theme = theme(plot.title = element_text(size = 16, face = "bold", hjust = 0.5))
#  )
#
#painel_final

### Panorama do acesso aberto por área - UFRJ ###

# --- 1. CONFIGURAÇÕES (Cores e Tradução) ---
labels_oa <- c(
  "diamond" = "Diamante", "gold" = "Dourado", "hybrid" = "Híbrido", 
  "green"   = "Verde",    "bronze" = "Bronze",  "closed"  = "Fechado"
)

cores_oa <- c(
  "diamond" = "#00B0F0", "gold" = "#FFD700", "hybrid" = "#FF8C00", 
  "green"   = "#00B050", "bronze" = "#CD7F32", "closed" = "#7F7F7F"
)
status_levels <- names(cores_oa)

# --- 2. PREPARAÇÃO DOS DADOS ---
df_dominios <- ufrj_data %>%
  unnest(dominios_nomes) %>%
  filter(!is.na(dominios_nomes), dominios_nomes != "NA") %>%
  filter(dominios_nomes %in% c("Health Sciences", "Life Sciences", "Physical Sciences", "Social Sciences")) %>%
  group_by(dominios_nomes) %>%
  count(oa_status) %>%
  mutate(
    prop = n / sum(n) * 100,
    label_completo = paste0(n, "\n(", round(prop, 1), "%)"),
    oa_status = factor(oa_status, levels = status_levels)
  ) %>%
  ungroup() %>%
  # CRIANDO AS ETIQUETAS (A, B, C, D)
  mutate(
    letra = case_when(
      dominios_nomes == "Health Sciences" ~ "A",
      dominios_nomes == "Life Sciences"   ~ "B",
      dominios_nomes == "Physical Sciences" ~ "C",
      dominios_nomes == "Social Sciences"   ~ "D"
    ),
    # Concatenando a letra com o nome traduzido para o título da faceta
    titulo_faceta = case_when(
      dominios_nomes == "Health Sciences" ~ "A) Ciências da Saúde",
      dominios_nomes == "Life Sciences"   ~ "B) Ciências da Vida",
      dominios_nomes == "Physical Sciences" ~ "C) Ciências Exatas",
      dominios_nomes == "Social Sciences"   ~ "D) Ciências Sociais"
    )
  )

# --- 3. CONSTRUÇÃO DO GRÁFICO ---
ggplot(df_dominios, aes(x = oa_status, y = n, fill = oa_status)) +
  geom_col(color = "white", linewidth = 0.1) +
  geom_text(aes(label = label_completo), vjust = -0.2, size = 3.8, fontface = "bold", lineheight = 0.8) +
  # Usando a nova coluna com letras para as facetas
  facet_wrap(~titulo_faceta, ncol = 2, scales = "free_y") + 
  scale_fill_manual(values = cores_oa, labels = labels_oa, name = "Status de Acesso") +
  scale_x_discrete(labels = labels_oa) +
  scale_y_continuous(expand = expansion(mult = c(0, 0.25))) +
  labs(
    title = "Distribuição de Status de Acesso Aberto por Domínio Científico",
    subtitle = "UFRJ (2001-2025)",
    y = "Total de Obras", 
    x = "Status de Acesso"
  ) +
  theme_minimal() +
  theme(
    legend.position = "bottom",
    plot.title = element_text(size = 16, face = "bold", hjust = 0.5),
    plot.subtitle = element_text(size = 11, hjust = 0.5),
    axis.text.x = element_text(angle = 45, hjust = 1, size = 9, face = "bold"),
    strip.background = element_rect(fill = "#D55E00"),
    strip.text = element_text(color = "white", face = "bold", size = 12),
    panel.spacing = unit(1.5, "lines") # espacamento entre os subplots
  )


###Tabela sumarizando diferencas de acesso entre docs###

# --- 1. PROCESSAMENTO E CLASSIFICAÇÃO ---
tabela_base <- ufrj_data %>%
  filter(!is.na(is_oa)) %>%
  mutate(
    status_oa = ifelse(is_oa, "Acesso Aberto", "Acesso Fechado"),
    tipo_colaboracao = case_when(
      countries_distinct_count > 1 ~ "Documentos em Colaboração Internacional",
      countries_distinct_count == 1 & institutions_distinct_count > 1 ~ "Documentos em Colaboração Nacional",
      institutions_distinct_count == 1 ~ "Sem Colaboração (Exclusivo UFRJ)",
      TRUE ~ "Colaboração indefinida"
    )
  )

# --- 2. CÁLCULO DAS MÉTRICAS (Agrupado por Categoria e Status) ---
stats_categorias <- tabela_base %>%
  group_by(tipo_colaboracao, status_oa) %>%
  summarise(
    n = n(),
    media_links = mean(locations_count, na.rm = TRUE),
    media_fwci = mean(fwci, na.rm = TRUE),
    media_citacoes = mean(cited_by_count, na.rm = TRUE),
    .groups = "drop"
  )

# --- 3. CÁLCULO DO TOTAL GERAL (Para ser a primeira linha) ---
stats_total <- stats_categorias %>%
  group_by(status_oa) %>%
  summarise(
    tipo_colaboracao = "Total de Documentos",
    n = sum(n),
    media_links = sum(n * media_links) / sum(n), # Médias ponderadas para o total
    media_fwci = sum(n * media_fwci) / sum(n),
    media_citacoes = sum(n * media_citacoes) / sum(n),
    .groups = "drop"
  )

# --- 4. FORMATAÇÃO E LIMPEZA DE NOTAÇÃO CIENTÍFICA ---
# Unimos os dfs e preparamos a versão final
resumo_pre_format <- bind_rows(stats_total, stats_categorias) %>%
  pivot_longer(cols = c(n, starts_with("media")), names_to = "metrica", values_to = "valor") %>%
  pivot_wider(names_from = status_oa, values_from = valor) %>%
  mutate(
    `Acesso Aberto` = replace_na(`Acesso Aberto`, 0),
    `Acesso Fechado` = replace_na(`Acesso Fechado`, 0)
  )

# Função auxiliar ajustada
formata_limpo <- function(x, eh_contagem = FALSE) {
  if (eh_contagem) {
    x <- as.integer(round(x))
    format(x, big.mark = ".", scientific = FALSE, trim = TRUE)
  } else {
    format(round(x, 2), nsmall = 2, decimal.mark = ",",
           big.mark = ".", scientific = FALSE, trim = TRUE)
  }
}

resumo_final_df <- resumo_pre_format %>%
  mutate(
    total_linha = `Acesso Aberto` + `Acesso Fechado`,
    # Aplicando a lógica de forma condicional à métrica
    `Acesso Aberto` = case_when(
      metrica == "n" ~ paste0(formata_limpo(`Acesso Aberto`, TRUE), " (", round(100 * `Acesso Aberto` / total_linha, 1), "%)"),
      TRUE ~ formata_limpo(`Acesso Aberto`, FALSE)
    ),
    `Acesso Fechado` = case_when(
      metrica == "n" ~ paste0(formata_limpo(`Acesso Fechado`, TRUE), " (", round(100 * `Acesso Fechado` / total_linha, 1), "%)"),
      TRUE ~ formata_limpo(`Acesso Fechado`, FALSE)
    )
  )

# --- 5. ORGANIZAÇÃO DA ESTRUTURA FINAL ---

tabela_ufrj_final <- bind_rows(
  # 1. Linhas de contagem (n)
  resumo_final_df %>% 
    filter(metrica == "n") %>% 
    filter(tipo_colaboracao != "Colaboração indefinida") %>% 
    arrange(desc(tipo_colaboracao == "Total de Documentos")),
  
  # 2. Linhas de médias (extraídas apenas do 'Total de Documentos')
  resumo_final_df %>% 
    filter(metrica != "n" & tipo_colaboracao == "Total de Documentos") %>%
    mutate(tipo_colaboracao = case_when(
      metrica == "media_links" ~ "Média de Links de Acesso",
      metrica == "media_fwci" ~ "Média de Impacto Normalizado (FWCI)",
      metrica == "media_citacoes" ~ "Média de Citações Recebidas"
    ))
) %>%
  select(Indicador = tipo_colaboracao, `Acesso Aberto`, `Acesso Fechado`)

# --- 6. EXPORTAÇÃO VISUAL (gt) ---
tabela_gt <- tabela_ufrj_final %>%
  gt() %>%
  # Dizemos ao gt para não formatar nada, apenas passar o texto
  fmt_passthrough(
    columns = everything()
  ) %>%
  tab_header(
    title = "Produção e Acesso Aberto: UFRJ (2001-2025)",
    subtitle = "Comparativo por Colaboração, Disponibilidade e Impacto"
  ) %>%
  # Alinhamento central para os dados
  cols_align(
    align = "center",
    columns = c(`Acesso Aberto`, `Acesso Fechado`)
  ) %>%
  # Estética e Negritos
  tab_style(
    style = cell_text(weight = "bold"),
    locations = cells_body(rows = 1) 
  ) %>%
  tab_style(
    style = cell_fill(color = "#F9F9F9"),
    locations = cells_body(rows = seq(2, 7, 2)) 
  ) %>%
  tab_options(
    table.width = px(750),
    column_labels.font.weight = "bold"
  )

# Visualizar
tabela_gt

######################################
#Nota: seis registros de acesso fechado possuem o campo countries_distinct_count == 0
#Por isso o número de documentos de acesso fechado não corresponde à soma das categorias individuais
#Seguem os documentos abaixo
######################################

# Criando o dataframe de investigação
df_investigacao <- ufrj_data %>%
  filter(!is.na(is_oa)) %>%
  # Recriamos a coluna de tipo para identificar o limbo
  mutate(
    tipo_teste = case_when(
      countries_distinct_count > 1 ~ "Internacional",
      countries_distinct_count == 1 & institutions_distinct_count > 1 ~ "Nacional",
      institutions_distinct_count == 1 ~ "Exclusiva UFRJ",
      TRUE ~ "LIMBO"
    )
  ) %>%
  # Filtramos apenas o que caiu no LIMBO
  filter(tipo_teste == "LIMBO")

# 2. O que tem nesses registros?
# Olhando as contagens originais para entender por que não entraram nas regras
df_investigacao %>%
  count(countries_distinct_count, institutions_distinct_count, type) %>%
  arrange(desc(n))

### Analisando documentos com zero citacoes ######


# Usando o seu dataframe analise_fwci_plot
plot_perc_rotulado <- ggplot(analise_fwci_plot, 
                             aes(x = publication_year, 
                                 y = perc_zero_fwci, 
                                 color = is_oa, 
                                 group = is_oa)) +
  geom_line(size = 1.2) +
  geom_point(size = 3) +
  
  # ADICIONANDO OS RÓTULOS DE TEXTO
  geom_text(
    aes(label = docs_zero_fwci), # O que será escrito (o total absoluto)
    vjust = -1.2,                # Joga o texto um pouco para cima do ponto
    size = 3.5,                  # Tamanho da fonte
    show.legend = FALSE          # Não adiciona "a" na legenda de cores
  ) +
  
  scale_y_continuous(
    labels = scales::percent_format(accuracy = 1),
    expand = expansion(mult = c(0.1, 0.2)) # Dá um espaço extra no topo para o texto não cortar
  ) + 
  scale_color_manual(values = c("Acesso Aberto" = "#00ba38", "Fechado" = "#f8766d")) +
  labs(
    title = "Evolução da Proporção de Documentos da UFRJ com FWCI = 0",
    subtitle = "Os números sobre os pontos indicam a contagem absoluta de artigos com zero citações",
    x = "Ano de Publicação",
    y = "% de Documentos com FWCI Zero no Ano",
    color = "Tipo de Acesso"
  ) +
  theme_minimal(base_size = 14) +
  theme(legend.position = "bottom")

print(plot_perc_rotulado)
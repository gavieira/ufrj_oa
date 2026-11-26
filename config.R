# Project settings shared by all scripts

years_to_download <- 2001:2025  # Publication years to retrieve

# OpenAlex institution ID of the Federal University of Rio de Janeiro (UFRJ)
ufrj_id <- "I122140584"

# Contact email sent to the OpenAlex API (polite pool).
# Set it outside the repository, e.g. in ~/.Renviron: OPENALEX_EMAIL=you@example.org
contact_email <- Sys.getenv("OPENALEX_EMAIL")

# Folders
data_dir    <- "data"
raw_dir     <- file.path(data_dir, "raw")
figures_dir <- "figures"
results_dir <- "results"

# Data files.
# The CSV and Parquet names match the files deposited on Zenodo
# (https://zenodo.org/records/18883350).
processed_data <- file.path(data_dir, "ufrj_openalex.rds")
zenodo_csv     <- file.path(data_dir, "ufrj_data_zenodo.csv")
zenodo_parquet <- file.path(data_dir, "ufrj_data_completo.parquet")

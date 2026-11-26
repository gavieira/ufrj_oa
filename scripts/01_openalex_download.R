library(httr2)
library(jsonlite)
library(tidyverse)
library(arrow)
source("config.R")

##### Function to retrieve works from the OpenAlex API #####

fetch_works_by_year <- function(institution_id, year, email) {
  base_url <- "https://api.openalex.org/works"

  base_request <- request(base_url) %>%
    req_headers("User-Agent" = paste0("mailto:", email)) %>%
    req_url_query(
      filter = paste0("authorships.institutions.lineage:", institution_id,
                      ",publication_year:", year),
      select = "id,title,doi,publication_year,cited_by_count,counts_by_year,fwci,countries_distinct_count,institutions_distinct_count,language,locations_count,topics,type,open_access",
      `per-page` = 200
    )

  # One data frame per results page
  pages <- list()
  cursor <- "*"

  repeat {
    message(paste("Fetching year", year, "- cursor:", substr(cursor, 1, 10), "..."))

    response <- base_request %>%
      req_url_query(cursor = cursor) %>%
      req_perform()

    # Read the body as text so that jsonlite can flatten nested fields
    body_text <- response %>% resp_body_string()
    page_data <- fromJSON(body_text, flatten = TRUE)

    # Stop when there are no more results
    if (length(page_data$results) == 0) break

    pages[[length(pages) + 1]] <- as.data.frame(page_data$results)

    next_cursor <- page_data$meta$next_cursor
    if (is.null(next_cursor) || cursor == next_cursor) break

    cursor <- next_cursor
  }

  as_tibble(jsonlite::rbind_pages(pages))
}

##### Download and save the data, one file per year #####

if (!dir.exists(raw_dir)) dir.create(raw_dir, recursive = TRUE)

for (year in years_to_download) {

  file_name <- file.path(raw_dir, paste0("openalex_ufrj_", year, ".rds"))

  # Skip years already downloaded (useful if the connection drops)
  if (file.exists(file_name)) {
    message(paste("Skipping year", year, "- file already exists."))
    next
  }

  message(paste("--- Downloading UFRJ works - year:", year, "---"))

  tryCatch({
    year_data <- fetch_works_by_year(ufrj_id, year, contact_email)

    # .rds keeps R data types
    saveRDS(year_data, file = file_name)

    message(paste("Saved to:", file_name))

    # Short pause to be polite to the API
    Sys.sleep(1)

  }, error = function(e) {
    message(paste("Error downloading year", year, ":", e$message))
  })
}

##### Merge the yearly files into a single data frame #####

raw_files <- list.files(raw_dir, pattern = "\\.rds$", full.names = TRUE)

ufrj_raw <- raw_files %>%
  map(readRDS) %>%
  bind_rows()

message(paste("Total records merged:", nrow(ufrj_raw)))
glimpse(ufrj_raw)

##### Clean the data #####

ufrj_clean <- ufrj_raw %>%
  # Extract the scientific domains of each work's topics.
  # The column keeps the name used in the Zenodo deposit (dominios_nomes).
  mutate(
    dominios_nomes = map(topics, function(topic_df) {
      if (is.null(topic_df) || !is.data.frame(topic_df) || nrow(topic_df) == 0) {
        return(NA_character_)
      }
      # unique() keeps each domain only once per work
      topic_df %>%
        pull(domain.display_name) %>%
        unique()
    })
  ) %>%
  # Keep only the OA flag and status
  rename(
    is_oa = open_access.is_oa,
    oa_status = open_access.oa_status
  ) %>%
  select(
    -starts_with("open_access."),
    -topics
  ) %>%
  # Keep only articles and reviews
  filter(
    type %in% c("article", "review")
  )

saveRDS(ufrj_clean, file = processed_data, compress = "xz")

ufrj_data <- readRDS(processed_data)

######## Files deposited on Zenodo (CSV and Parquet) ########

#### CSV

# Safe version of toJSON that returns NA on failure
safe_json <- possibly(function(x) {
  if (is.null(x) || (is.logical(x) && all(is.na(x))) || !is.data.frame(x) || nrow(x) == 0) {
    return(NA_character_)
  }
  as.character(toJSON(x))
}, otherwise = NA_character_)

ufrj_csv <- ufrj_data %>%
  mutate(
    # Yearly citation history serialized as JSON
    counts_by_year = map_chr(counts_by_year, safe_json),

    # Domains as a "; "-delimited string
    dominios_nomes = map_chr(dominios_nomes, ~ {
      val <- unlist(.x)
      if (is.null(val) || all(is.na(val)) || length(val) == 0) {
        return(NA_character_)
      }
      paste(unique(trimws(as.character(val))), collapse = "; ")
    })
  )

write.table(ufrj_csv, zenodo_csv,
            sep = ";",
            dec = ",",
            row.names = FALSE,
            quote = TRUE,
            qmethod = "double",
            fileEncoding = "UTF-8")

#### Parquet

# Give counts_by_year a consistent structure for Arrow
ufrj_parquet <- ufrj_data %>%
  mutate(
    counts_by_year = map(counts_by_year, ~ {
      if (is.null(.x) || !is.data.frame(.x) || nrow(.x) == 0) {
        return(data.frame(year = integer(), cited_by_count = integer()))
      }
      as.data.frame(.x) %>% select(year, cited_by_count)
    })
  )

write_parquet(ufrj_parquet, zenodo_parquet)

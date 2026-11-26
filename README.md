# ufrj_oa

Open access (OA) and citation impact at the Federal University of Rio de Janeiro (UFRJ), based on OpenAlex data.

This repository contains the code for the research article submitted to the *Journal of Scientometric Research* (JSciRes). The article examines how UFRJ's OA output, broken down by route (diamond, gold, hybrid, green, bronze), relates to field-normalized citation impact (FWCI) once publication language, collaboration, domain and document type are taken into account.

## Data

All analyses use a dataset extracted from the OpenAlex API on 4 March 2026 and deposited on Zenodo: <https://zenodo.org/records/18883350>.

The data are not stored in this repository. To reproduce the analyses, download `ufrj_data_zenodo.csv` and `ufrj_data_completo.parquet` from Zenodo into a `data/` folder, or run `scripts/01_openalex_download.R` to retrieve the data again from OpenAlex. OpenAlex updates its records continuously, so a new download will not exactly reproduce the deposited dataset.

## Structure

| Path | Content |
|---|---|
| `config.R` | Settings shared by all scripts (years, UFRJ's OpenAlex ID, folders, file names) |
| `scripts/01_openalex_download.R` | Downloads the data from OpenAlex, cleans it and writes the files deposited on Zenodo |
| `scripts/02_models.R` | Regression models (Models A, B and C) and robustness checks by language; writes `results/` |
| `scripts/03_figures.R` | Figures 2 to 6 of the article; writes `figures/` |
| `scripts/04_conceptual_diagram.R` | Figure 1 of the article (conceptual framework); writes `figures/` |
| `results/` | Model coefficients, fit statistics and robustness checks (CSV) |
| `figures/` | Article figures (PNG, 300 dpi) |

Run the scripts from the repository root (for example, by opening `ufrj_oa.Rproj` in RStudio).

`01_openalex_download.R` reads the contact email for the OpenAlex API from the `OPENALEX_EMAIL` environment variable. Set it outside the repository, for example in `~/.Renviron`:

```
OPENALEX_EMAIL=you@example.org
```

## R packages

`tidyverse`, `httr2`, `jsonlite`, `arrow`, `sandwich`, `lmtest` and `scales`.

## Versions

- `ebbc-2026`: code used in the expanded abstract presented at EBBC 2026, which preceded this article (Portuguese figures in `scripts/02_plots.R`).
- `jscires-2026`: code used in the article submitted to the *Journal of Scientometric Research* (to be tagged at submission).

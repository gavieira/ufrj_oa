# ufrj_oa
Estudo sobre o Acesso Aberto da UFRJ com base nos dados da OpenAlex

Study of open access (OA) and citation impact at the Federal University of Rio de Janeiro (UFRJ), based on OpenAlex data.

## Data

All analyses use the same dataset, extracted from the OpenAlex API on 4 March 2026 and deposited on Zenodo: <https://zenodo.org/records/18883350>. The data are not stored in this repository; download them into a `dados/` folder before running the scripts.

## Which scripts belong to which study

| Script | Used in | What it does |
|---|---|---|
| `scripts/01_openalex_download.R` | Both | Downloads the data from OpenAlex, cleans it and generates the Zenodo files |
| `scripts/02_plots.R` | EBBC 2026 (expanded abstract) | Figures and tables in Portuguese |
| `scripts/03_modelos.R` | JSciRes (research article) | Regression models and robustness checks; writes `resultados/` |
| `scripts/04_figuras_en.R` | JSciRes (research article) | Figures in English; writes `figuras/` |
| `scripts/05_diagrama_conceitual.R` | JSciRes (research article) | Conceptual framework diagram (Figure 1) |

## Versions

Each study is tied to a tagged version of this repository:

- `ebbc-2026`: code used in the EBBC 2026 expanded abstract. In this version, the last block of `02_plots.R` (share of documents with FWCI = 0) does not run because the object `analise_fwci_plot` is not defined; this figure was not part of the abstract.
- `jscires-2026`: code used in the article submitted to the *Journal of Scientometric Research* (to be tagged at submission).

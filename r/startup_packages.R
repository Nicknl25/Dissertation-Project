# ============================================
#  startup_packages.R
#  Purpose: Ensure all required R packages are installed and loaded
#  Project: Dissertation Forecasting Framework
# ============================================

options(repos = c(CRAN = "https://cloud.r-project.org"))

# ---------- MASTER PACKAGE SETUP (FULL) ----------

pkgs <- c(
  # Core
  "tidyverse", "data.table", "zoo", "xts", "lubridate", "stringr", "janitor",
  
  # Forecasting & Time Series
  "forecast", "fpp3", "tseries", "urca", "lmtest",
  "Metrics", "forecastHybrid", "TSstudio", "ggfortify",
  "Mcomp", "thief", "caret", "prophet", "vars", "imputeTS", "rugarch",
  
  # Visualization
  "patchwork", "cowplot", "GGally",
  
  # DB / API
  "httr2", "jsonlite", "DBI", "odbc",
  
  # Reporting / Reproducibility
  "openxlsx", "rmarkdown", "knitr", "here", "config", "renv", "reticulate",
  
  # Statistics / Econometrics
  "AER", "car",
  
  # Dissertation Project Add-ons (required for your stack)
  "RMySQL", "dotenv", "fredr"
)

# ---------- INSTALL & LOAD ----------
for (p in pkgs) {
  if (!requireNamespace(p, quietly = TRUE)) {
    cat("📦 Installing missing package:", p, "\n")
    install.packages(p, dependencies = TRUE)
  }
}

suppressPackageStartupMessages({
  invisible(lapply(pkgs, library, character.only = TRUE))
})

cat("✅ All project packages installed and loaded successfully!\n")

#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  # Load after ensuring install
})

ensure_packages <- function(pkgs) {
  to_install <- pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly = TRUE)]
  if (length(to_install)) {
    repos <- getOption("repos")
    if (is.null(repos) || isTRUE(repos[["CRAN"]] == "@CRAN@")) {
      options(repos = c(CRAN = "https://cloud.r-project.org"))
    }
    install.packages(to_install, quiet = TRUE)
  }
}

ensure_packages(c("dotenv", "readr"))

suppressPackageStartupMessages({
  library(dotenv)
  library(readr)
})

try(dotenv::load_dot_env(file = ".env"), silent = TRUE)
key <- Sys.getenv("ALPHAVANTAGE_API_KEY", unset = "")
if (!nzchar(key)) {
  cat("ALPHAVANTAGE_API_KEY not found in environment.\n")
  quit(status = 2)
}

url <- paste0(
  "https://www.alphavantage.co/query?function=EARNINGS_CALENDAR&symbol=AAPL&horizon=12month&apikey=",
  key,
  "&datatype=csv"
)

cat("Requesting CSV from Alpha Vantage...\n")
df <- tryCatch(readr::read_csv(url, show_col_types = FALSE), error = function(e) {
  cat("Error reading CSV: ", conditionMessage(e), "\n", sep = "")
  return(NULL)
})

if (is.null(df)) {
  cat("No data frame returned (NULL).\n")
} else {
  cat("Rows returned: ", nrow(df), "\n", sep = "")
  print(utils::head(df, 10))
}


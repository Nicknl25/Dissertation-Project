#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  # Loaded after ensuring install
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

ensure_packages(c("dotenv", "DBI", "RMariaDB", "tidyverse", "lubridate", "readr"))

suppressPackageStartupMessages({
  library(dotenv)
  library(DBI)
  library(RMariaDB)
  library(tidyverse)
  library(lubridate)
  library(readr)
})

# Configurable symbols (later can read from Excel)
symbols <- c("AAPL", "MSFT", "AMZN", "GOOGL")

load_dot_env <- function() {
  tryCatch({ dotenv::load_dot_env(file = ".env") }, error = function(e) {
    message("Warning: .env not loaded: ", conditionMessage(e))
  })
}

get_env <- function(key) {
  val <- Sys.getenv(key, unset = "")
  val <- trimws(val)
  if (identical(val, "")) return(NULL)
  val
}

alpha_key <- NULL

get_earnings_calendar_one <- function(symbol) {
  horizons <- c("3month", "6month", "12month")
  rows <- list()
  for (h in horizons) {
    url <- paste0(
      "https://www.alphavantage.co/query?function=EARNINGS_CALENDAR",
      "&symbol=", symbol,
      "&horizon=", h,
      "&apikey=", alpha_key,
      "&datatype=csv"
    )
    # CSV-only read
    df <- tryCatch(readr::read_csv(url, show_col_types = FALSE), error = function(e) NULL)
    print(paste("Fetched", ifelse(is.null(df), 0, nrow(df)), "rows for", symbol, "horizon:", h))
    if (!is.null(df)) {
      print(names(df))
      print(utils::head(df, 2))
      # Normalize names and select
      names(df) <- tolower(names(df))
      if ("reportdate" %in% names(df)) {
        df <- dplyr::rename(df, reportDate = reportdate)
      }
      # Ensure reportDate as Date and filter non-NA
      if ("reportDate" %in% names(df)) {
        df$reportDate <- suppressWarnings(as.Date(df$reportDate))
      }
      df <- dplyr::filter(df, !is.na(.data$reportDate))
      if (nrow(df)) {
        rows[[h]] <- tibble(
          symbol = symbol,
          horizon = h,
          reportDate = df$reportDate,
          pull_date = Sys.Date(),
          source = "AlphaVantage"
        )
      }
    }
    # Be polite to API
    Sys.sleep(13)
  }
  dplyr::bind_rows(rows)
}

main <- function() {
  load_dot_env()
  alpha_key <<- get_env("ALPHAVANTAGE_API_KEY")
  if (is.null(alpha_key)) stop("ALPHAVANTAGE_API_KEY is missing in .env")

  mysql_host <- get_env("MYSQL_HOST")
  mysql_user <- get_env("MYSQL_USER")
  mysql_password <- get_env("MYSQL_PASSWORD")
  mysql_db <- get_env("MYSQL_DB")
  missing <- c()
  if (is.null(mysql_host)) missing <- c(missing, "MYSQL_HOST")
  if (is.null(mysql_user)) missing <- c(missing, "MYSQL_USER")
  if (is.null(mysql_password)) missing <- c(missing, "MYSQL_PASSWORD")
  if (is.null(mysql_db)) missing <- c(missing, "MYSQL_DB")
  if (length(missing)) stop("Missing DB env vars: ", paste(missing, collapse = ", "))

  message(sprintf("Loading earnings calendar for %d symbols: %s", length(symbols), paste(symbols, collapse = ", ")))
  parts <- lapply(symbols, function(s) { message("  symbol ", s); get_earnings_calendar_one(s) })
  ec <- dplyr::bind_rows(parts)

  con <- dbConnect(MariaDB(), host = mysql_host, user = mysql_user, password = mysql_password, dbname = mysql_db)
  on.exit({ try(dbDisconnect(con), silent = TRUE) }, add = TRUE)

  # Ensure table exists
  try(dbExecute(con, paste0(
    "CREATE TABLE IF NOT EXISTS `Earnings_Calendar` (",
    "`id` INT AUTO_INCREMENT PRIMARY KEY,",
    "`symbol` VARCHAR(10),",
    "`horizon` VARCHAR(10),",
    "`reportDate` DATE,",
    "`pull_date` DATE,",
    "`source` VARCHAR(20))"
  )), silent = TRUE)

  total_inserted <- 0L
  if (nrow(ec)) {
    key_pairs <- ec |>
      dplyr::select(symbol, horizon) |>
      dplyr::distinct()
    if (nrow(key_pairs)) {
      apply(key_pairs, 1, function(row) {
        sym <- row[["symbol"]]; hor <- row[["horizon"]]
        sub <- ec |>
          dplyr::filter(.data$symbol == sym, .data$horizon == hor)
        # Delete existing rows for this (symbol, horizon)
        try(dbExecute(con, sprintf(
          "DELETE FROM `Earnings_Calendar` WHERE symbol = '%s' AND horizon = '%s'",
          dbEscapeStrings(con, sym), dbEscapeStrings(con, hor)
        )), silent = TRUE)
        # Insert new rows
        if (nrow(sub)) {
          dbWriteTable(con, "Earnings_Calendar", sub, append = TRUE)
          total_inserted <<- total_inserted + nrow(sub)
          cat(sprintf("\u2705 Inserted %d rows for %s\n", nrow(sub), sym))
        } else {
          cat(sprintf("No rows to insert for %s (%s)\n", sym, hor))
        }
      })
    }
  } else {
    message("No earnings calendar rows to insert.")
  }

  invisible(ec)
}

# Allow running standalone
if (identical(environment(), globalenv())) {
  try(main(), silent = FALSE)
}

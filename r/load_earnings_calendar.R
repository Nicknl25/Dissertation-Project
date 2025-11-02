#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(dotenv)
  library(DBI)
  library(RMariaDB)
  library(tidyverse)
  library(lubridate)
  library(readr)
  library(httr2)
})

# -----------------------------------------------------------------------------
# Helpers and environment
# -----------------------------------------------------------------------------

ensure_packages <- function(pkgs) {
  to_install <- pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly = TRUE)]
  if (length(to_install)) {
    options(repos = c(CRAN = "https://cloud.r-project.org"))
    install.packages(to_install, quiet = TRUE)
  }
}

`%||%` <- function(a, b) if (!is.null(a)) a else b

load_dot_env <- function() {
  try(dotenv::load_dot_env(file = ".env"), silent = TRUE)
  parent_env <- file.path("..", ".env")
  if (file.exists(parent_env)) try(dotenv::load_dot_env(parent_env, override = FALSE), silent = TRUE)
}

get_env <- function(key) {
  val <- Sys.getenv(key, unset = "")
  val <- trimws(val)
  if (identical(val, "")) return(NULL)
  val
}

# -----------------------------------------------------------------------------
# Cleanup + logging
# -----------------------------------------------------------------------------

cleanup_resources <- function() {
  try(closeAllConnections(), silent = TRUE)
  if ("httr2" %in% loadedNamespaces()) try(httr2::req_perform_cancel_all(), silent = TRUE)
  invisible(gc(full = TRUE))
}

maybe_cleanup <- function(i, every = 25L) {
  if ((i %% every) == 0L) cleanup_resources()
}

log_msg <- function(...) cat(paste0("[earnings] ", paste0(..., collapse = ""), "\n"))

# -----------------------------------------------------------------------------
# Alpha Vantage call with horizon fallback
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# Listing-status lookup
# -----------------------------------------------------------------------------

get_listing_status <- function(symbol, alpha_key) {
  url <- paste0(
    "https://www.alphavantage.co/query?function=LISTING_STATUS",
    "&symbol=", symbol,
    "&apikey=", alpha_key,
    "&datatype=csv"
  )
  df <- tryCatch(readr::read_csv(url, show_col_types = FALSE), error = function(e) NULL)
  if (is.null(df) || !nrow(df)) return(NA_character_)
  if (!"status" %in% names(df)) return(NA_character_)
  # Alpha Vantage normally returns "Active" or "Delisted"
  return(tolower(df$status[1]))
}


get_earnings_calendar_one <- function(symbol, alpha_key) {
  horizons <- c("3month", "6month", "12month")
  df <- tibble::tibble()

  for (h in horizons) {
    url <- paste0(
      "https://www.alphavantage.co/query?function=EARNINGS_CALENDAR",
      "&symbol=", symbol,
      "&horizon=", h,
      "&apikey=", alpha_key,
      "&datatype=csv"
    )

    tmp <- tryCatch(readr::read_csv(url, show_col_types = FALSE), error = function(e) NULL)

    if (!is.null(tmp) && nrow(tmp) > 0 && any(!is.na(tmp$reportDate))) {
      tmp$horizon <- h
      df <- tmp
      log_msg(symbol, " — found data (", h, ")")
      break
    } else {
      log_msg(symbol, " — no data for ", h)
    }

    Sys.sleep(0.8) # small pause between horizons
  }

  if (nrow(df) == 0) return(tibble::tibble())

  # Keep only API-native fields + horizon for reference
  df <- dplyr::select(df, dplyr::any_of(c(
    "symbol", "name", "reportDate", "fiscalDateEnding", "estimate", "currency", "horizon"
  )))
  df
}

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

main <- function() {
  log_msg("start")

  load_dot_env()
  alpha_key <- get_env("ALPHAVANTAGE_API_KEY")
  if (is.null(alpha_key)) stop("ALPHAVANTAGE_API_KEY missing in .env")

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

  api_sleep <- as.numeric(get_env("API_SLEEP_SECONDS") %||% 2)
  if (is.na(api_sleep) || api_sleep < 0) api_sleep <- 2
  test_mode <- tolower((get_env("TEST_MODE") %||% "false"))
  test_mode <- test_mode %in% c("1", "true", "t", "yes", "y")
  ticker_limit <- suppressWarnings(as.integer(get_env("TICKER_LIMIT") %||% "10"))
  if (is.na(ticker_limit) || ticker_limit <= 0) ticker_limit <- 10

  maria_drv <- RMariaDB::MariaDB()
  con <- DBI::dbConnect(maria_drv,
    host = mysql_host,
    user = mysql_user,
    password = mysql_password,
    dbname = mysql_db
  )
  on.exit({ suppressWarnings(try(DBI::dbDisconnect(con), silent = TRUE)) }, add = TRUE)

  qry <- if (isTRUE(test_mode))
    sprintf("SELECT Symbol FROM ticker_master ORDER BY Symbol ASC LIMIT %d;", ticker_limit)
  else
    "SELECT Symbol FROM ticker_master ORDER BY Symbol ASC;"
  tickers <- DBI::dbGetQuery(con, qry)
  symbols <- tickers$Symbol
  log_msg("symbols from DB: ", length(symbols))
  if (length(symbols) == 0) {
    log_msg("no symbols to process (populate ticker_master)")
    return(invisible(FALSE))
  }

  # Ensure table exists (with horizon column)
  DBI::dbExecute(con, paste0(
    "CREATE TABLE IF NOT EXISTS `Earnings_Calendar` (",
    "`symbol` VARCHAR(10) PRIMARY KEY,",
    "`name` VARCHAR(255),",
    "`reportDate` DATE,",
    "`fiscalDateEnding` DATE,",
    "`estimate` DECIMAL(15,4),",
    "`currency` VARCHAR(10),",
    "`horizon` VARCHAR(10))"
  ))

  total_inserted <- 0L

  # ---------------------------------------------------------------------------
  # Streaming per symbol
  # ---------------------------------------------------------------------------
  for (i in seq_along(symbols)) {
    sym <- symbols[[i]]
    log_msg("API ", i, "/", length(symbols), ": ", sym)

    # Get listing status for every ticker (active/delisted)
status_val <- get_listing_status(sym, alpha_key)
if (is.na(status_val)) status_val <- "unknown"
log_msg(sym, " — listing status: ", status_val)


    ec <- try(get_earnings_calendar_one(sym, alpha_key), silent = TRUE)
    if (inherits(ec, "try-error") || is.null(ec)) ec <- tibble::tibble()

    if (nrow(ec) > 0) {
      con_sym <- DBI::dbConnect(maria_drv,
        host = mysql_host, user = mysql_user,
        password = mysql_password, dbname = mysql_db
      )
      try({
        DBI::dbExecute(con_sym,
          "DELETE FROM `Earnings_Calendar` WHERE symbol = ?",
          params = list(sym)
        )
       # Add listing_status column before insert
if (!"listing_status" %in% names(ec)) ec$listing_status <- status_val
DBI::dbAppendTable(con_sym, "Earnings_Calendar", ec)

        total_inserted <- total_inserted + nrow(ec)
        log_msg("inserted ", nrow(ec), " row(s) for ", sym)
      }, silent = TRUE)
      suppressWarnings(try(DBI::dbDisconnect(con_sym), silent = TRUE))
    } else {




      # When no earnings data, check listing status
status_val <- get_listing_status(sym, alpha_key)
log_msg(sym, " — listing status: ", status_val)

# Insert placeholder row if symbol exists but no earnings
if (!is.na(status_val)) {
  con_sym <- DBI::dbConnect(maria_drv,
    host = mysql_host, user = mysql_user,
    password = mysql_password, dbname = mysql_db
  )
  placeholder <- tibble::tibble(
    symbol = sym,
    name = NA_character_,
    reportDate = NA_Date_,
    fiscalDateEnding = NA_Date_,
    estimate = NA_real_,
    currency = NA_character_,
    horizon = NA_character_,
    listing_status = status_val
  )
  try({
    DBI::dbExecute(con_sym,
      "DELETE FROM `Earnings_Calendar` WHERE symbol = ?",
      params = list(sym)
    )
    DBI::dbAppendTable(con_sym, "Earnings_Calendar", placeholder)
    total_inserted <- total_inserted + 1L
    log_msg("inserted placeholder row for ", sym, " (status=", status_val, ")")
  }, silent = TRUE)
  suppressWarnings(try(DBI::dbDisconnect(con_sym), silent = TRUE))
}




      log_msg("no rows from API for ", sym)
    }

    cleanup_resources()
    maybe_cleanup(i, every = 25)
    Sys.sleep(api_sleep)
  }

  log_msg("done. symbols=", length(symbols), ", rows_inserted=", total_inserted)
  invisible(TRUE)
}

# -----------------------------------------------------------------------------
# Execute when sourced
# -----------------------------------------------------------------------------
try(main(), silent = FALSE)

# --- Final unconditional cleanup ---
try({ suppressWarnings(closeAllConnections()) }, silent = TRUE)
if ("httr2" %in% loadedNamespaces()) try(suppressWarnings(httr2::req_perform_cancel_all()), silent = TRUE)
invisible(gc(full = TRUE)); Sys.sleep(0.2); invisible(gc(full = TRUE))

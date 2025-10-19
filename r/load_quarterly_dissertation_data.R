#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  # We'll load these after ensuring install
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

ensure_packages(c(
  "dotenv", "httr2", "jsonlite", "DBI", "RMariaDB", "tidyverse", "lubridate"
))

suppressPackageStartupMessages({
  library(dotenv)
  library(httr2)
  library(jsonlite)
  library(DBI)
  library(RMariaDB)
  library(tidyverse)
  library(lubridate)
})

# -----------------------------------------------------------------------------
# Configurable parameters (easy to change later or source from Excel)
# -----------------------------------------------------------------------------
lookback_years <- 10
symbols <- c("AAPL", "MSFT", "AMZN", "GOOGL")

# -----------------------------------------------------------------------------
# Environment and helpers
# -----------------------------------------------------------------------------
load_dot_env <- function() {
  tryCatch({
    dotenv::load_dot_env(file = ".env")
  }, error = function(e) {
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
last_request_time <- NULL
polite_get <- function(req) {
  # Alpha Vantage free tier: 5 requests/minute
  now <- Sys.time()
  if (!is.null(last_request_time)) {
    delta <- as.numeric(difftime(now, last_request_time, units = "secs"))
    if (delta < 13) {
      Sys.sleep(13 - delta)
    }
  }
  resp <- req_perform(req)
  last_request_time <<- Sys.time()
  resp
}

get_alpha <- function(function_name, symbol) {
  # Returns a tibble of quarterlyReports with added columns when applicable
  req <- request("https://www.alphavantage.co/query") |>
    req_url_query(!!!list('function' = function_name, symbol = symbol, apikey = alpha_key)) |>
    req_user_agent("DissertationApp/1.0 (R)") |>
    req_timeout(60)

  resp <- polite_get(req)
  json <- resp_body_json(resp, simplifyVector = TRUE)

  # If rate-limited or error, Alpha Vantage returns a "Note" or "Information" field
  note_text <- NULL
  if (!is.null(json$Note)) note_text <- json$Note
  if (is.null(note_text) && !is.null(json$Information)) note_text <- json$Information
  if (is.null(note_text) && !is.null(json[["Error Message"]])) note_text <- json[["Error Message"]]
  if (!is.null(note_text)) message(sprintf("Alpha Vantage response for %s/%s: %s", function_name, symbol, note_text))

  # For financial statements we expect quarterlyReports
  qr <- json$quarterlyReports
  if (!is.null(qr)) {
    df <- as_tibble(qr)
    if (!"fiscalDateEnding" %in% names(df)) {
      df <- df
    }
    df <- df |>
      mutate(
        symbol = symbol,
        statement_type = function_name,
        fiscalDateEnding = suppressWarnings(ymd(as.character(.data$fiscalDateEnding)))
      )
    return(df)
  }

  # For OVERVIEW and GLOBAL_QUOTE, return as small tibble with needed fields
  if (identical(function_name, "OVERVIEW")) {
    tibble(
      symbol = symbol,
      industry = json$Industry %||% NA_character_,
      sharesOutstanding = suppressWarnings(as.numeric(json$SharesOutstanding))
    )
  } else if (identical(function_name, "GLOBAL_QUOTE")) {
    gq <- json[["Global Quote"]]
    price <- NA_real_
    if (!is.null(gq)) {
      price <- suppressWarnings(as.numeric(gq[["05. price"]]))
    }
    tibble(
      symbol = symbol,
      sharePrice = price
    )
  } else {
    tibble()
  }
}

`%||%` <- function(a, b) if (!is.null(a)) a else b

# -----------------------------------------------------------------------------
# New: share price on/near quarter end and earnings calendar
# -----------------------------------------------------------------------------

# Cache for per-symbol daily series to avoid repeated API calls
price_cache <- new.env(parent = emptyenv())

get_share_price_on_date <- function(symbol, date) {
  # Returns adjusted close on date or closest previous trading day
  if (is.na(date)) return(NA_real_)
  if (!exists(symbol, envir = price_cache, inherits = FALSE)) {
    # Use outputsize=full to ensure historical coverage
    url <- paste0(
      "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED",
      "&symbol=", symbol,
      "&apikey=", alpha_key,
      "&outputsize=full"
    )
    req <- request(url) |>
      req_user_agent("DissertationApp/1.0 (R)") |>
      req_timeout(60)
    resp <- polite_get(req)
    js <- resp_body_json(resp, simplifyVector = TRUE)
    ts <- js[["Time Series (Daily)"]]
    if (is.null(ts)) {
      return(NA_real_)
    }
    # Convert list of named rows to tibble(Date, adj_close)
    dates <- names(ts)
    df <- tibble(
      d = suppressWarnings(ymd(dates)),
      adj = suppressWarnings(as.numeric(vapply(ts, function(x) x[["5. adjusted close"]], FUN.VALUE = character(1))))
    ) |>
      arrange(d)
    assign(symbol, df, envir = price_cache)
  } else {
    df <- get(symbol, envir = price_cache, inherits = FALSE)
  }
  # Find the latest trading day on/before 'date'
  cand <- df |>
    filter(d <= as.Date(date)) |>
    slice_tail(n = 1)
  if (nrow(cand) == 0) return(NA_real_)
  as.numeric(cand$adj[[1]])
}

# Forward-looking earnings calendar (3M / 6M / 12M)
get_earnings_calendar <- function(symbol) {
  horizons <- c("3month", "6month", "12month")
  out <- list()
  for (h in horizons) {
    url <- paste0(
      "https://www.alphavantage.co/query?function=EARNINGS_CALENDAR",
      "&symbol=", symbol,
      "&horizon=", h,
      "&apikey=", alpha_key
    )
    req <- request(url) |>
      req_user_agent("DissertationApp/1.0 (R)") |>
      req_timeout(60)
    resp <- polite_get(req)
    txt <- resp_body_string(resp)
    # Try JSON first
    cal_df <- NULL
    try({
      js <- jsonlite::fromJSON(txt, simplifyVector = TRUE)
      if (is.list(js) && !is.null(js$earningsCalendar)) {
        cal_df <- as_tibble(js$earningsCalendar)
      }
    }, silent = TRUE)
    # Fallback to CSV
    if (is.null(cal_df)) {
      con <- textConnection(txt)
      cal_df <- tryCatch(suppressWarnings(readr::read_csv(con, show_col_types = FALSE)), error = function(e) NULL)
      try(close(con), silent = TRUE)
    }
    if (!is.null(cal_df) && nrow(cal_df) > 0) {
      rd_name <- intersect(names(cal_df), c("reportDate", "ReportDate", "report_date"))
      if (length(rd_name) > 0) {
        earliest <- suppressWarnings(min(ymd(as.character(cal_df[[rd_name[1]]])), na.rm = TRUE))
        if (is.infinite(earliest)) earliest <- as.Date(NA)
      } else {
        earliest <- as.Date(NA)
      }
    } else {
      earliest <- as.Date(NA)
    }
    out[[h]] <- earliest
  }
  tibble(
    symbol = symbol,
    earningsDate_3M = out[["3month"]],
    earningsDate_6M = out[["6month"]],
    earningsDate_12M = out[["12month"]]
  )
}

# Combine earnings calendar for a vector of symbols and return long-form tibble
load_earnings_calendar <- function(symbols) {
  if (length(symbols) == 0) {
    return(tibble(symbol = character(), horizon = character(), reportDate = as.Date(character()), pull_date = as.Date(character()), source = character()))
  }
  frames <- lapply(symbols, get_earnings_calendar)
  ec <- bind_rows(frames)
  if (nrow(ec) == 0) {
    return(tibble(symbol = character(), horizon = character(), reportDate = as.Date(character()), pull_date = as.Date(character()), source = character()))
  }
  long <- ec |>
    tidyr::pivot_longer(cols = starts_with("earningsDate_"), names_to = "horizon_label", values_to = "reportDate") |>
    mutate(
      horizon = dplyr::case_when(
        horizon_label == "earningsDate_3M" ~ "3month",
        horizon_label == "earningsDate_6M" ~ "6month",
        horizon_label == "earningsDate_12M" ~ "12month",
        TRUE ~ NA_character_
      ),
      reportDate = suppressWarnings(as.Date(reportDate)),
      pull_date = Sys.Date(),
      source = "AlphaVantage"
    ) |>
    select(symbol, horizon, reportDate, pull_date, source) |>
    filter(!is.na(horizon) & !is.na(reportDate))
  long
}

make_symbol_frame <- function(sym) {
  # Pull statements
  inc <- get_alpha("INCOME_STATEMENT", sym)
  bal <- get_alpha("BALANCE_SHEET", sym)
  cfl <- get_alpha("CASH_FLOW", sym)

  # Restrict to expected key and remove statement_type before join
  inc2 <- inc |>
    select(-any_of(c("statement_type")))
  bal2 <- bal |>
    select(-any_of(c("statement_type")))
  cfl2 <- cfl |>
    select(-any_of(c("statement_type")))

  # Ensure fiscalDateEnding exists as Date
  for (nm in c("fiscalDateEnding")) {
    if (nm %in% names(inc2)) inc2[[nm]] <- suppressWarnings(ymd(inc2[[nm]]))
    if (nm %in% names(bal2)) bal2[[nm]] <- suppressWarnings(ymd(bal2[[nm]]))
    if (nm %in% names(cfl2)) cfl2[[nm]] <- suppressWarnings(ymd(cfl2[[nm]]))
  }

  # Full-join on fiscalDateEnding
  frames <- list(inc2, bal2, cfl2) |>
    purrr::discard(~ nrow(.x) == 0)
  if (length(frames) == 0) {
    joined <- tibble(symbol = sym, fiscalDateEnding = as.Date(NA))
  } else if (length(frames) == 1) {
    joined <- frames[[1]]
  } else {
    joined <- purrr::reduce(frames, ~ full_join(.x, .y, by = c("fiscalDateEnding", "symbol")))
  }

  if (nrow(joined) == 0) {
    # No financial rows; still add symbol key cols so later merges work
    joined <- tibble(symbol = sym, fiscalDateEnding = as.Date(NA))
  }

  # OVERVIEW
  ov <- get_alpha("OVERVIEW", sym)

  # Add overview columns (broadcast across rows for the symbol)
  if (!is.null(ov) && nrow(ov)) {
    joined <- joined |>
      mutate(
        industry = ov$industry[1] %||% NA_character_,
        sharesOutstanding = ov$sharesOutstanding[1] %||% NA_real_
      )
  }

  # Compute share price at/near each quarter end
  if ("fiscalDateEnding" %in% names(joined)) {
    joined <- joined |>
      rowwise() |>
      mutate(sharePrice = get_share_price_on_date(symbol, fiscalDateEnding)) |>
      ungroup()
  }

  # Forward-looking earnings calendar (same for all rows per symbol)
  cal <- get_earnings_calendar(sym)
  if (!is.null(cal) && nrow(cal)) {
    joined <- joined |>
      mutate(
        earningsDate_3M = cal$earningsDate_3M[1],
        earningsDate_6M = cal$earningsDate_6M[1],
        earningsDate_12M = cal$earningsDate_12M[1]
      )
  }

  # Add symbol and pull_date; filter by lookback
  joined <- joined |>
    mutate(
      symbol = sym,
      pull_date = Sys.Date()
    ) |>
    filter(is.na(fiscalDateEnding) | fiscalDateEnding >= Sys.Date() - years(lookback_years))

  joined
}

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
load_dot_env()

alpha_key <- get_env("ALPHAVANTAGE_API_KEY")
if (is.null(alpha_key)) {
  stop("ALPHAVANTAGE_API_KEY is missing. Add it to .env")
}

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

message(sprintf("Processing %d symbols: %s", length(symbols), paste(symbols, collapse = ", ")))

# Build data for each symbol with progress
all_frames <- list()
for (sym in symbols) {
  message(sprintf("Fetching data for %s ...", sym))
  all_frames[[sym]] <- make_symbol_frame(sym)
}

master <- bind_rows(all_frames)

# -----------------------------------------------------------------------------
# Persist to MySQL: table Dissertation_Data
# -----------------------------------------------------------------------------
con <- dbConnect(MariaDB(), host = mysql_host, user = mysql_user, password = mysql_password, dbname = mysql_db)
on.exit({ try(dbDisconnect(con), silent = TRUE) }, add = TRUE)

tbl_name <- "Dissertation_Data"
new_rows_appended <- 0L

if (!dbExistsTable(con, tbl_name)) {
  message("Creating table Dissertation_Data ...")
  dbWriteTable(con, tbl_name, master, overwrite = TRUE)
  new_rows_appended <- nrow(master)
} else {
  # Ensure schema: drop legacy columns, ensure sharePrice exists
  fields_now <- dbListFields(con, tbl_name)
  if ("earningsReportDate" %in% fields_now) {
    try(dbExecute(con, "ALTER TABLE `Dissertation_Data` DROP COLUMN `earningsReportDate`"), silent = TRUE)
    fields_now <- setdiff(fields_now, "earningsReportDate")
  }
  for (col in c("earningsDate_3M", "earningsDate_6M", "earningsDate_12M")) {
    if (col %in% fields_now) {
      try(dbExecute(con, paste0("ALTER TABLE `Dissertation_Data` DROP COLUMN `", col, "`")), silent = TRUE)
      fields_now <- setdiff(fields_now, col)
    }
  }
  if (!("sharePrice" %in% fields_now)) {
    try(dbExecute(con, "ALTER TABLE `Dissertation_Data` ADD COLUMN `sharePrice` DOUBLE NULL"), silent = TRUE)
  }
  try(dbExecute(con, "ALTER TABLE `Dissertation_Data` MODIFY COLUMN `sharePrice` DOUBLE NULL"), silent = TRUE)

  # Append-only for new rows, skip duplicates on (symbol, fiscalDateEnding)
  existing <- tryCatch({
    dbGetQuery(con, paste0("SELECT symbol, fiscalDateEnding FROM ", DBI::dbQuoteIdentifier(con, tbl_name)))
  }, error = function(e) tibble(symbol = character(), fiscalDateEnding = as.Date(character())))

  if (!"fiscalDateEnding" %in% names(existing)) {
    existing$fiscalDateEnding <- as.Date(NA)
  }
  existing <- existing |>
    mutate(fiscalDateEnding = suppressWarnings(as.Date(fiscalDateEnding))) |>
    distinct()

  # Ensure same columns for append: align to table fields
  table_fields <- dbListFields(con, tbl_name)
  to_insert <- master

  # Drop columns not in table
  to_insert <- to_insert[, intersect(names(to_insert), table_fields), drop = FALSE]

  # Add any missing columns as NA to match table schema
  missing_cols <- setdiff(table_fields, names(to_insert))
  for (mc in missing_cols) to_insert[[mc]] <- NA
  to_insert <- to_insert[, table_fields, drop = FALSE]

  # Deduplicate against existing keys
  if (nrow(existing)) {
    to_insert <- to_insert |>
      mutate(fiscalDateEnding = suppressWarnings(as.Date(fiscalDateEnding))) |>
      anti_join(existing, by = c("symbol", "fiscalDateEnding"))
  }

  if (nrow(to_insert)) {
    dbWriteTable(con, tbl_name, to_insert, append = TRUE)
    new_rows_appended <- nrow(to_insert)
  }
  # Update existing rows with the new values for these columns via a joined update
  updates <- master |>
    select(any_of(c("symbol", "fiscalDateEnding", "sharePrice"))) |>
    distinct()
  if (nrow(updates)) {
    tmp_name <- paste0("tmp_updates_", as.integer(Sys.time()))
    dbWriteTable(con, tmp_name, updates, temporary = TRUE, overwrite = TRUE)
    qry <- paste0(
      "UPDATE `", tbl_name, "` d JOIN `", tmp_name, "` u",
      " ON d.symbol = u.symbol AND d.fiscalDateEnding = u.fiscalDateEnding ",
      "SET d.sharePrice = COALESCE(u.sharePrice, d.sharePrice)"
    )
    try(dbExecute(con, qry), silent = TRUE)
    try(dbExecute(con, paste0("DROP TEMPORARY TABLE IF EXISTS `", tmp_name, "`")), silent = TRUE)
  }
}

# -----------------------------------------------------------------------------
# Summary (show most recent 3 quarters per symbol)
# -----------------------------------------------------------------------------
num_symbols <- length(unique(master$symbol))

# Compute count of newly appended rows by comparing current table vs before insert is tricky here.
# Instead, report rows present for lookback period.

final_preview <- master |>
  select(any_of(c(
    "symbol", "fiscalDateEnding", "sharePrice"
  ))) |>
  group_by(symbol) |>
  arrange(desc(fiscalDateEnding), .by_group = TRUE) |>
  slice_head(n = 3) |>
  ungroup()

cat("\nSummary:\n")
cat(sprintf("- Symbols processed: %d\n", num_symbols))
cat(sprintf("- New rows appended: %s\n", format(new_rows_appended, big.mark = ",")))

# Try to count newly appended rows by re-querying table for recent window
recent_rows <- tryCatch({
  qry <- paste0(
    "SELECT COUNT(*) AS n FROM ", tbl_name,
    " WHERE fiscalDateEnding >= DATE_SUB(CURDATE(), INTERVAL ", lookback_years, " YEAR)"
  )
  as.integer(dbGetQuery(con, qry)$n[1])
}, error = function(e) NA_integer_)

if (!is.na(recent_rows)) {
  cat(sprintf("- Rows within lookback window now in table: %s\n", format(recent_rows, big.mark = ",")))
}

cat("- Preview (most recent 3 quarters per symbol):\n")
print(final_preview, n = 10, width = Inf)

# Load and persist forward earnings calendar to its own table
ec <- load_earnings_calendar(symbols)
# Ensure table exists regardless of whether we have rows now
try(dbExecute(con, paste0(
  "CREATE TABLE IF NOT EXISTS `Earnings_Calendar` (",
  "`id` INT AUTO_INCREMENT PRIMARY KEY,",
  "`symbol` VARCHAR(10),",
  "`horizon` VARCHAR(10),",
  "`reportDate` DATE,",
  "`pull_date` DATE,",
  "`source` VARCHAR(20))"
)), silent = TRUE)

if (nrow(ec)) {
  # Replace older rows for same (symbol, horizon)
  key_pairs <- ec |>
    select(symbol, horizon) |>
    distinct()
  if (nrow(key_pairs)) {
    invisible(apply(key_pairs, 1, function(row) {
      sym <- row[["symbol"]]; hor <- row[["horizon"]]
      try(dbExecute(con, sprintf(
        "DELETE FROM `Earnings_Calendar` WHERE symbol = '%s' AND horizon = '%s'",
        dbEscapeStrings(con, sym), dbEscapeStrings(con, hor)
      )), silent = TRUE)
    }))
  }
  # Append new rows
  dbWriteTable(con, "Earnings_Calendar", ec, append = TRUE)
}

# Verification output: earnings calendar dates
if (nrow(ec)) {
  cat("\nEarnings Calendar Verification:\n")
  print(ec |> arrange(symbol, horizon) |> select(symbol, horizon, reportDate), n = nrow(ec), width = Inf)
} else {
  cat("\nEarnings Calendar Verification: no dates retrieved.\n")
}

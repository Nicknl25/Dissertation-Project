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
# Date range and symbols are now provided at runtime:
# - Dates via START_DATE / END_DATE env vars (set after load_dot_env() in Main)
# - Symbols loaded dynamically from the database (in Main)

# -----------------------------------------------------------------------------
# Environment and helpers
# -----------------------------------------------------------------------------
load_dot_env <- function() {
  loaded <- FALSE
  try({
    dotenv::load_dot_env(file = ".env")
    loaded <- TRUE
  }, silent = TRUE)
  if (!loaded) {
    # Fallback: if working dir is the script's subfolder (e.g., r/), try parent
    parent_env <- file.path("..", ".env")
    if (file.exists(parent_env)) {
      try(dotenv::load_dot_env(file = parent_env, override = FALSE), silent = TRUE)
      loaded <- TRUE
    } else {
      message("Warning: .env not found in working directory; also tried ../.env")
    }
  }
}

get_env <- function(key) {
  val <- Sys.getenv(key, unset = "")
  val <- trimws(val)
  if (identical(val, "")) return(NULL)
  val
}

# -----------------------------------------------------------------------------
# Resource cleanup helper (prevents file-handle leaks in long runs)
# -----------------------------------------------------------------------------
cleanup_resources <- function() {
  try(closeAllConnections(), silent = TRUE)
  try(gc(), silent = TRUE)
  invisible(TRUE)
}

maybe_cleanup <- function(i, every = 25) {
  if (is.na(every) || every <= 1L || (i %% every) == 0L) {
    cleanup_resources()
  }
}

alpha_key <- NULL
last_request_time <- NULL
polite_get <- function(req) {
  # Fast path for paid plans; retry once on transient errors (429/5xx)
  resp <- req_perform(req)
  status <- try(resp_status(resp), silent = TRUE)
  if (!inherits(status, "try-error") && is.numeric(status) && status %in% c(429, 500, 502, 503, 504)) {
    Sys.sleep(1)
    resp <- req_perform(req)
  }
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
  try(httr2::resp_close(resp), silent = TRUE)

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
    try(httr2::resp_close(resp), silent = TRUE)
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
    # Respect global end_date if available (avoid using future/out-of-range prices)
    if (exists("end_date", inherits = TRUE) && !is.null(end_date) && !is.na(end_date)) {
      df <- df |>
        dplyr::filter(d <= as.Date(end_date))
    }
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
    try(httr2::resp_close(resp), silent = TRUE)
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
    filter(is.na(fiscalDateEnding) | (fiscalDateEnding >= start_date & fiscalDateEnding <= end_date))

  joined
}

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
load_dot_env()

# Fixed date range from environment (robust parsing)
parse_date_env <- function(key) {
  val <- get_env(key)
  if (is.null(val) || identical(val, "")) return(NA_Date_)
  suppressWarnings(as.Date(val))
}
start_date <- parse_date_env("START_DATE")
end_date   <- parse_date_env("END_DATE")
if (is.na(start_date) || is.na(end_date)) {
  stop(sprintf(
    "START_DATE or END_DATE missing/invalid in environment (.env). Working dir: %s", getwd()
  ))
}

# Echo the configured date range once at start
message(sprintf(
  "Run date range: %s to %s",
  as.character(as.Date(start_date)), as.character(as.Date(end_date))
))

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

# Open DB connection early to fetch symbols dynamically
# Ensure we only use RMariaDB (avoid conflicts if RMySQL is attached)
if ("package:RMySQL" %in% search()) {
  try(detach("package:RMySQL", unload = TRUE, character.only = TRUE), silent = TRUE)
}

# Anchor the RMariaDB driver object to avoid weak_ptr lifecycle issues
maria_drv <- RMariaDB::MariaDB()
con <- DBI::dbConnect(
  maria_drv,
  host = mysql_host,
  user = mysql_user,
  password = mysql_password,
  dbname = mysql_db
)
on.exit({ try(DBI::dbDisconnect(con), silent = TRUE) }, add = TRUE)

# Helper to ensure DB connection remains valid across long runs/cleanup
ensure_db_valid <- function() {
  ok <- TRUE
  try({ ok <- DBI::dbIsValid(con) }, silent = TRUE)
  if (!isTRUE(ok)) {
    try(DBI::dbDisconnect(con), silent = TRUE)
    con <<- DBI::dbConnect(
      maria_drv,
      host = mysql_host,
      user = mysql_user,
      password = mysql_password,
      dbname = mysql_db
    )
  }
  invisible(TRUE)
}

# API pacing from env (seconds between symbols)
api_sleep <- as.numeric(get_env("API_SLEEP_SECONDS") %||% 2)
if (is.na(api_sleep) || api_sleep < 0) api_sleep <- 2
orig_api_sleep <- api_sleep
api_sleep <- min(api_sleep, 15)
if (!is.na(orig_api_sleep) && orig_api_sleep > 15) {
  message("⚠️ API_SLEEP_SECONDS capped at 15 seconds (value too high)")
}
message(sprintf("API pacing: %s seconds between symbols", api_sleep))

# Test mode and ticker limit from env
test_mode <- tolower((get_env("TEST_MODE") %||% "false"))
test_mode <- test_mode %in% c("1", "true", "t", "yes", "y")
ticker_limit <- suppressWarnings(as.integer(get_env("TICKER_LIMIT") %||% "10"))
if (is.na(ticker_limit) || ticker_limit <= 0) ticker_limit <- 10

# Build ticker query based on TEST_MODE
qry <- if (isTRUE(test_mode)) {
  sprintf("SELECT Symbol FROM ticker_master ORDER BY Symbol ASC LIMIT %d;", ticker_limit)
} else {
  "SELECT Symbol FROM ticker_master ORDER BY Symbol ASC;"
}
# Fetch tickers with robust reconnect-if-needed to avoid weak_ptr errors
tickers <- tryCatch({
  DBI::dbGetQuery(con, qry)
}, error = function(e) {
  message("Initial dbGetQuery failed (", conditionMessage(e), "). Reconnecting and retrying once.")
  try(DBI::dbDisconnect(con), silent = TRUE)
  con <<- DBI::dbConnect(
    maria_drv,
    host = mysql_host,
    user = mysql_user,
    password = mysql_password,
    dbname = mysql_db
  )
  DBI::dbGetQuery(con, qry)
})
symbols <- tickers$Symbol

if (isTRUE(test_mode)) {
  message(sprintf("Running TEST MODE: start=%s, end=%s, tickers=%d", start_date, end_date, length(symbols)))
}

message(sprintf("Processing %d symbols: %s", length(symbols), paste(symbols, collapse = ", ")))

# -----------------------------------------------------------------------------
# Persist to MySQL: table Dissertation_Data
# -----------------------------------------------------------------------------

# Re-establish connection if it timed out during API calls
if (!DBI::dbIsValid(con)) {
  try(DBI::dbDisconnect(con), silent = TRUE)
  con <- DBI::dbConnect(
    maria_drv,
    host = mysql_host,
    user = mysql_user,
    password = mysql_password,
    dbname = mysql_db
  )
}

tbl_name <- "Dissertation_Data"
new_rows_appended_total <- 0L

# Helpers to coerce and sync schema to avoid 1406 errors

# Heuristic: is a vector numeric-like?
is_numeric_like <- function(x) {
  if (inherits(x, "Date")) return(FALSE)
  if (is.numeric(x)) return(TRUE)
  if (!is.character(x)) return(FALSE)
  y <- trimws(gsub(",", "", x, fixed = TRUE))
  y[y %in% c("", "NA", "NaN", "null", "NULL")] <- NA_character_
  suppressWarnings({
    z <- as.numeric(y)
  })
  nz <- sum(!is.na(z))
  if (length(y) == 0) return(FALSE)
  ratio <- if (length(y) > 0) nz / length(y) else 0
  nz > 0 && ratio >= 0.8
}

# Coerce numeric-like character columns to numeric (DOUBLE on DB side)
coerce_numeric_like_cols <- function(df) {
  keep_text <- function(nm) grepl("(?i)currency|industry|symbol|horizon|source|statement_type", nm)
  for (nm in names(df)) {
    col <- df[[nm]]
    if (inherits(col, "Date") || keep_text(nm)) next
    if (is_numeric_like(col)) {
      suppressWarnings({ df[[nm]] <- as.numeric(gsub(",", "", as.character(col), fixed = TRUE)) })
    }
  }
  df
}

compute_field_types <- function(df) {
  types <- list()
  for (nm in names(df)) {
    col <- df[[nm]]
    if (inherits(col, "Date")) {
      types[[nm]] <- "DATE"
    } else if (is_numeric_like(col)) {
      types[[nm]] <- "DOUBLE NULL"
    } else {
      # Default wide text to avoid length issues
      types[[nm]] <- "TEXT NULL"
    }
  }
  types
}

sync_table_schema <- function(con, tbl, df) {
  info <- tryCatch(DBI::dbGetQuery(con, paste0("SHOW COLUMNS FROM `", tbl, "`")), error = function(e) NULL)
  if (is.null(info)) return(invisible())
  existing <- if (nrow(info)) info$Field else character()
  # Add missing columns
  to_add <- setdiff(names(df), existing)
  if (length(to_add)) {
    ft <- compute_field_types(df[to_add])
    for (nm in names(ft)) {
      sql <- sprintf("ALTER TABLE `%s` ADD COLUMN `%s` %s", tbl, nm, ft[[nm]])
      try(DBI::dbExecute(con, sql), silent = TRUE)
    }
    info <- tryCatch(DBI::dbGetQuery(con, paste0("SHOW COLUMNS FROM `", tbl, "`")), error = function(e) info)
  }
  if (!nrow(info)) return(invisible())
  # Widen or change type where needed
  for (nm in intersect(names(df), info$Field)) {
    desired <- if (inherits(df[[nm]], "Date")) "DATE" else if (is_numeric_like(df[[nm]])) "DOUBLE" else "TEXT"
    t <- tolower(info$Type[info$Field == nm][1])
    is_text <- grepl("text|char", t)
    is_varchar <- grepl("varchar\\((\\d+)\\)", t)
    is_numeric <- grepl("int|double|decimal|float|bigint", t)
    is_date <- grepl("date", t)
    if (identical(desired, "DATE") && !is_date) {
      try(DBI::dbExecute(con, sprintf("ALTER TABLE `%s` MODIFY COLUMN `%s` DATE NULL", tbl, nm)), silent = TRUE)
    } else if (identical(desired, "DOUBLE") && !is_numeric) {
      try(DBI::dbExecute(con, sprintf("ALTER TABLE `%s` MODIFY COLUMN `%s` DOUBLE NULL", tbl, nm)), silent = TRUE)
    } else if (identical(desired, "TEXT")) {
      # Ensure at least VARCHAR(255); prefer TEXT for safety
      if (!(grepl("text", t))) {
        try(DBI::dbExecute(con, sprintf("ALTER TABLE `%s` MODIFY COLUMN `%s` TEXT NULL", tbl, nm)), silent = TRUE)
      }
    } else if (is_text && is_varchar) {
      # Widen varchar(<255) to 255
      m <- regexec("varchar\\((\\d+)\\)", t)
      g <- regmatches(t, m)[[1]]
      if (length(g) >= 2) {
        n <- suppressWarnings(as.integer(g[2])); if (is.na(n) || n < 255) {
          try(DBI::dbExecute(con, sprintf("ALTER TABLE `%s` MODIFY COLUMN `%s` VARCHAR(255) NULL", tbl, nm)), silent = TRUE)
        }
      }
    }
  }
}

# BEGIN: Robust schema sync helpers (override previous definitions)
get_indexed_columns <- function(con, tbl) {
  idx <- tryCatch(DBI::dbGetQuery(con, paste0("SHOW INDEX FROM `", tbl, "`")), error = function(e) NULL)
  if (is.null(idx) || !nrow(idx)) return(character())
  unique(idx$Column_name)
}

compute_field_types <- function(df, con = NULL, tbl = NULL) {
  indexed <- character()
  if (!is.null(con) && !is.null(tbl)) {
    indexed <- get_indexed_columns(con, tbl)
  }
  types <- list()
  for (nm in names(df)) {
    col <- df[[nm]]
    if (inherits(col, "Date")) {
      types[[nm]] <- "DATE"
    } else if (is_numeric_like(col)) {
      types[[nm]] <- "DOUBLE NULL"
    } else {
      # Use TEXT for free-form text; keep index columns (e.g., symbol) as VARCHAR(255)
      if (nm %in% indexed || identical(nm, "symbol")) {
        types[[nm]] <- "VARCHAR(255) NULL"
      } else {
        types[[nm]] <- "TEXT NULL"
      }
    }
  }
  types
}

sync_table_schema <- function(con, tbl, df) {
  info <- tryCatch(DBI::dbGetQuery(con, paste0("SHOW COLUMNS FROM `", tbl, "`")), error = function(e) NULL)
  if (is.null(info)) return(invisible())
  existing <- if (nrow(info)) info$Field else character()
  indexed <- get_indexed_columns(con, tbl)
  # Add any new columns with wide-safe types immediately
  to_add <- setdiff(names(df), existing)
  if (length(to_add)) {
    ft <- compute_field_types(df[to_add], con = con, tbl = tbl)
    for (nm in names(ft)) {
      sql <- sprintf("ALTER TABLE `%s` ADD COLUMN `%s` %s", tbl, nm, ft[[nm]])
      try(DBI::dbExecute(con, sql), silent = TRUE)
    }
    info <- tryCatch(DBI::dbGetQuery(con, paste0("SHOW COLUMNS FROM `", tbl, "`")), error = function(e) info)
  }
  if (!nrow(info)) return(invisible())
  # Enforce desired types: DATE, DOUBLE, TEXT/VARCHAR(255)
  for (nm in intersect(names(df), info$Field)) {
    col <- df[[nm]]
    desired <- if (inherits(col, "Date")) {
      "DATE"
    } else if (is_numeric_like(col)) {
      "DOUBLE"
    } else if (nm %in% indexed || identical(nm, "symbol")) {
      "VARCHAR"
    } else {
      "TEXT"
    }

    t <- tolower(info$Type[info$Field == nm][1])
   is_varchar <- grepl("varchar\\((\\d+)\\)", t, perl = TRUE)
    is_text <- grepl("text", t)
    is_double <- grepl("double", t)
    is_date <- grepl("date", t)

    if (identical(desired, "DATE")) {
      if (!is_date) {
        try(DBI::dbExecute(con, sprintf("ALTER TABLE `%s` MODIFY COLUMN `%s` DATE NULL", tbl, nm)), silent = TRUE)
      }
      next
    }

    if (identical(desired, "DOUBLE")) {
      # Always normalize numeric to DOUBLE NULL (covers INT/DECIMAL/etc.)
      if (!is_double) {
        try(DBI::dbExecute(con, sprintf("ALTER TABLE `%s` MODIFY COLUMN `%s` DOUBLE NULL", tbl, nm)), silent = TRUE)
      }
      next
    }

    if (identical(desired, "TEXT")) {
      if (!is_text) {
        ok <- TRUE
        res <- try(DBI::dbExecute(con, sprintf("ALTER TABLE `%s` MODIFY COLUMN `%s` TEXT NULL", tbl, nm)), silent = TRUE)
        if (inherits(res, "try-error")) ok <- FALSE
        if (!ok) {
          # Fallback if TEXT fails (e.g., indexed): ensure VARCHAR(255)
          try(DBI::dbExecute(con, sprintf("ALTER TABLE `%s` MODIFY COLUMN `%s` VARCHAR(255) NULL", tbl, nm)), silent = TRUE)
        }
      }
    } else if (identical(desired, "VARCHAR")) {
      # Ensure at least VARCHAR(255)
      widen <- TRUE
      if (is_varchar) {
        m <- regexec("varchar\\((\\d+)\\)", t, perl = TRUE)
        g <- regmatches(t, m)[[1]]
        if (length(g) >= 2) {
          n <- suppressWarnings(as.integer(g[2]))
          widen <- is.na(n) || n < 255
        }
      }
      if (widen || !is_varchar) {
        try(DBI::dbExecute(con, sprintf("ALTER TABLE `%s` MODIFY COLUMN `%s` VARCHAR(255) NULL", tbl, nm)), silent = TRUE)
      }
    }
  }
}
# END: Robust schema sync helpers

# Stream processing: handle one symbol at a time
# Close the initial read connection (if still open) before per-ticker processing
try(DBI::dbDisconnect(con), silent = TRUE)

for (i in seq_along(symbols)) {
  sym <- symbols[[i]]
  message(sprintf("Processing %d of %d: %s ...", i, length(symbols), sym))

  # Build data only for this symbol
  sym_df <- make_symbol_frame(sym)

  # Fresh DB connection for this ticker
  con_sym <- DBI::dbConnect(
    maria_drv,
    host = mysql_host,
    user = mysql_user,
    password = mysql_password,
    dbname = mysql_db
  )

  # Create table on first write if needed; otherwise sync and append
  if (!DBI::dbExistsTable(con_sym, tbl_name)) {
    message("Creating table Dissertation_Data ...")
    sym_df <- coerce_numeric_like_cols(sym_df)
    types <- compute_field_types(sym_df, con = con_sym, tbl = tbl_name)
    DBI::dbWriteTable(con_sym, tbl_name, sym_df, overwrite = TRUE, field.types = types)
    appended <- nrow(sym_df)
  } else {
    # Coerce numeric-like cols and sync schema
    sym_df <- coerce_numeric_like_cols(sym_df)
    sync_table_schema(con_sym, tbl_name, sym_df)

    # Ensure schema fixes: drop legacy cols and ensure sharePrice DOUBLE
    fields_now <- DBI::dbListFields(con_sym, tbl_name)
    if ("earningsReportDate" %in% fields_now) {
      try(DBI::dbExecute(con_sym, "ALTER TABLE `Dissertation_Data` DROP COLUMN `earningsReportDate`"), silent = TRUE)
      fields_now <- setdiff(fields_now, "earningsReportDate")
    }
    for (col in c("earningsDate_3M", "earningsDate_6M", "earningsDate_12M")) {
      if (col %in% fields_now) {
        try(DBI::dbExecute(con_sym, paste0("ALTER TABLE `Dissertation_Data` DROP COLUMN `", col, "`")), silent = TRUE)
        fields_now <- setdiff(fields_now, col)
      }
    }
    if (!("sharePrice" %in% fields_now)) {
      try(DBI::dbExecute(con_sym, "ALTER TABLE `Dissertation_Data` ADD COLUMN `sharePrice` DOUBLE NULL"), silent = TRUE)
    }
    try(DBI::dbExecute(con_sym, "ALTER TABLE `Dissertation_Data` MODIFY COLUMN `sharePrice` DOUBLE NULL"), silent = TRUE)

    # Align to table schema and dedup vs existing for this symbol
    table_fields <- DBI::dbListFields(con_sym, tbl_name)
    to_insert <- sym_df[, intersect(names(sym_df), table_fields), drop = FALSE]
    missing_cols <- setdiff(table_fields, names(to_insert))
    for (mc in missing_cols) to_insert[[mc]] <- NA
    to_insert <- to_insert[, table_fields, drop = FALSE]

    existing <- tryCatch({
      qsym <- DBI::dbQuoteString(con_sym, sym)
      DBI::dbGetQuery(con_sym, sprintf("SELECT symbol, fiscalDateEnding FROM `%s` WHERE symbol = %s", tbl_name, as.character(qsym)))
    }, error = function(e) tibble::tibble(symbol = character(), fiscalDateEnding = as.Date(character())))
    if (!"fiscalDateEnding" %in% names(existing)) existing$fiscalDateEnding <- as.Date(NA)
    existing <- dplyr::distinct(dplyr::mutate(existing, fiscalDateEnding = suppressWarnings(as.Date(fiscalDateEnding))))

    if (nrow(existing)) {
      to_insert <- to_insert |>
        dplyr::mutate(fiscalDateEnding = suppressWarnings(as.Date(fiscalDateEnding))) |>
        dplyr::anti_join(existing, by = c("symbol", "fiscalDateEnding"))
    }

    appended <- 0L
    if (nrow(to_insert)) {
      DBI::dbWriteTable(con_sym, tbl_name, to_insert, append = TRUE)
      appended <- nrow(to_insert)
    }

    # Update sharePrice for existing rows for this symbol
    updates <- sym_df |>
      dplyr::select(dplyr::any_of(c("symbol", "fiscalDateEnding", "sharePrice"))) |>
      dplyr::filter(symbol == sym) |>
      dplyr::distinct()
    if (nrow(updates)) {
      tmp_name <- paste0("tmp_updates_", as.integer(Sys.time()), "_", gsub("[^A-Za-z0-9]", "", sym))
      DBI::dbWriteTable(con_sym, tmp_name, updates, overwrite = TRUE)
      qry <- paste0(
        "UPDATE `", tbl_name, "` d JOIN `", tmp_name, "` u",
        " ON d.symbol = u.symbol AND d.fiscalDateEnding = u.fiscalDateEnding ",
        "SET d.sharePrice = COALESCE(u.sharePrice, d.sharePrice)"
      )
      try(DBI::dbExecute(con_sym, qry), silent = TRUE)
      try(DBI::dbExecute(con_sym, paste0("DROP TABLE IF EXISTS `", tmp_name, "`")), silent = TRUE)
    }
  }

  # Earnings calendar: write per symbol to its own table
  ec_row <- try(get_earnings_calendar(sym), silent = TRUE)
  if (!inherits(ec_row, "try-error") && !is.null(ec_row) && nrow(ec_row)) {
    long_ec <- tibble::tibble(
      symbol = sym,
      horizon = c("3month", "6month", "12month"),
      reportDate = as.Date(c(ec_row$earningsDate_3M[1], ec_row$earningsDate_6M[1], ec_row$earningsDate_12M[1])),
      pull_date = Sys.Date(),
      source = "AlphaVantage"
    ) |>
      dplyr::filter(!is.na(reportDate))
    # Ensure table exists
    try(DBI::dbExecute(con_sym, paste0(
      "CREATE TABLE IF NOT EXISTS `Earnings_Calendar` (",
      "`id` INT AUTO_INCREMENT PRIMARY KEY,",
      "`symbol` VARCHAR(10),",
      "`horizon` VARCHAR(10),",
      "`reportDate` DATE,",
      "`pull_date` DATE,",
      "`source` VARCHAR(20))"
    )), silent = TRUE)
    # Replace rows for this symbol
    qsym <- DBI::dbQuoteString(con_sym, sym)
    try(DBI::dbExecute(con_sym, sprintf("DELETE FROM `Earnings_Calendar` WHERE symbol = %s", as.character(qsym))), silent = TRUE)
    if (nrow(long_ec)) DBI::dbWriteTable(con_sym, "Earnings_Calendar", long_ec, append = TRUE)
  }

  new_rows_appended_total <- new_rows_appended_total + as.integer(appended)
  message(sprintf("Appended %d new rows for %s", as.integer(appended), sym))

  # Disconnect and cleanup before moving to the next ticker
  try(DBI::dbDisconnect(con_sym), silent = TRUE)
  if (exists(sym, envir = price_cache, inherits = FALSE)) {
    try(rm(list = sym, envir = price_cache), silent = TRUE)
  }
  cleanup_resources()
  Sys.sleep(api_sleep)
}

cat("\nSummary:\n")
cat(sprintf("- Symbols processed: %d\n", length(symbols)))
cat(sprintf("- New rows appended: %s\n", format(new_rows_appended_total, big.mark = ",")))

# Count rows in date range now present in the table
recent_rows <- tryCatch({
  con_chk <- DBI::dbConnect(
    maria_drv,
    host = mysql_host,
    user = mysql_user,
    password = mysql_password,
    dbname = mysql_db
  )
  on.exit({ try(DBI::dbDisconnect(con_chk), silent = TRUE) }, add = TRUE)
  s <- DBI::dbQuoteString(con_chk, as.character(as.Date(start_date)))
  e <- DBI::dbQuoteString(con_chk, as.character(as.Date(end_date)))
  qry <- sprintf(
    "SELECT COUNT(*) AS n FROM `%s` WHERE fiscalDateEnding BETWEEN %s AND %s",
    tbl_name, as.character(s), as.character(e)
  )
  as.integer(DBI::dbGetQuery(con_chk, qry)$n[1])
}, error = function(e) NA_integer_)

if (!is.na(recent_rows)) {
  cat(sprintf("- Rows within date range now in table: %s\n", format(recent_rows, big.mark = ",")))
}

## Earnings calendar is written per-ticker during streaming above

# -----------------------------------------------------------------------------
# Final: refresh derived fields via stored procedure
# -----------------------------------------------------------------------------
tryCatch({
  ensure_db_valid()
  DBI::dbExecute(con, "CALL sp_refresh_market_cap();")
  message("✅ Market cap, year_qtr, and cap_category refreshed successfully.")
}, error = function(e) {
  message("⚠️ Stored procedure refresh failed: ", e$message)
})

# -----------------------------------------------------------------------------
# Backfill: load any tickers missing from Dissertation_Data (DISABLED)
# -----------------------------------------------------------------------------
# Temporarily disabled: main streaming loader handles inserts/dedup. Keeping code
# in place for future gap detection work. Wrapped in if (FALSE) to prevent eval.
if (FALSE) {
missing_syms <- tryCatch({
  s <- DBI::dbQuoteString(con, as.character(as.Date(start_date)))
  e <- DBI::dbQuoteString(con, as.character(as.Date(end_date)))
  sql <- paste0(
    "SELECT tm.Symbol AS Symbol FROM `ticker_master` tm ",
    "LEFT JOIN (",
    "  SELECT DISTINCT symbol FROM `", tbl_name, "` ",
    "  WHERE fiscalDateEnding BETWEEN ", as.character(s), " AND ", as.character(e),
    ") d ON tm.Symbol = d.symbol ",
    "WHERE d.symbol IS NULL ",
    "ORDER BY tm.Symbol ASC"
  )
  df <- DBI::dbGetQuery(con, sql)
  unique(df$Symbol)
}, error = function(e) character())

# Respect TEST_MODE and TICKER_LIMIT for backfill as well
if (isTRUE(test_mode)) {
  original_missing <- length(missing_syms)
  if (original_missing > ticker_limit) {
    missing_syms <- utils::head(missing_syms, ticker_limit)
  }
  message(sprintf(
    "Backfill TEST MODE: limiting to %d of %d missing tickers",
    length(missing_syms), original_missing
  ))
}

if (length(missing_syms)) {
  message(sprintf(
    "Backfill: %d tickers with no rows in date range %s to %s.",
    length(missing_syms), as.character(as.Date(start_date)), as.character(as.Date(end_date))
  ))
  for (i in seq_along(missing_syms)) {
    sym <- missing_syms[[i]]
    message(sprintf("Backfill %d of %d: %s ...", i, length(missing_syms), sym))
    # Build, write, and release this ticker immediately (no accumulation)
    sym_df <- make_symbol_frame(sym)

    con_sym <- DBI::dbConnect(
      maria_drv,
      host = mysql_host,
      user = mysql_user,
      password = mysql_password,
      dbname = mysql_db
    )

    sym_df <- coerce_numeric_like_cols(sym_df)
    sync_table_schema(con_sym, tbl_name, sym_df)
    table_fields <- DBI::dbListFields(con_sym, tbl_name)
    to_insert_bf <- sym_df[, intersect(names(sym_df), table_fields), drop = FALSE]
    missing_cols <- setdiff(table_fields, names(to_insert_bf))
    for (mc in missing_cols) to_insert_bf[[mc]] <- NA
    to_insert_bf <- to_insert_bf[, table_fields, drop = FALSE]

    existing_bf <- tryCatch({
      qsym <- DBI::dbQuoteString(con_sym, sym)
      DBI::dbGetQuery(con_sym, sprintf("SELECT symbol, fiscalDateEnding FROM `%s` WHERE symbol = %s", tbl_name, as.character(qsym)))
    }, error = function(e) tibble::tibble(symbol = character(), fiscalDateEnding = as.Date(character())))
    if (!"fiscalDateEnding" %in% names(existing_bf)) existing_bf$fiscalDateEnding <- as.Date(NA)
    existing_bf <- dplyr::distinct(dplyr::mutate(existing_bf, fiscalDateEnding = suppressWarnings(as.Date(fiscalDateEnding))))
    if (nrow(existing_bf)) {
      to_insert_bf <- to_insert_bf |>
        dplyr::mutate(fiscalDateEnding = suppressWarnings(as.Date(fiscalDateEnding))) |>
        dplyr::anti_join(existing_bf, by = c("symbol", "fiscalDateEnding"))
    }
    if (nrow(to_insert_bf)) {
      DBI::dbWriteTable(con_sym, tbl_name, to_insert_bf, append = TRUE)
    }

    # Earnings calendar for this symbol
    ec_row <- try(get_earnings_calendar(sym), silent = TRUE)
    if (!inherits(ec_row, "try-error") && !is.null(ec_row) && nrow(ec_row)) {
      long_ec <- tibble::tibble(
        symbol = sym,
        horizon = c("3month", "6month", "12month"),
        reportDate = as.Date(c(ec_row$earningsDate_3M[1], ec_row$earningsDate_6M[1], ec_row$earningsDate_12M[1])),
        pull_date = Sys.Date(),
        source = "AlphaVantage"
      ) |>
        dplyr::filter(!is.na(reportDate))
      try(DBI::dbExecute(con_sym, paste0(
        "CREATE TABLE IF NOT EXISTS `Earnings_Calendar` (",
        "`id` INT AUTO_INCREMENT PRIMARY KEY,",
        "`symbol` VARCHAR(10),",
        "`horizon` VARCHAR(10),",
        "`reportDate` DATE,",
        "`pull_date` DATE,",
        "`source` VARCHAR(20))"
      )), silent = TRUE)
      qsym <- DBI::dbQuoteString(con_sym, sym)
      try(DBI::dbExecute(con_sym, sprintf("DELETE FROM `Earnings_Calendar` WHERE symbol = %s", as.character(qsym))), silent = TRUE)
      if (nrow(long_ec)) DBI::dbWriteTable(con_sym, "Earnings_Calendar", long_ec, append = TRUE)
    }

    # Release resources before the next ticker
    try(DBI::dbDisconnect(con_sym), silent = TRUE)
    if (exists(sym, envir = price_cache, inherits = FALSE)) try(rm(list = sym, envir = price_cache), silent = TRUE)
    cleanup_resources()
    # Keep pacing the same
    maybe_cleanup(i, every = 25)
    Sys.sleep(api_sleep)
  }
  message("✅ Backfill complete")
}
} else {
  message("Backfill disabled: skipping gap detection and insert step.")
}

# Optional: total row count confirmation (short-lived connection)
con_chk <- DBI::dbConnect(
  maria_drv,
  host = mysql_host,
  user = mysql_user,
  password = mysql_password,
  dbname = mysql_db
)
on.exit({ try(DBI::dbDisconnect(con_chk), silent = TRUE) }, add = TRUE)
total <- tryCatch({
  DBI::dbGetQuery(con_chk, sprintf("SELECT COUNT(*) AS n FROM `%s`", tbl_name))
}, error = function(e) NULL)
if (!is.null(total)) {
  message(sprintf("✅ Total rows now in %s: %d", tbl_name, as.integer(total$n[1])))
}

# --- Final cleanup ---
try({ closeAllConnections() }, silent = TRUE)
if ("httr2" %in% loadedNamespaces()) try(httr2::req_perform_cancel_all(), silent = TRUE)
invisible(gc(full = TRUE)); Sys.sleep(0.2); invisible(gc(full = TRUE))

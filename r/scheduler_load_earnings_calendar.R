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

ensure_packages(c("dotenv", "httr2", "jsonlite", "DBI", "RMariaDB", "tidyverse", "lubridate"))

suppressPackageStartupMessages({
  library(dotenv)
  library(DBI)
  library(RMariaDB)
  library(lubridate)
})

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

main <- function() {
  load_dot_env()

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

  start_time <- Sys.time()
  status <- "SUCCESS"

  # Run the earnings calendar loader
  tryCatch({
    source("r/load_earnings_calendar.R")
  }, error = function(e) {
    status <<- "ERROR"
    message("Loader error: ", conditionMessage(e))
  })

  runtime_seconds <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))

  # Connect for logging and counting
  con <- dbConnect(MariaDB(), host = mysql_host, user = mysql_user, password = mysql_password, dbname = mysql_db)
  on.exit({ try(dbDisconnect(con), silent = TRUE) }, add = TRUE)

  # Ensure log table exists
  try(dbExecute(con, paste0(
    "CREATE TABLE IF NOT EXISTS `Earnings_Calendar_Log` (",
    "`id` INT AUTO_INCREMENT PRIMARY KEY,",
    "`run_timestamp` DATETIME,",
    "`records_inserted` INT,",
    "`runtime_seconds` DOUBLE,",
    "`status` VARCHAR(20))"
  )), silent = TRUE)

  # Count rows loaded today (by pull_date)
  inserted <- 0L
  if (status == "SUCCESS") {
    try({
      qry <- "SELECT COUNT(*) AS n FROM `Earnings_Calendar` WHERE pull_date = CURDATE()"
      inserted <- as.integer(dbGetQuery(con, qry)$n[1])
    }, silent = TRUE)
  }

  # Insert log row
  try(dbExecute(con, sprintf(
    "INSERT INTO `Earnings_Calendar_Log` (run_timestamp, records_inserted, runtime_seconds, status) VALUES (NOW(), %d, %.3f, '%s')",
    inserted, runtime_seconds, status
  )), silent = TRUE)

  # Print summary line
  stamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  if (identical(status, "SUCCESS")) {
    cat(sprintf("\u2705 Earnings calendar updated: %d rows inserted (%s)\n", inserted, stamp))
  } else {
    cat(sprintf("\u274C Earnings calendar update failed after %.1fs (%s)\n", runtime_seconds, stamp))
  }
}

if (identical(environment(), globalenv())) {
  try(main(), silent = FALSE)
}


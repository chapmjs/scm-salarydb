# SCM Salary Data Downloader to MySQL
# This script downloads BLS data and stores it in MySQL database
# Database: chapmjs_scm_salarydb on mexico.bbfarm.org

# Load required libraries
library(RMySQL)
library(DBI)
library(dplyr)
library(jsonlite)
library(stringr)
library(lubridate)

# Install blsAPI if needed
if (!requireNamespace("blsAPI", quietly = TRUE)) {
  if (requireNamespace("devtools", quietly = TRUE)) {
    devtools::install_github("mikeasilva/blsAPI", quiet = TRUE)
  } else {
    stop("Please install devtools first: install.packages('devtools')")
  }
}
library(blsAPI)

# Configuration - all values from environment variables
DB_CONFIG <- list(
  host = Sys.getenv("MYSQL_HOST"),
  dbname = Sys.getenv("MYSQL_DATABASE"),
  username = Sys.getenv("MYSQL_USERNAME"),
  password = Sys.getenv("MYSQL_PASSWORD")
)

BLS_API_KEY <- Sys.getenv("BLS_KEY")

# Utility Functions
log_message <- function(message, level = "INFO") {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  cat(sprintf("[%s] %s: %s\n", timestamp, level, message))
}

# Database connection function
get_db_connection <- function() {
  tryCatch({
    conn <- dbConnect(
      MySQL(),
      host = DB_CONFIG$host,
      dbname = DB_CONFIG$dbname,
      username = DB_CONFIG$username,
      password = DB_CONFIG$password
    )
    return(conn)
  }, error = function(e) {
    log_message(paste("Database connection failed:", e$message), "ERROR")
    stop(e)
  })
}

# Check if data already exists for a given year
check_data_exists <- function(conn, year) {
  query <- "CALL CheckDataExists(?, @exists, @count)"
  dbExecute(conn, query, params = list(year))
  
  result <- dbGetQuery(conn, "SELECT @exists as exists_flag, @count as record_count")
  return(list(
    exists = as.logical(result$exists_flag),
    count = result$record_count
  ))
}

# Get occupation definitions from database
get_occupation_definitions <- function(conn, category = "both") {
  if (category == "both") {
    query <- "SELECT occupation_code, occupation_name FROM occupation_definitions WHERE is_active = TRUE"
  } else {
    query <- "SELECT occupation_code, occupation_name FROM occupation_definitions WHERE occupation_category = ? AND is_active = TRUE"
  }
  
  if (category == "both") {
    result <- dbGetQuery(conn, query)
  } else {
    result <- dbGetQuery(conn, query, params = list(category))
  }
  
  # Convert to named list for compatibility with original code
  occupations <- setNames(result$occupation_name, result$occupation_code)
  return(occupations)
}

# BLS API Functions (from original script)
construct_series_ids <- function(occupation_code) {
  clean_code <- sprintf("%06s", gsub("-", "", occupation_code))
  base_id <- paste0("OEUN0000000000000", clean_code)
  series_ids <- paste0(base_id, c("01", "04", "13"))
  names(series_ids) <- c("employment", "mean_wage", "median_wage")
  return(series_ids)
}

get_occupation_data <- function(occupation_code, year, max_retries = 3) {
  series_ids <- construct_series_ids(occupation_code)
  
  payload <- list(
    'seriesid' = as.vector(series_ids),
    'startyear' = as.character(year),
    'endyear' = as.character(year),
    'registrationKey' = BLS_API_KEY
  )
  
  for(attempt in 1:max_retries) {
    tryCatch({
      response <- blsAPI(payload, api_version = 2)
      json_data <- fromJSON(response)
      
      if(json_data$status == "REQUEST_SUCCEEDED") {
        return(json_data)
      } else {
        log_message(paste("BLS API returned status:", json_data$status), "WARN")
        if(attempt == max_retries) return(NULL)
      }
    }, error = function(e) {
      log_message(paste("API call attempt", attempt, "failed:", e$message), "WARN")
      if(attempt == max_retries) return(NULL)
      Sys.sleep(1)
    })
  }
  return(NULL)
}

process_occupation_data <- function(api_response, occupation_code, occupation_name) {
  if(is.null(api_response) || is.null(api_response$Results) || is.null(api_response$Results$series)) {
    return(list(
      occupation_code = occupation_code,
      employment = NA,
      median_wage = NA,
      mean_wage = NA,
      data_available = FALSE,
      raw_response = NULL
    ))
  }
  
  series_df <- api_response$Results$series
  
  if(!is.data.frame(series_df) || nrow(series_df) == 0) {
    return(list(
      occupation_code = occupation_code,
      employment = NA,
      median_wage = NA,
      mean_wage = NA,
      data_available = FALSE,
      raw_response = NULL
    ))
  }
  
  results <- list(employment = NA, median_wage = NA, mean_wage = NA)
  
  for(i in 1:nrow(series_df)) {
    tryCatch({
      series_id <- series_df$seriesID[i]
      
      if(!"data" %in% names(series_df) || !is.list(series_df$data)) {
        next
      }
      
      series_data <- series_df$data[[i]]
      
      if(is.null(series_data) || !is.data.frame(series_data) || nrow(series_data) == 0 || !"value" %in% names(series_data)) {
        next
      }
      
      raw_value <- series_data$value[1]
      if(is.na(raw_value) || raw_value == "" || raw_value == "-") {
        next
      }
      
      value <- as.numeric(raw_value)
      if(is.na(value)) {
        next
      }
      
      if(grepl("01$", series_id)) {
        results$employment <- value
      } else if(grepl("04$", series_id)) {
        results$mean_wage <- value
      } else if(grepl("13$", series_id)) {
        results$median_wage <- value
      }
      
    }, error = function(e) {
      log_message(paste("Error processing series data:", e$message), "WARN")
    })
  }
  
  return(list(
    occupation_code = occupation_code,
    employment = results$employment,
    median_wage = results$median_wage,
    mean_wage = results$mean_wage,
    data_available = !all(is.na(c(results$employment, results$median_wage, results$mean_wage))),
    raw_response = toJSON(api_response, auto_unbox = TRUE)
  ))
}

# Calculate derived fields
calculate_derived_fields <- function(data_row) {
  median_hourly <- if(!is.na(data_row$median_wage)) data_row$median_wage / 2080 else NA
  mean_hourly <- if(!is.na(data_row$mean_wage)) data_row$mean_wage / 2080 else NA
  
  wage_ratio <- if(!is.na(data_row$mean_wage) && !is.na(data_row$median_wage) && data_row$median_wage > 0) {
    data_row$mean_wage / data_row$median_wage
  } else NA
  
  wage_distribution <- if(!is.na(wage_ratio)) {
    if(wage_ratio > 1.15) "Right-skewed (high earners)"
    else if(wage_ratio < 0.85) "Left-skewed (compressed)"  
    else "Relatively symmetric"
  } else NULL
  
  return(list(
    median_hourly = median_hourly,
    mean_hourly = mean_hourly,
    wage_ratio = wage_ratio,
    wage_distribution = wage_distribution
  ))
}

# Insert data into database
insert_salary_data <- function(conn, data_list, year) {
  # Get series IDs for storage
  series_ids <- construct_series_ids(data_list$occupation_code)
  
  # Calculate derived fields
  derived <- calculate_derived_fields(data_list)
  
  # Prepare insert query
  query <- "
    INSERT INTO scm_salary_data (
      occupation_code, data_year, employment, median_wage, mean_wage,
      median_hourly, mean_hourly, wage_ratio, wage_distribution,
      data_available, bls_employment_series_id, bls_median_wage_series_id, 
      bls_mean_wage_series_id, raw_api_response
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      employment = VALUES(employment),
      median_wage = VALUES(median_wage),
      mean_wage = VALUES(mean_wage),
      median_hourly = VALUES(median_hourly),
      mean_hourly = VALUES(mean_hourly),
      wage_ratio = VALUES(wage_ratio),
      wage_distribution = VALUES(wage_distribution),
      data_available = VALUES(data_available),
      raw_api_response = VALUES(raw_api_response),
      updated_date = CURRENT_TIMESTAMP
  "
  
  dbExecute(conn, query, params = list(
    data_list$occupation_code,
    year,
    data_list$employment,
    data_list$median_wage,
    data_list$mean_wage,
    derived$median_hourly,
    derived$mean_hourly,
    derived$wage_ratio,
    derived$wage_distribution,
    data_list$data_available,
    series_ids[["employment"]],
    series_ids[["median_wage"]],
    series_ids[["mean_wage"]],
    data_list$raw_response
  ))
}

# Log refresh attempt
log_refresh <- function(conn, year, occupation_set, total_occupations, successful_count, api_calls, duration, errors = 0, error_details = NULL) {
  status <- if(errors == 0) "success" else if(successful_count > 0) "partial" else "failed"
  
  query <- "
    INSERT INTO data_refresh_log (
      data_year, occupation_set, occupations_requested, occupations_successful,
      api_calls_made, refresh_duration_seconds, error_count, error_details, refresh_status
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  "
  
  dbExecute(conn, query, params = list(
    year, occupation_set, total_occupations, successful_count, 
    api_calls, duration, errors, error_details, status
  ))
}

# Main download function
download_scm_data <- function(year = 2024, occupation_set = "core", force_refresh = FALSE) {
  if(BLS_API_KEY == "") {
    stop("BLS API key not found. Please set BLS_KEY environment variable.")
  }
  
  # Check all required database environment variables
  required_vars <- c("MYSQL_HOST", "MYSQL_DATABASE", "MYSQL_USERNAME", "MYSQL_PASSWORD")
  missing_vars <- required_vars[sapply(required_vars, function(x) Sys.getenv(x) == "")]
  
  if(length(missing_vars) > 0) {
    stop(paste("Missing required environment variables:", paste(missing_vars, collapse = ", ")))
  }
  
  log_message("Starting SCM salary data download...")
  start_time <- Sys.time()
  
  # Connect to database
  conn <- get_db_connection()
  on.exit(dbDisconnect(conn))
  
  # Check if data already exists
  existing_data <- check_data_exists(conn, year)
  if(existing_data$exists && !force_refresh) {
    log_message(paste("Data for year", year, "already exists (", existing_data$count, "records). Use force_refresh=TRUE to update."))
    return(existing_data)
  }
  
  # Get occupation definitions
  occupations <- get_occupation_definitions(conn, occupation_set)
  total_occupations <- length(occupations)
  log_message(paste("Retrieved", total_occupations, "occupations from database"))
  
  # Download data
  successful_count <- 0
  api_calls <- 0
  errors <- 0
  error_messages <- c()
  
  for(i in seq_along(occupations)) {
    code <- names(occupations)[i]
    name <- occupations[[code]]
    
    log_message(paste("Processing", i, "of", total_occupations, ":", name))
    
    if(i > 1) Sys.sleep(0.5) # Rate limiting
    
    # Get data from BLS API
    api_calls <- api_calls + 1
    raw_data <- get_occupation_data(code, year)
    processed_data <- process_occupation_data(raw_data, code, name)
    
    # Insert into database
    tryCatch({
      insert_salary_data(conn, processed_data, year)
      if(processed_data$data_available) {
        successful_count <- successful_count + 1
        log_message(paste("✓ Successfully processed:", name))
      } else {
        log_message(paste("⚠ No data available for:", name), "WARN")
      }
    }, error = function(e) {
      errors <- errors + 1
      error_msg <- paste("Failed to insert data for", name, ":", e$message)
      error_messages <- c(error_messages, error_msg)
      log_message(error_msg, "ERROR")
    })
  }
  
  # Log the refresh
  duration <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
  error_details <- if(length(error_messages) > 0) paste(error_messages, collapse = "; ") else NULL
  
  log_refresh(conn, year, occupation_set, total_occupations, successful_count, api_calls, duration, errors, error_details)
  
  # Summary
  log_message("Download completed!")
  log_message(paste("Total occupations processed:", total_occupations))
  log_message(paste("Successful downloads:", successful_count))
  log_message(paste("API calls made:", api_calls))
  log_message(paste("Errors:", errors))
  log_message(paste("Duration:", round(duration, 2), "seconds"))
  
  return(list(
    total = total_occupations,
    successful = successful_count,
    errors = errors,
    duration = duration
  ))
}

# Usage examples and main execution
if(interactive()) {
  # Interactive usage
  cat("SCM Salary Data Downloader\n")
  cat("Usage examples:\n")
  cat("  download_scm_data(2024, 'core')     # Download core occupations for 2024\n")
  cat("  download_scm_data(2023, 'both')     # Download all occupations for 2023\n")
  cat("  download_scm_data(2024, 'core', TRUE)  # Force refresh existing data\n")
} else {
  # Script execution
  args <- commandArgs(trailingOnly = TRUE)
  
  year <- if(length(args) >= 1) as.numeric(args[1]) else 2024
  occupation_set <- if(length(args) >= 2) args[2] else "core"
  force_refresh <- if(length(args) >= 3) as.logical(args[3]) else FALSE
  
  result <- download_scm_data(year, occupation_set, force_refresh)
}

# Helper function to view recent data
view_recent_data <- function(limit = 10) {
  conn <- get_db_connection()
  on.exit(dbDisconnect(conn))
  
  query <- "
    SELECT 
      od.occupation_name,
      sd.data_year,
      sd.employment,
      sd.median_wage,
      sd.mean_wage,
      sd.data_available
    FROM scm_salary_data sd
    JOIN occupation_definitions od ON sd.occupation_code = od.occupation_code
    WHERE sd.data_available = TRUE
    ORDER BY sd.updated_date DESC
    LIMIT ?
  "
  
  result <- dbGetQuery(conn, query, params = list(limit))
  return(result)
}
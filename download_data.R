# Source the downloader script
source("scm_data_downloader.R")

# Download core occupations for 2024
download_scm_data(year = 2024, occupation_set = "core")

# Download all occupations for 2023
download_scm_data(year = 2023, occupation_set = "both")

# Force refresh existing data
download_scm_data(year = 2024, occupation_set = "core", force_refresh = TRUE)
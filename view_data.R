# View recent downloads
recent_data <- view_recent_data(limit = 20)
print(recent_data)

# Connect and run custom queries
conn <- get_db_connection()

# Get summary by occupation level
level_summary <- dbGetQuery(conn, "SELECT * FROM v_salary_summary_by_level ORDER BY avg_median_wage DESC")

# Get current year data
current_data <- dbGetQuery(conn, "SELECT * FROM v_current_salary_data WHERE data_available = TRUE ORDER BY median_wage DESC")

dbDisconnect(conn)
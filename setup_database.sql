-- SCM Salary Database Setup Script

-- Drop tables if they exist (for clean setup)
DROP TABLE IF EXISTS scm_salary_data;
DROP TABLE IF EXISTS occupation_definitions;
DROP TABLE IF EXISTS data_refresh_log;

-- Table 1: Occupation Definitions
-- This table stores the occupation codes and their metadata
CREATE TABLE occupation_definitions (
    occupation_code VARCHAR(10) PRIMARY KEY,
    occupation_name VARCHAR(255) NOT NULL,
    occupation_category ENUM('core', 'extended') NOT NULL,
    occupation_level ENUM(
        'Management', 
        'Core SCM Professional', 
        'SCM-Adjacent Analytical', 
        'Operational/Support', 
        'Other'
    ) DEFAULT 'Other',
    scm_function ENUM(
        'Procurement & Sourcing',
        'Transportation & Logistics', 
        'Supply Chain Planning',
        'Production Planning',
        'Supply Chain Analysis',
        'Process Optimization',
        'General Operations',
        'Other SCM Functions'
    ) DEFAULT 'Other SCM Functions',
    is_active BOOLEAN DEFAULT TRUE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_category (occupation_category),
    INDEX idx_level (occupation_level),
    INDEX idx_function (scm_function)
);

-- Table 2: SCM Salary Data
-- This table stores the actual salary and employment data by year
CREATE TABLE scm_salary_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    occupation_code VARCHAR(10) NOT NULL,
    data_year YEAR NOT NULL,
    employment INT DEFAULT NULL,
    median_wage DECIMAL(10,2) DEFAULT NULL,
    mean_wage DECIMAL(10,2) DEFAULT NULL,
    median_hourly DECIMAL(8,2) DEFAULT NULL,
    mean_hourly DECIMAL(8,2) DEFAULT NULL,
    wage_ratio DECIMAL(6,4) DEFAULT NULL,
    wage_distribution ENUM(
        'Right-skewed (high earners)',
        'Left-skewed (compressed)',
        'Relatively symmetric'
    ) DEFAULT NULL,
    data_available BOOLEAN DEFAULT FALSE,
    bls_employment_series_id VARCHAR(50) DEFAULT NULL,
    bls_median_wage_series_id VARCHAR(50) DEFAULT NULL,
    bls_mean_wage_series_id VARCHAR(50) DEFAULT NULL,
    raw_api_response JSON DEFAULT NULL,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (occupation_code) REFERENCES occupation_definitions(occupation_code) ON DELETE CASCADE,
    UNIQUE KEY unique_occupation_year (occupation_code, data_year),
    INDEX idx_year (data_year),
    INDEX idx_data_available (data_available),
    INDEX idx_median_wage (median_wage),
    INDEX idx_employment (employment)
);

-- Table 3: Data Refresh Log
-- This table tracks when data was last updated for audit purposes
CREATE TABLE data_refresh_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    refresh_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_year YEAR NOT NULL,
    occupation_set ENUM('core', 'extended', 'both') NOT NULL,
    occupations_requested INT NOT NULL,
    occupations_successful INT NOT NULL,
    api_calls_made INT NOT NULL,
    refresh_duration_seconds INT DEFAULT NULL,
    bls_api_key_used VARCHAR(50) DEFAULT NULL,
    error_count INT DEFAULT 0,
    error_details TEXT DEFAULT NULL,
    refresh_status ENUM('success', 'partial', 'failed') DEFAULT 'success',
    INDEX idx_refresh_date (refresh_date),
    INDEX idx_data_year (data_year)
);

-- Insert Core SCM Occupations
INSERT INTO occupation_definitions (occupation_code, occupation_name, occupation_category, occupation_level, scm_function) VALUES
-- Core SCM Management
('11-3061', 'Purchasing Managers', 'core', 'Management', 'Procurement & Sourcing'),
('11-3071', 'Transportation, Storage, and Distribution Managers', 'core', 'Management', 'Transportation & Logistics'),
('11-9199', 'Managers, All Other (includes Operations Managers)', 'core', 'Management', 'General Operations'),

-- Core SCM Professional/Analytical
('13-1081', 'Logisticians', 'core', 'Core SCM Professional', 'Supply Chain Planning'),
('13-1023', 'Purchasing Agents, Except Wholesale, Retail, and Farm Products', 'core', 'Core SCM Professional', 'Procurement & Sourcing'),
('13-1022', 'Wholesale and Retail Buyers, Except Farm Products', 'core', 'Core SCM Professional', 'Procurement & Sourcing'),
('13-1199', 'Business Operations Specialists, All Other (includes Supply Chain Analysts)', 'core', 'Core SCM Professional', 'Supply Chain Analysis'),

-- SCM-Adjacent Analytical Roles
('13-1111', 'Management Analysts (often work on supply chain optimization)', 'core', 'SCM-Adjacent Analytical', 'Process Optimization'),
('15-2031', 'Operations Research Analysts', 'core', 'SCM-Adjacent Analytical', 'Process Optimization'),
('17-2112', 'Industrial Engineers', 'core', 'SCM-Adjacent Analytical', 'Process Optimization'),

-- Core SCM Operational/Support
('43-5011', 'Cargo and Freight Agents', 'core', 'Operational/Support', 'Transportation & Logistics'),
('43-5061', 'Production, Planning, and Expediting Clerks', 'core', 'Operational/Support', 'Production Planning'),
('43-5071', 'Shipping, Receiving, and Traffic Clerks', 'core', 'Operational/Support', 'Transportation & Logistics'),
('53-1047', 'Traffic Technicians', 'core', 'Operational/Support', 'Transportation & Logistics');

-- Insert Extended SCM Occupations
INSERT INTO occupation_definitions (occupation_code, occupation_name, occupation_category, occupation_level, scm_function) VALUES
('13-1021', 'Buyers and Purchasing Agents, Farm Products', 'extended', 'Core SCM Professional', 'Procurement & Sourcing'),
('43-5021', 'Couriers and Messengers', 'extended', 'Operational/Support', 'Transportation & Logistics'),
('43-5052', 'Postal Service Mail Carriers', 'extended', 'Operational/Support', 'Transportation & Logistics'),
('53-7064', 'Packers and Packagers, Hand', 'extended', 'Operational/Support', 'Other SCM Functions'),
('53-7065', 'Stockers and Order Fillers', 'extended', 'Operational/Support', 'Other SCM Functions');

-- Create useful views for common queries
CREATE VIEW v_current_salary_data AS
SELECT 
    od.occupation_code,
    od.occupation_name,
    od.occupation_category,
    od.occupation_level,
    od.scm_function,
    sd.data_year,
    sd.employment,
    sd.median_wage,
    sd.mean_wage,
    sd.median_hourly,
    sd.wage_ratio,
    sd.wage_distribution,
    sd.data_available
FROM occupation_definitions od
LEFT JOIN scm_salary_data sd ON od.occupation_code = sd.occupation_code
WHERE sd.data_year = (SELECT MAX(data_year) FROM scm_salary_data WHERE data_available = TRUE)
   OR sd.data_year IS NULL;

CREATE VIEW v_salary_summary_by_level AS
SELECT 
    occupation_level,
    data_year,
    COUNT(*) as occupation_count,
    SUM(employment) as total_employment,
    AVG(median_wage) as avg_median_wage,
    MIN(median_wage) as min_median_wage,
    MAX(median_wage) as max_median_wage,
    STDDEV(median_wage) as stddev_median_wage
FROM v_current_salary_data
WHERE data_available = TRUE
GROUP BY occupation_level, data_year;

-- Create stored procedures for common operations

DELIMITER //

-- Procedure to check if data exists for a given year
CREATE PROCEDURE CheckDataExists(
    IN p_year YEAR,
    OUT p_exists BOOLEAN,
    OUT p_record_count INT
)
BEGIN
    SELECT COUNT(*) INTO p_record_count
    FROM scm_salary_data 
    WHERE data_year = p_year AND data_available = TRUE;
    
    SET p_exists = (p_record_count > 0);
END //

-- Procedure to get the latest refresh information
CREATE PROCEDURE GetLatestRefreshInfo()
BEGIN
    SELECT 
        refresh_date,
        data_year,
        occupation_set,
        occupations_requested,
        occupations_successful,
        refresh_status,
        error_count
    FROM data_refresh_log 
    ORDER BY refresh_date DESC 
    LIMIT 1;
END //

-- Procedure to clean old data (keep last 5 years)
CREATE PROCEDURE CleanOldData()
BEGIN
    DECLARE current_year YEAR DEFAULT YEAR(CURDATE());
    DECLARE cutoff_year YEAR DEFAULT current_year - 5;
    
    DELETE FROM scm_salary_data 
    WHERE data_year < cutoff_year;
    
    DELETE FROM data_refresh_log 
    WHERE data_year < cutoff_year;
END //

DELIMITER ;

-- Create indexes for better performance on large datasets
CREATE INDEX idx_salary_year_available ON scm_salary_data(data_year, data_available);
CREATE INDEX idx_salary_code_year ON scm_salary_data(occupation_code, data_year);

-- Insert a sample refresh log entry
INSERT INTO data_refresh_log (
    data_year, 
    occupation_set, 
    occupations_requested, 
    occupations_successful, 
    api_calls_made,
    refresh_status
) VALUES (
    2024, 
    'core', 
    14, 
    0, 
    0,
    'pending'
);

-- Display setup completion message
SELECT 'Database setup completed successfully!' as Status,
       COUNT(*) as Total_Occupations_Defined
FROM occupation_definitions;

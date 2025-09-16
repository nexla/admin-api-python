-- MySQL initialization for Python-only stack
-- This sets up the database for pure Python/FastAPI usage

-- Ensure proper character sets
ALTER DATABASE nexla_admin_dev CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Create Python-specific database user
CREATE USER IF NOT EXISTS 'python_app'@'%' IDENTIFIED BY 'python123';
GRANT ALL PRIVILEGES ON nexla_admin_dev.* TO 'python_app'@'%';

-- Create separate test database
CREATE DATABASE IF NOT EXISTS nexla_admin_test;
ALTER DATABASE nexla_admin_test CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
GRANT ALL PRIVILEGES ON nexla_admin_test.* TO 'python_app'@'%';
GRANT ALL PRIVILEGES ON nexla_admin_test.* TO 'root'@'%';

-- Set up session settings for Python compatibility
SET GLOBAL time_zone = '+00:00';
SET GLOBAL sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- Optimize for Python applications
SET GLOBAL max_connections = 200;
SET GLOBAL innodb_buffer_pool_size = 1073741824;  -- 1GB

-- Enable general log for development
SET GLOBAL general_log = 'ON';
SET GLOBAL general_log_file = '/var/lib/mysql/python-general.log';

-- Flush privileges
FLUSH PRIVILEGES;

-- Show created databases and users
SHOW DATABASES;
SELECT User, Host FROM mysql.user WHERE User IN ('python_app', 'root');
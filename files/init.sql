GRANT ALL PRIVILEGES ON airflow_db.* TO 'airflow_user'@'%';
CREATE DATABASE IF NOT EXISTS sales_db;
CREATE USER IF NOT EXISTS 'sales_user'@'%' IDENTIFIED BY 'sales_password';
GRANT ALL PRIVILEGES ON sales_db.* TO 'sales_user'@'%';

FLUSH PRIVILEGES;

CREATE DATABASE IF NOT EXISTS airflow_db;
CREATE USER IF NOT EXISTS 'airflow_user'@'%' IDENTIFIED BY 'airflow_password';
GRANT ALL PRIVILEGES ON airflow_db.* TO 'airflow_user'@'%';

CREATE DATABASE IF NOT EXISTS sales_db;
GRANT ALL PRIVILEGES ON sales_db.* TO 'sales_user'@'%';

FLUSH PRIVILEGES;

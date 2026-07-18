#!/bin/bash
set -e
set -u

function create_user_and_database() {
    local database=$1
    local username=$2
    local password=$3
    echo "Creating user '$username' and database '$database'"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
        CREATE USER $username WITH PASSWORD '$password';
        CREATE DATABASE $database OWNER $username;
        GRANT ALL PRIVILEGES ON DATABASE $database TO $username;
EOSQL
    echo "  User '$username' and database '$database' created successfully"
}

# Create Mart database with mart user
create_user_and_database $MART_DB $MART_DB_USERNAME $MART_DB_PASSWORD

# Create Airflow database with airflow user
create_user_and_database $AIRFLOW_DB $AIRFLOW_DB_USERNAME $AIRFLOW_DB_PASSWORD

echo "All databases and users created successfully"

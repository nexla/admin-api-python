#!/bin/bash

# Wait for database to be ready

set -e

host="$DB_HOST"
port="$DB_PORT"
user="$DB_USER"
database="$DB_NAME"

echo "Waiting for MySQL at $host:$port..."

until mysql -h "$host" -P "$port" -u "$user" -p"$DB_PASSWORD" -e "SELECT 1" "$database" > /dev/null 2>&1; do
  >&2 echo "MySQL is unavailable - sleeping"
  sleep 1
done

>&2 echo "MySQL is up - executing command"
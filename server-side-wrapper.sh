#!/bin/bash -x

export MYSQL_PWD=password #limits exposure of password on the system

USER="user_name"
LOGPATH="path_to_your_log_files" #usually default to /var/log/mysql
DATABASE="database_name"
DATE="$(date -I | cut -c 3-)"
SCHEMA_DUMP="${DATE}-dump.sql"


mysqldump -u "$USER" --no-data --skip-dump-date --skip-comments --force --log-error="$LOGPATH"/dump_failure.log "$DATABASE" > "$SCHEMA_DUMP"

python3 s3-schema-version-check.py


# infra/postgres/Dockerfile
FROM postgres:15

# Add any PostgreSQL extensions or custom scripts here
# COPY will place scripts into /docker-entrypoint-initdb.d/
COPY initdb/*.sql /docker-entrypoint-initdb.d/

# Expose the PostgreSQL port
EXPOSE 5432
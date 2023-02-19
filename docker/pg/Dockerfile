# Use the official PostgreSQL image from Docker Hub
FROM postgres:latest

# Set the environment variables for the default PostgreSQL user and database
ENV POSTGRES_HOST=usajobs_db
ENV POSTGRES_USER=tasman_user
ENV POSTGRES_PASSWORD=tasman_senior_data_engineer
ENV POSTGRES_DB=tasman_db

# Copy a SQL script to initialize the database
COPY init.sql /docker-entrypoint-initdb.d/

# Mount a host directory to a volume inside the container to persist data
VOLUME /var/lib/postgresql/data

# Endpoint where the database will be exposed
EXPOSE 5432
-- bootstrap_db_and_users_fixed.sql

-- Reload configuration
ALTER SYSTEM SET log_statement = 'ddl';
SELECT pg_reload_conf();

-- -------------------------------
-- Create database if it doesn't exist
-- -------------------------------
DO $$
DECLARE
    db_name text := :'DB_NAME';
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = db_name) THEN
        EXECUTE format('CREATE DATABASE %I ENCODING ''UTF8''', db_name);
    END IF;
END
$$;

-- Set database timezone (needs dynamic SQL)
DO $$
DECLARE
    db_name text := :'DB_NAME';
BEGIN
    EXECUTE format('ALTER DATABASE %I SET timezone TO ''UTC''', db_name);
END
$$;

-- -------------------------------
-- Create roles if they don't exist
-- -------------------------------
DO $$
DECLARE
    admin_user text := :'ADMIN_USER';
    admin_pass text := :'ADMIN_PASS';
    scraper_user text := :'SCRAPER_USER';
    scraper_pass text := :'SCRAPER_PASS';
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = admin_user) THEN
        EXECUTE format('CREATE ROLE %I WITH LOGIN PASSWORD %L', admin_user, admin_pass);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = scraper_user) THEN
        EXECUTE format('CREATE ROLE %I WITH LOGIN PASSWORD %L', scraper_user, scraper_pass);
    END IF;
END
$$;

-- -------------------------------
-- Set database ownership
-- -------------------------------
DO $$
DECLARE
    db_name text := :'DB_NAME';
    admin_user text := :'ADMIN_USER';
BEGIN
    EXECUTE format('ALTER DATABASE %I OWNER TO %I', db_name, admin_user);
END
$$;

-- -------------------------------
-- Connect to the new database
-- -------------------------------
\connect :'DB_NAME'

-- -------------------------------
-- Create schema if it doesn't exist (owned by scraper_user)
-- -------------------------------
DO $$
DECLARE
    schema_name text := :'SCRAPER_SCHEMA';
    scraper_user text := :'SCRAPER_USER';
BEGIN
    EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I AUTHORIZATION %I', schema_name, scraper_user);
END
$$;

-- Grant privileges
DO $$
DECLARE
    schema_name text := :'SCRAPER_SCHEMA';
    admin_user text := :'ADMIN_USER';
    scraper_user text := :'SCRAPER_USER';
BEGIN
    -- Admin gets full privileges even though they don't own the schema
    EXECUTE format('GRANT ALL PRIVILEGES ON SCHEMA %I TO %I', schema_name, admin_user);
    -- Scraper user already owns the schema, but grant explicit usage and create for clarity
    EXECUTE format('GRANT USAGE, CREATE ON SCHEMA %I TO %I', schema_name, scraper_user);
END
$$;

-- Set default privileges for tables created in schema
DO $$
DECLARE
    schema_name text := :'SCRAPER_SCHEMA';
    scraper_user text := :'SCRAPER_USER';
    admin_user text := :'ADMIN_USER';
BEGIN
    -- Default privileges: tables created by scraper_user allow admin_user access
    EXECUTE format(
        'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA %I GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %I',
        scraper_user, schema_name, admin_user
    );
END
$$;
-- Limit connections for scraper_user
DO $$
DECLARE
    scraper_user text := :'SCRAPER_USER';
BEGIN
    EXECUTE format('ALTER ROLE %I CONNECTION LIMIT 5', scraper_user);
END
$$;

-- List schemas and roles (optional)
\dn+
\du

-- bootstrap_db_and_users_fixed.sql

-- 1. SET SESSION VARIABLES
-- We use the 'vars.' prefix for custom parameters so the PostgreSQL server recognizes them.
-- The psql client substitutes :'VAR' here, and the server saves the value as a setting.
SET SESSION "vars.DB_NAME" TO :'DB_NAME';
SET SESSION "vars.TEST_DB_NAME" TO :'TEST_DB_NAME';
SET SESSION "vars.ADMIN_USER" TO :'ADMIN_USER';
SET SESSION "vars.ADMIN_PASS" TO :'ADMIN_PASS';
SET SESSION "vars.SCRAPER_USER" TO :'SCRAPER_USER';
SET SESSION "vars.SCRAPER_PASS" TO :'SCRAPER_PASS';
SET SESSION "vars.TEST_DB_USER" TO :'TEST_DB_USER';
SET SESSION "vars.TEST_DB_PASS" TO :'TEST_DB_PASS';
SET SESSION "vars.SCRAPER_SCHEMA" TO :'SCRAPER_SCHEMA';

-- Optional: Enable logging for DDL
ALTER SYSTEM SET log_statement = 'ddl';
SELECT pg_reload_conf();

---

-- 2. CREATE DATABASES (if they don't exist)
SELECT format(
    'CREATE DATABASE %I ENCODING ''UTF8''',
    current_setting('vars.DB_NAME')
)
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = current_setting('vars.DB_NAME')
)
\gexec


-- Create test database if it does not exist
SELECT format(
    'CREATE DATABASE %I ENCODING ''UTF8''',
    current_setting('vars.TEST_DB_NAME')
)
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = current_setting('vars.TEST_DB_NAME')
)
\gexec

-- Set database timezone
DO $$
DECLARE
    db_name text := current_setting('vars.DB_NAME');
BEGIN
    EXECUTE format('ALTER DATABASE %I SET timezone TO ''UTC''', db_name);
END
$$;

---

-- 3. CREATE/UPDATE ROLES (with password and login)
DO $$
DECLARE
    -- Access variables
    admin_user text := current_setting('vars.ADMIN_USER');
    admin_pass text := current_setting('vars.ADMIN_PASS');
    scraper_user text := current_setting('vars.SCRAPER_USER');
    scraper_pass text := current_setting('vars.SCRAPER_PASS');
    test_user text := current_setting('vars.TEST_DB_USER');
    test_pass text := current_setting('vars.TEST_DB_PASS');
BEGIN
    -- ADMIN USER (SUPERUSER)
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = admin_user) THEN
        EXECUTE format('CREATE ROLE %I WITH LOGIN SUPERUSER PASSWORD %L', admin_user, admin_pass);
    ELSE
        EXECUTE format('ALTER ROLE %I WITH LOGIN SUPERUSER PASSWORD %L', admin_user, admin_pass);
    END IF;

    -- SCRAPER USER (NOSUPERUSER)
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = scraper_user) THEN
        EXECUTE format('CREATE ROLE %I WITH LOGIN NOSUPERUSER PASSWORD %L', scraper_user, scraper_pass);
    ELSE
        -- Use ALTER ROLE to guarantee LOGIN and update the password
        EXECUTE format('ALTER ROLE %I WITH LOGIN NOSUPERUSER PASSWORD %L', scraper_user, scraper_pass);
    END IF;

    -- TEST DB USER (NOSUPERUSER)
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = test_user) THEN
        EXECUTE format('CREATE ROLE %I WITH LOGIN NOSUPERUSER PASSWORD %L', test_user, test_pass);
    ELSE
        EXECUTE format('ALTER ROLE %I WITH LOGIN NOSUPERUSER PASSWORD %L', test_user, test_pass);
    END IF;
END
$$;

---

-- 4. SET OWNERSHIP AND CONNECT

-- Set database ownership
DO $$
DECLARE
    db_name text := current_setting('vars.DB_NAME');
    admin_user text := current_setting('vars.ADMIN_USER');
    test_db_name text := current_setting('vars.TEST_DB_NAME');
    test_user text := current_setting('vars.TEST_DB_USER');
BEGIN
    EXECUTE format('ALTER DATABASE %I OWNER TO %I', db_name, admin_user);
    EXECUTE format('ALTER DATABASE %I OWNER TO %I', test_db_name, test_user);
END
$$;

-- Connect to the main database to create schemas/privileges
\connect :DB_NAME

-- Re-set variables
SET SESSION "vars.SCRAPER_SCHEMA" TO :'SCRAPER_SCHEMA';
SET SESSION "vars.SCRAPER_USER" TO :'SCRAPER_USER';
SET SESSION "vars.ADMIN_USER" TO :'ADMIN_USER';

-- 5. CREATE SCHEMA (and ownership)
DO $$
DECLARE
    schema_name text := current_setting('vars.SCRAPER_SCHEMA');
    scraper_user text := current_setting('vars.SCRAPER_USER');
BEGIN
    EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I AUTHORIZATION %I', schema_name, scraper_user);
END
$$;

-- 6. GRANT PRIVILEGES

-- Grant all privileges to the admin user
DO $$
DECLARE
    schema_name text := current_setting('vars.SCRAPER_SCHEMA');
    admin_user text := current_setting('vars.ADMIN_USER');
BEGIN
    EXECUTE format('GRANT ALL PRIVILEGES ON SCHEMA %I TO %I', schema_name, admin_user);
END
$$;

-- Set default privileges for tables created in schema
DO $$
DECLARE
    schema_name text := current_setting('vars.SCRAPER_SCHEMA');
    scraper_user text := current_setting('vars.SCRAPER_USER');
    admin_user text := current_setting('vars.ADMIN_USER');
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
    scraper_user text := current_setting('vars.SCRAPER_USER');
BEGIN
    EXECUTE format('ALTER ROLE %I CONNECTION LIMIT 5', scraper_user);
END
$$;

-- List schemas and roles (optional)
\dn+
\du
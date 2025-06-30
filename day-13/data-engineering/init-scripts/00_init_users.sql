-- init-scripts/00_init_users.sql
-- This file runs first (00_) to set up users and permissions

-- The dw_admin user is already created by the environment variables
-- but let's ensure proper permissions

-- Grant all privileges on the database to dw_admin
GRANT ALL PRIVILEGES ON DATABASE retail_dw TO dw_admin;

-- Grant usage on schemas
GRANT USAGE ON SCHEMA public TO dw_admin;
GRANT CREATE ON SCHEMA public TO dw_admin;

-- The user will be able to create schemas as needed
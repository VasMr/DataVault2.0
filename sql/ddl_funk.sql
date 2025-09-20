-- ========================================================
--                     EXTENSION
-- ========================================================
CREATE EXTENSION IF NOT EXISTS pgcrypto;


-- ========================================================
--                     SCHEMAS
-- ========================================================

-- STG
CREATE SCHEMA IF NOT EXISTS stg;

-- DDS
CREATE SCHEMA IF NOT EXISTS dds;


-- ========================================================
--                     STG: raw emails
-- ========================================================

-- stg: raw_json
DROP TABLE IF EXISTS stg.raw_json;

CREATE TABLE IF NOT EXISTS stg.raw_json (
    id SERIAL4 PRIMARY KEY,
    "json" JSON NULL,
    load_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ========================================================
--                     Hubs (DDS)
-- ========================================================

-- Hub: User
DROP TABLE IF EXISTS dds.hub_user;

CREATE TABLE dds.hub_user (
    user_id INT NOT NULL,
    user_hk VARCHAR(64) NOT NULL,
    source_system TEXT NOT NULL DEFAULT 'jsonplaceholder',
    load_dttm TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (user_hk)
);

-- Hub: Email
DROP TABLE IF EXISTS dds.hub_email;

CREATE TABLE dds.hub_email (
    email_id INT NOT NULL,
    email_hk VARCHAR(64) NOT NULL,
    source_system TEXT NOT NULL DEFAULT 'jsonplaceholder',
    load_dttm TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (email_hk)
);


-- ========================================================
--                   Links (DDS)
-- ========================================================

-- Link: User - Email
DROP TABLE IF EXISTS dds.link_user_email;

CREATE TABLE dds.link_user_email (
    user_hk VARCHAR(64) NOT NULL,
    email_hk VARCHAR(64) NOT NULL,
    load_dttm TIMESTAMPTZ DEFAULT now(),
    source_system TEXT NOT NULL DEFAULT 'jsonplaceholder',
    PRIMARY KEY (user_hk, email_hk)
);


-- ========================================================
--                   Satellites (DDS)
-- ========================================================


-- Satellite: Email Meta
DROP TABLE IF EXISTS dds.sat_email_meta;

CREATE TABLE dds.sat_email_meta (
    email_hk VARCHAR(64) NOT NULL,
    title TEXT,
    body TEXT,
    hashdiff VARCHAR(64) NOT NULL,
    load_dttm TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (email_hk, load_dttm)
);

-- ========================================================
--  DDS: loader functions (PL/pgSQL)
-- ========================================================


-- 1) load_hub_user
CREATE OR REPLACE FUNCTION dds.load_hub_user()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO dds.hub_user (user_id, user_hk, source_system, load_dttm)
    SELECT DISTINCT
        (r.json->>'userId')::int AS user_id,
        encode(digest('user|' || ((r.json->>'userId')::text), 'sha256'), 'hex') AS user_hk,
        'jsonplaceholder'::text AS source_system,
        now() AT TIME ZONE 'Europe/Moscow'
    FROM stg.raw_json r
    WHERE r.json->>'userId' IS NOT NULL
    ON CONFLICT (user_hk) DO NOTHING;
END;
$$;

-- 2) load_hub_email
CREATE OR REPLACE FUNCTION dds.load_hub_email()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO dds.hub_email (email_id, email_hk, source_system, load_dttm)
    SELECT DISTINCT
        (r.json->>'id')::int AS email_id,
        encode(digest('email|' || ((r.json->>'id')::text), 'sha256'), 'hex') AS email_hk,
        'jsonplaceholder'::text AS source_system,
        now() AT TIME ZONE 'Europe/Moscow'
    FROM stg.raw_json r
    WHERE r.json->>'id' IS NOT NULL
    ON CONFLICT (email_hk) DO NOTHING;
END;
$$;

-- 3) load_link_user_email
CREATE OR REPLACE FUNCTION dds.load_link_user_email()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO dds.link_user_email (user_hk, email_hk, load_dttm, source_system)
    SELECT DISTINCT
        encode(digest('user|'  || ((r.json->>'userId')::text), 'sha256'), 'hex') AS user_hk,
        encode(digest('email|' || ((r.json->>'id')::text),     'sha256'), 'hex') AS email_hk,
        now() AT TIME ZONE 'Europe/Moscow' AS load_dttm,
        'jsonplaceholder'::text AS source_system
    FROM stg.raw_json r
    WHERE r.json->>'userId' IS NOT NULL
      AND r.json->>'id' IS NOT NULL
    ON CONFLICT (user_hk, email_hk) DO NOTHING;
END;
$$;

-- 4) load_sat_email_meta
-- hashdiff check for email_hk
CREATE OR REPLACE FUNCTION dds.load_sat_email_meta()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    WITH src AS (
        SELECT
            encode(digest('email|' || ((r.json->>'id')::text), 'sha256'), 'hex') AS email_hk,
            r.json->>'title' AS title,
            r.json->>'body'  AS body,
            encode(digest(
                coalesce(r.json->>'title','') || '|' || coalesce(r.json->>'body',''),
                'sha256'
            ), 'hex') AS hashdiff
        FROM stg.raw_json r
        WHERE r.json->>'id' IS NOT NULL
    )
    INSERT INTO dds.sat_email_meta (email_hk, title, body, hashdiff, load_dttm)
    SELECT s.email_hk, s.title, s.body, s.hashdiff, now() AT TIME ZONE 'Europe/Moscow'
    FROM src s
    LEFT JOIN dds.sat_email_meta existing
        ON existing.email_hk = s.email_hk AND existing.hashdiff = s.hashdiff
    WHERE existing.email_hk IS NULL;
END;
$$;


-- 5) wrapper func, all load
CREATE OR REPLACE FUNCTION dds.load_all_dds()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM dds.load_hub_user();
    PERFORM dds.load_hub_email();
    PERFORM dds.load_link_user_email();
    PERFORM dds.load_sat_email_meta();
END;
$$;

select dds.load_all_dds();

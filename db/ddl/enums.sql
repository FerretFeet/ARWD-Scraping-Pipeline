

SET search_path TO :"SCRAPER_SCHEMA";

DO $$ BEGIN CREATE TYPE chamber AS ENUM ('senate', 'house'); EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE TYPE sponsor_type AS ENUM ('lead_sponsor', 'other_primary_sponsor', 'cosponsor'); EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE TYPE vote_type AS ENUM ('yea', 'nay', 'non_voting', 'present', 'excused'); EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE TYPE doc_type AS ENUM ('bill_text', 'act_text', 'amendment', 'fiscal_note', 'other'); EXCEPTION WHEN duplicate_object THEN NULL; END $$;


-- Table: public.data_catalog

-- DROP TABLE IF EXISTS public.data_catalog;

CREATE TABLE IF NOT EXISTS public.data_catalog
(
    "MERGE Category" text COLLATE pg_catalog."default",
    "Data Source" text COLLATE pg_catalog."default",
    "Original Variable Name" text COLLATE pg_catalog."default",
    "Modified Variable Name/Label" text COLLATE pg_catalog."default",
    "Unit" text COLLATE pg_catalog."default",
    "MERGE Pre-Calculation" text COLLATE pg_catalog."default",
    "Description" text COLLATE pg_catalog."default",
    "Selected Value (Areal Calculation)" text COLLATE pg_catalog."default",
    "Temperal Coverage" text COLLATE pg_catalog."default",
    "Temperal Resolution" text COLLATE pg_catalog."default",
    "Geo Coverage" text COLLATE pg_catalog."default",
    "Original DB Table Name" text COLLATE pg_catalog."default",
    "Original Data Source" text COLLATE pg_catalog."default"
)

TABLESPACE pg_default;
-- View: public.mv_geospatial_gleam

-- DROP MATERIALIZED VIEW IF EXISTS public.mv_geospatial_gleam;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_geospatial_gleam
TABLESPACE pg_default
AS
 SELECT gid,
    admin_level,
    date,
    max(
        CASE
            WHEN variable = 'SMrz'::text THEN mean
            ELSE NULL::numeric
        END) AS "Root-zone Soil Moisture"
   FROM geospatial_data_gleam
  GROUP BY gid, admin_level, date
WITH DATA;


CREATE INDEX idx_mv_geospatial_gleam_gid_admin_level_date
    ON public.mv_geospatial_gleam USING btree
    (gid COLLATE pg_catalog."default", admin_level, date)
    TABLESPACE pg_default;
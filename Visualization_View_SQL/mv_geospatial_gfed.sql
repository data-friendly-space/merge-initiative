-- View: public.mv_geospatial_gfed

-- DROP MATERIALIZED VIEW IF EXISTS public.mv_geospatial_gfed;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_geospatial_gfed
TABLESPACE pg_default
AS
 SELECT gid,
    admin_level,
    date,
    max(
        CASE
            WHEN variable = 'Monthly Burnt Area (Total)'::text THEN mean
            ELSE NULL::numeric
        END) AS "Monthly Total Burnt Area"
   FROM geospatial_data_gfed
  GROUP BY gid, admin_level, date
WITH DATA;



CREATE INDEX idx_mv_geospatial_gfed_gid_admin_level_date
    ON public.mv_geospatial_gfed USING btree
    (gid COLLATE pg_catalog."default", admin_level, date)
    TABLESPACE pg_default;
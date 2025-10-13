-- View: public.mv_geospatial_worldpop

-- DROP MATERIALIZED VIEW IF EXISTS public.mv_geospatial_worldpop;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_geospatial_worldpop
TABLESPACE pg_default
AS
 SELECT gid,
    admin_level,
    date,
    max(
        CASE
            WHEN variable = 'population_count'::text THEN sum
            ELSE NULL::numeric
        END) AS "WorldPop - Population Count",
    max(
        CASE
            WHEN variable = 'population_density'::text THEN mean
            ELSE NULL::numeric
        END) AS "WorldPop - Population Density"
   FROM geospatial_data_worldpop
  GROUP BY gid, admin_level, date
WITH DATA;


CREATE INDEX idx_mv_geospatial_worldpop_gid_admin_level_date
    ON public.mv_geospatial_worldpop USING btree
    (gid COLLATE pg_catalog."default", admin_level, date)
    TABLESPACE pg_default;
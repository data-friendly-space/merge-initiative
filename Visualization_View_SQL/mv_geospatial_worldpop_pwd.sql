-- View: public.mv_geospatial_worldpop_pwd

-- DROP MATERIALIZED VIEW IF EXISTS public.mv_geospatial_worldpop_pwd;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_geospatial_worldpop_pwd
TABLESPACE pg_default
AS
 SELECT gid,
    admin_level,
    date,
    max(
        CASE
            WHEN variable::text = 'Population_Weighted_Density_G'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "WorldPop - Population Weighted Density - Geometric Mean"
   FROM geospatial_data_worldpop_pwd
  GROUP BY gid, admin_level, date
WITH DATA;


CREATE INDEX idx_mv_geospatial_worldpop_pwd_gid_admin_level_date
    ON public.mv_geospatial_worldpop_pwd USING btree
    (gid COLLATE pg_catalog."default", admin_level, date)
    TABLESPACE pg_default;
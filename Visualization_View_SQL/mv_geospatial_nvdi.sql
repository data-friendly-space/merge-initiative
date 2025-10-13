-- View: public.mv_geospatial_nvdi

-- DROP MATERIALIZED VIEW IF EXISTS public.mv_geospatial_nvdi;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_geospatial_nvdi
TABLESPACE pg_default
AS
 SELECT gid,
    admin_level,
    date,
    max(
        CASE
            WHEN variable = 'NDVI'::text THEN mean
            ELSE NULL::numeric
        END) AS "Normalized Difference Vegetation Index"
   FROM geospatial_data_nvdi
  GROUP BY gid, admin_level, date
WITH DATA;


CREATE INDEX idx_mv_geospatial_nvdi_gid_admin_level_date
    ON public.mv_geospatial_nvdi USING btree
    (gid COLLATE pg_catalog."default", admin_level, date)
    TABLESPACE pg_default;
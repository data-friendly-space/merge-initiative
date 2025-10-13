-- View: public.mv_geospatial_merra2

-- DROP MATERIALIZED VIEW IF EXISTS public.mv_geospatial_merra2;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_geospatial_merra2
TABLESPACE pg_default
AS
 SELECT gid,
    admin_level,
    date,
    max(
        CASE
            WHEN variable = 'BCSMASS'::text THEN mean
            ELSE NULL::numeric
        END) AS "Daily Average Dust Surface Mass Concentration - PM 2.5",
    max(
        CASE
            WHEN variable = 'DUSMASS25'::text THEN mean
            ELSE NULL::numeric
        END) AS "Daily Average Organic Carbon Surface Mass Concentration",
    max(
        CASE
            WHEN variable = 'OCSMASS'::text THEN mean
            ELSE NULL::numeric
        END) AS "Daily Average Black Carbon Surface Mass Concentration"
   FROM geospatial_data_merra2
  GROUP BY gid, admin_level, date
WITH DATA;


CREATE INDEX idx_mv_geospatial_merra2_gid_admin_level_date
    ON public.mv_geospatial_merra2 USING btree
    (gid COLLATE pg_catalog."default", admin_level, date)
    TABLESPACE pg_default;
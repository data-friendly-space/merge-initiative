-- View: public.mv_geospatial_era5

-- DROP MATERIALIZED VIEW IF EXISTS public.mv_geospatial_era5;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_geospatial_era5
TABLESPACE pg_default
AS
 SELECT gid,
    admin_level,
    date,
    max(
        CASE
            WHEN variable = '2m_temperature'::text THEN mean
            ELSE NULL::numeric
        END) AS "Daily Average Temperature of Air (2m)",
    max(
        CASE
            WHEN variable = 'evaporation'::text THEN mean
            ELSE NULL::numeric
        END) AS "Daily Total Evaporation",
    max(
        CASE
            WHEN variable = 'maximum_2m_temperature_since_previous_post_processing'::text THEN mean
            ELSE NULL::numeric
        END) AS "Daily Maximun Temperature of Air (2m)",
    max(
        CASE
            WHEN variable = 'minimum_2m_temperature_since_previous_post_processing'::text THEN mean
            ELSE NULL::numeric
        END) AS "Daily Minimun Temperature of Air (2m)",
    max(
        CASE
            WHEN variable = 'surface_net_solar_radiation'::text THEN mean
            ELSE NULL::numeric
        END) AS "Daily Total Surface Net Solar Radiation",
    max(
        CASE
            WHEN variable = 'total_precipitation'::text THEN mean
            ELSE NULL::numeric
        END) AS "Daily Total Precipitation"
   FROM geospatial_data_era5
  GROUP BY gid, admin_level, date
WITH DATA;


CREATE INDEX idx_mv_geospatial_era5_gid_admin_level_date
    ON public.mv_geospatial_era5 USING btree
    (gid COLLATE pg_catalog."default", admin_level, date)
    TABLESPACE pg_default;
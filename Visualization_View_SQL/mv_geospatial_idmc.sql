-- View: public.mv_geospatial_idmc

-- DROP MATERIALIZED VIEW IF EXISTS public.mv_geospatial_idmc;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_geospatial_idmc
TABLESPACE pg_default
AS
 SELECT gid,
    admin_level,
    date,
    max(
        CASE
            WHEN variable::text = 'Conflict Internal Displacements'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Conflict Internal Displacements",
    max(
        CASE
            WHEN variable::text = 'Conflict Total Displacement'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Conflict Total Displacement",
    max(
        CASE
            WHEN variable::text = 'Disaster Internal Displacements'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Disaster Internal Displacements",
    max(
        CASE
            WHEN variable::text = 'Disaster Total Displacement'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Disaster Total Displacement",
    max(
        CASE
            WHEN variable::text = 'Conflict_Both sexes_0-4'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Conflict Displacements Both Sexes Age 0-4",
    max(
        CASE
            WHEN variable::text = 'Conflict_Both sexes_5-11'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Conflict Displacements Both Sexes Age 5-11",
    max(
        CASE
            WHEN variable::text = 'Conflict_Both sexes_12-17'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Conflict Displacements Both Sexes Age 12-17",
    max(
        CASE
            WHEN variable::text = 'Conflict_Both sexes_18-59'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Conflict Displacements Both Sexes Age 18-59",
    max(
        CASE
            WHEN variable::text = 'Conflict_Both sexes_60+'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Conflict Displacements Both Sexes Age 60+",
    max(
        CASE
            WHEN variable::text = 'Conflict_Female_0-4'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Conflict Displacements Female Age 0-4",
    max(
        CASE
            WHEN variable::text = 'Conflict_Female_5-11'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Conflict Displacements Female Age 5-11",
    max(
        CASE
            WHEN variable::text = 'Conflict_Female_12-17'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Conflict Displacements Female Age 12-17",
    max(
        CASE
            WHEN variable::text = 'Conflict_Female_18-59'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Conflict Displacements Female Age 18-59",
    max(
        CASE
            WHEN variable::text = 'Conflict_Female_60+'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Conflict Displacements Female Age 60+",
    max(
        CASE
            WHEN variable::text = 'Conflict_Male_0-4'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Conflict Displacements Male Age 0-4",
    max(
        CASE
            WHEN variable::text = 'Conflict_Male_5-11'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Conflict Displacements Male Age 5-11",
    max(
        CASE
            WHEN variable::text = 'Conflict_Male_12-17'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Conflict Displacements Male Age 12-17",
    max(
        CASE
            WHEN variable::text = 'Conflict_Male_18-59'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Conflict Displacements Male Age 18-59",
    max(
        CASE
            WHEN variable::text = 'Conflict_Male_60+'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Conflict Displacements Male 60+",
    max(
        CASE
            WHEN variable::text = 'Disaster_Both sexes_0-4'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Disaster Displacements Both Sexes 0-4",
    max(
        CASE
            WHEN variable::text = 'Disaster_Both sexes_5-11'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Disaster Displacements Both Sexes 5-11",
    max(
        CASE
            WHEN variable::text = 'Disaster_Both sexes_12-17'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Disaster Displacements Both Sexes 12-17",
    max(
        CASE
            WHEN variable::text = 'Disaster_Both sexes_18-59'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Disaster Displacements Both Sexes 18-59",
    max(
        CASE
            WHEN variable::text = 'Disaster_Both sexes_60+'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Disaster Displacements Both Sexes 60+",
    max(
        CASE
            WHEN variable::text = 'Disaster_Female_0-4'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Disaster Displacements Female 0-4",
    max(
        CASE
            WHEN variable::text = 'Disaster_Female_5-11'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Disaster Displacements Female 5-11",
    max(
        CASE
            WHEN variable::text = 'Disaster_Female_12-17'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Disaster Displacements Female 12-17",
    max(
        CASE
            WHEN variable::text = 'Disaster_Female_18-59'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Disaster Displacements Female 18-59",
    max(
        CASE
            WHEN variable::text = 'Disaster_Female_60+'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Disaster Displacements Female 60+",
    max(
        CASE
            WHEN variable::text = 'Disaster_Male_0-4'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Disaster Displacements Male 0-4",
    max(
        CASE
            WHEN variable::text = 'Disaster_Male_5-11'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Disaster Displacements Male 5-11",
    max(
        CASE
            WHEN variable::text = 'Disaster_Male_12-17'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Disaster Displacements Male 12-17",
    max(
        CASE
            WHEN variable::text = 'Disaster_Male_18-59'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Disaster Displacements Male 18-59",
    max(
        CASE
            WHEN variable::text = 'Disaster_Male_60+'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Disaster Displacements Male 60+"
   FROM geospatial_data_idmc
  GROUP BY gid, admin_level, date
WITH DATA;


CREATE INDEX idx_mv_geospatial_idmc_gid_admin_level_date
    ON public.mv_geospatial_idmc USING btree
    (gid COLLATE pg_catalog."default", admin_level, date)
    TABLESPACE pg_default;
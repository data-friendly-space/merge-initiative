-- View: public.geospatial_viz

-- DROP MATERIALIZED VIEW IF EXISTS public.geospatial_viz;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.geospatial_viz
TABLESPACE pg_default
AS
 SELECT subquery."ISO3 (GADM - GID)",
    subquery."Admin Level",
    subquery."Date",
    COALESCE(dc."Modified Variable Name/Label", subquery."Variable") AS "Variable",
    subquery."Sum",
    subquery."Mean",
    subquery."Min",
    subquery."Max",
    subquery."Original Raw Value",
    subquery."Percentage of Area Missing Value",
    COALESCE(dc."Data Source", subquery."Source") AS "Source",
    subquery."Unit",
    subquery."Selected Calculation Value",
    SUBSTRING(subquery."ISO3 (GADM - GID)" FROM 1 FOR 3) AS "ISO3 - Admin Level 0",
        CASE
            WHEN subquery."Admin Level" = 1 THEN ga1.iso3_1
            ELSE NULL::character varying
        END AS "ISO3 - Admin Level 1"
   FROM ( SELECT all_data.gid AS "ISO3 (GADM - GID)",
            all_data.admin_level AS "Admin Level",
            all_data.date AS "Date",
            all_data.variable AS "Variable",
            all_data.sum AS "Sum",
            all_data.mean AS "Mean",
            all_data.min AS "Min",
            all_data.max AS "Max",
            all_data.raw_value AS "Original Raw Value",
            all_data.missing_value_percentage AS "Percentage of Area Missing Value",
            all_data.source AS "Source",
            all_data.unit AS "Unit",
            all_data.table_source AS "Table Source",
                CASE
                    WHEN all_data.table_source = 'geospatial_data_worldpop'::text AND all_data.variable = 'population_count'::text THEN all_data.sum
                    WHEN all_data.table_source = 'geospatial_data_worldpop'::text AND all_data.variable = 'population_density'::text THEN all_data.mean
                    WHEN all_data.table_source = 'geospatial_data_worldpop_age_sex'::text AND all_data.variable ~~* 'population_sex_age%'::text THEN all_data.sum
                    WHEN all_data.table_source = ANY (ARRAY['geospatial_data_gfed'::text, 'geospatial_data_nvdi'::text, 'geospatial_data_gleam'::text, 'geospatial_data_era5'::text, 'geospatial_data_merra2'::text]) THEN all_data.mean
                    ELSE all_data.raw_value
                END AS "Selected Calculation Value"
           FROM ( SELECT era5.gid,
                    era5.admin_level,
                    era5.date,
                    era5.variable,
                    era5.sum,
                    era5.mean,
                    era5.min,
                    era5.max,
                    era5.raw_value,
                    era5.missing_value_percentage,
                    era5.source,
                    era5.unit,
                    era5.table_source
                   FROM ( SELECT geospatial_data_era5.gid,
                            geospatial_data_era5.admin_level,
                            geospatial_data_era5.date,
                            geospatial_data_era5.variable,
                            geospatial_data_era5.sum,
                            geospatial_data_era5.mean,
                            geospatial_data_era5.min,
                            geospatial_data_era5.max,
                            geospatial_data_era5.raw_value,
                            COALESCE(geospatial_data_era5.missing_value_percentage, 0::numeric) AS missing_value_percentage,
                            geospatial_data_era5.source,
                            COALESCE(geospatial_data_era5.unit, 'N/A'::text) AS unit,
                            'geospatial_data_era5'::text AS table_source
                           FROM geospatial_data_era5) era5
                UNION ALL
                 SELECT gdl.gid,
                    gdl.admin_level,
                    gdl.date,
                    gdl.variable,
                    gdl.sum,
                    gdl.mean,
                    gdl.min,
                    gdl.max,
                    gdl.raw_value,
                    gdl.missing_value_percentage,
                    gdl.source,
                    gdl.unit,
                    gdl.table_source
                   FROM ( SELECT geospatial_data_gdl.gid,
                            geospatial_data_gdl.admin_level,
                            geospatial_data_gdl.date,
                            geospatial_data_gdl.variable,
                            geospatial_data_gdl.sum,
                            geospatial_data_gdl.mean,
                            geospatial_data_gdl.min,
                            geospatial_data_gdl.max,
                            geospatial_data_gdl.raw_value,
                            NULL::numeric AS missing_value_percentage,
                            geospatial_data_gdl.source,
                            NULL::text AS unit,
                            'geospatial_data_gdl'::text AS table_source
                           FROM geospatial_data_gdl) gdl
                UNION ALL
                 SELECT gfed.gid,
                    gfed.admin_level,
                    gfed.date,
                    gfed.variable,
                    gfed.sum,
                    gfed.mean,
                    gfed.min,
                    gfed.max,
                    gfed.raw_value,
                    gfed.missing_value_percentage,
                    gfed.source,
                    gfed.unit,
                    gfed.table_source
                   FROM ( SELECT geospatial_data_gfed.gid,
                            geospatial_data_gfed.admin_level,
                            geospatial_data_gfed.date,
                            geospatial_data_gfed.variable,
                            geospatial_data_gfed.sum,
                            geospatial_data_gfed.mean,
                            geospatial_data_gfed.min,
                            geospatial_data_gfed.max,
                            geospatial_data_gfed.raw_value,
                            COALESCE(geospatial_data_gfed.missing_value_percentage, 0::numeric) AS missing_value_percentage,
                            geospatial_data_gfed.source,
                            COALESCE(geospatial_data_gfed.unit, 'N/A'::text) AS unit,
                            'geospatial_data_gfed'::text AS table_source
                           FROM geospatial_data_gfed) gfed
                UNION ALL
                 SELECT gleam.gid,
                    gleam.admin_level,
                    gleam.date,
                    gleam.variable,
                    gleam.sum,
                    gleam.mean,
                    gleam.min,
                    gleam.max,
                    gleam.raw_value,
                    gleam.missing_value_percentage,
                    gleam.source,
                    gleam.unit,
                    gleam.table_source
                   FROM ( SELECT geospatial_data_gleam.gid,
                            geospatial_data_gleam.admin_level,
                            geospatial_data_gleam.date,
                            geospatial_data_gleam.variable,
                            geospatial_data_gleam.sum,
                            geospatial_data_gleam.mean,
                            geospatial_data_gleam.min,
                            geospatial_data_gleam.max,
                            geospatial_data_gleam.raw_value,
                            COALESCE(geospatial_data_gleam.missing_value_percentage, 0::numeric) AS missing_value_percentage,
                            geospatial_data_gleam.source,
                            COALESCE(geospatial_data_gleam.unit, 'N/A'::text) AS unit,
                            'geospatial_data_gleam'::text AS table_source
                           FROM geospatial_data_gleam) gleam
                UNION ALL
                 SELECT idmc.gid,
                    idmc.admin_level,
                    idmc.date,
                    idmc.variable,
                    idmc.sum,
                    idmc.mean,
                    idmc.min,
                    idmc.max,
                    idmc.raw_value,
                    idmc.missing_value_percentage,
                    idmc.source,
                    idmc.unit,
                    idmc.table_source
                   FROM ( SELECT geospatial_data_idmc.gid,
                            geospatial_data_idmc.admin_level,
                            geospatial_data_idmc.date,
                            geospatial_data_idmc.variable,
                            geospatial_data_idmc.sum,
                            geospatial_data_idmc.mean,
                            geospatial_data_idmc.min,
                            geospatial_data_idmc.max,
                            geospatial_data_idmc.raw_value,
                            NULL::numeric AS missing_value_percentage,
                            geospatial_data_idmc.source,
                            NULL::text AS unit,
                            'geospatial_data_idmc'::text AS table_source
                           FROM geospatial_data_idmc) idmc
                UNION ALL
                 SELECT landcover.gid,
                    landcover.admin_level,
                    landcover.date,
                    landcover.variable,
                    landcover.sum,
                    landcover.mean,
                    landcover.min,
                    landcover.max,
                    landcover.raw_value,
                    landcover.missing_value_percentage,
                    landcover.source,
                    landcover.unit,
                    landcover.table_source
                   FROM ( SELECT geospatial_data_landcover.gid,
                            geospatial_data_landcover.admin_level,
                            geospatial_data_landcover.date,
                            geospatial_data_landcover.variable,
                            geospatial_data_landcover.sum,
                            geospatial_data_landcover.mean,
                            geospatial_data_landcover.min,
                            geospatial_data_landcover.max,
                            geospatial_data_landcover.raw_value,
                            COALESCE(geospatial_data_landcover.missing_value_percentage, 0::numeric) AS missing_value_percentage,
                            geospatial_data_landcover.source,
                            COALESCE(geospatial_data_landcover.unit, 'N/A'::text) AS unit,
                            'geospatial_data_landcover'::text AS table_source
                           FROM geospatial_data_landcover) landcover
                UNION ALL
                 SELECT merra2.gid,
                    merra2.admin_level,
                    merra2.date,
                    merra2.variable,
                    merra2.sum,
                    merra2.mean,
                    merra2.min,
                    merra2.max,
                    merra2.raw_value,
                    merra2.missing_value_percentage,
                    merra2.source,
                    merra2.unit,
                    merra2.table_source
                   FROM ( SELECT geospatial_data_merra2.gid,
                            geospatial_data_merra2.admin_level,
                            geospatial_data_merra2.date,
                            geospatial_data_merra2.variable,
                            geospatial_data_merra2.sum,
                            geospatial_data_merra2.mean,
                            geospatial_data_merra2.min,
                            geospatial_data_merra2.max,
                            geospatial_data_merra2.raw_value,
                            COALESCE(geospatial_data_merra2.missing_value_percentage, 0::numeric) AS missing_value_percentage,
                            geospatial_data_merra2.source,
                            COALESCE(geospatial_data_merra2.unit, 'N/A'::text) AS unit,
                            'geospatial_data_merra2'::text AS table_source
                           FROM geospatial_data_merra2) merra2
                UNION ALL
                 SELECT nvdi.gid,
                    nvdi.admin_level,
                    nvdi.date,
                    nvdi.variable,
                    nvdi.sum,
                    nvdi.mean,
                    nvdi.min,
                    nvdi.max,
                    nvdi.raw_value,
                    nvdi.missing_value_percentage,
                    nvdi.source,
                    nvdi.unit,
                    nvdi.table_source
                   FROM ( SELECT geospatial_data_nvdi.gid,
                            geospatial_data_nvdi.admin_level,
                            geospatial_data_nvdi.date,
                            geospatial_data_nvdi.variable,
                            geospatial_data_nvdi.sum,
                            geospatial_data_nvdi.mean,
                            geospatial_data_nvdi.min,
                            geospatial_data_nvdi.max,
                            geospatial_data_nvdi.raw_value,
                            COALESCE(geospatial_data_nvdi.missing_value_percentage, 0::numeric) AS missing_value_percentage,
                            geospatial_data_nvdi.source,
                            COALESCE(geospatial_data_nvdi.unit, 'N/A'::text) AS unit,
                            'geospatial_data_nvdi'::text AS table_source
                           FROM geospatial_data_nvdi) nvdi
                UNION ALL
                 SELECT worldpop.gid,
                    worldpop.admin_level,
                    worldpop.date,
                    worldpop.variable,
                    worldpop.sum,
                    worldpop.mean,
                    worldpop.min,
                    worldpop.max,
                    worldpop.raw_value,
                    worldpop.missing_value_percentage,
                    worldpop.source,
                    worldpop.unit,
                    worldpop.table_source
                   FROM ( SELECT geospatial_data_worldpop.gid,
                            geospatial_data_worldpop.admin_level,
                            geospatial_data_worldpop.date,
                            geospatial_data_worldpop.variable,
                            geospatial_data_worldpop.sum,
                            geospatial_data_worldpop.mean,
                            geospatial_data_worldpop.min,
                            geospatial_data_worldpop.max,
                            geospatial_data_worldpop.raw_value,
                            COALESCE(geospatial_data_worldpop.missing_value_percentage, 0::numeric) AS missing_value_percentage,
                            geospatial_data_worldpop.source,
                            COALESCE(geospatial_data_worldpop.unit, 'N/A'::text) AS unit,
                            'geospatial_data_worldpop'::text AS table_source
                           FROM geospatial_data_worldpop) worldpop
                UNION ALL
                 SELECT worldpop_age_sex.gid,
                    worldpop_age_sex.admin_level,
                    worldpop_age_sex.date,
                    worldpop_age_sex.variable,
                    worldpop_age_sex.sum,
                    worldpop_age_sex.mean,
                    worldpop_age_sex.min,
                    worldpop_age_sex.max,
                    worldpop_age_sex.raw_value,
                    worldpop_age_sex.missing_value_percentage,
                    worldpop_age_sex.source,
                    worldpop_age_sex.unit,
                    worldpop_age_sex.table_source
                   FROM ( SELECT geospatial_data_worldpop_age_sex.gid,
                            geospatial_data_worldpop_age_sex.admin_level,
                            geospatial_data_worldpop_age_sex.date,
                            geospatial_data_worldpop_age_sex.variable,
                            geospatial_data_worldpop_age_sex.sum,
                            geospatial_data_worldpop_age_sex.mean,
                            geospatial_data_worldpop_age_sex.min,
                            geospatial_data_worldpop_age_sex.max,
                            geospatial_data_worldpop_age_sex.raw_value,
                            COALESCE(geospatial_data_worldpop_age_sex.missing_value_percentage, 0::numeric) AS missing_value_percentage,
                            geospatial_data_worldpop_age_sex.source,
                            COALESCE(geospatial_data_worldpop_age_sex.unit, 'N/A'::text) AS unit,
                            'geospatial_data_worldpop_age_sex'::text AS table_source
                           FROM geospatial_data_worldpop_age_sex) worldpop_age_sex
                UNION ALL
                 SELECT worldpop_pwd.gid,
                    worldpop_pwd.admin_level,
                    worldpop_pwd.date,
                    worldpop_pwd.variable,
                    worldpop_pwd.sum,
                    worldpop_pwd.mean,
                    worldpop_pwd.min,
                    worldpop_pwd.max,
                    worldpop_pwd.raw_value,
                    worldpop_pwd.missing_value_percentage,
                    worldpop_pwd.source,
                    worldpop_pwd.unit,
                    worldpop_pwd.table_source
                   FROM ( SELECT geospatial_data_worldpop_pwd.gid,
                            geospatial_data_worldpop_pwd.admin_level,
                            geospatial_data_worldpop_pwd.date,
                            geospatial_data_worldpop_pwd.variable,
                            geospatial_data_worldpop_pwd.sum,
                            geospatial_data_worldpop_pwd.mean,
                            geospatial_data_worldpop_pwd.min,
                            geospatial_data_worldpop_pwd.max,
                            geospatial_data_worldpop_pwd.raw_value,
                            NULL::numeric AS missing_value_percentage,
                            geospatial_data_worldpop_pwd.source,
                            NULL::text AS unit,
                            'geospatial_data_worldpop_pwd'::text AS table_source
                           FROM geospatial_data_worldpop_pwd) worldpop_pwd) all_data) subquery
     LEFT JOIN gadm_admin1 ga1 ON subquery."Admin Level" = 1 AND subquery."ISO3 (GADM - GID)"::text = ga1.gid_1::text
     LEFT JOIN data_catalog dc ON subquery."Variable" = dc."Original Variable Name"
WITH DATA;


CREATE INDEX idx_geospatial_viz_admin_level
    ON public.geospatial_viz USING btree
    ("Admin Level")
    TABLESPACE pg_default;
CREATE INDEX idx_geospatial_viz_date
    ON public.geospatial_viz USING btree
    ("Date")
    TABLESPACE pg_default;
CREATE INDEX idx_geospatial_viz_gid
    ON public.geospatial_viz USING btree
    ("ISO3 (GADM - GID)" COLLATE pg_catalog."default")
    TABLESPACE pg_default;
CREATE INDEX idx_geospatial_viz_iso3
    ON public.geospatial_viz USING btree
    ("ISO3 - Admin Level 0" COLLATE pg_catalog."default")
    TABLESPACE pg_default;
CREATE INDEX idx_geospatial_viz_iso3_1
    ON public.geospatial_viz USING btree
    ("ISO3 - Admin Level 1" COLLATE pg_catalog."default")
    TABLESPACE pg_default;
CREATE UNIQUE INDEX idx_geospatial_viz_mv_unique
    ON public.geospatial_viz USING btree
    ("ISO3 (GADM - GID)" COLLATE pg_catalog."default", "Admin Level", "Date", "Variable" COLLATE pg_catalog."default", "Source" COLLATE pg_catalog."default")
    TABLESPACE pg_default;
CREATE INDEX idx_geospatial_viz_source
    ON public.geospatial_viz USING btree
    ("Source" COLLATE pg_catalog."default")
    TABLESPACE pg_default;
CREATE INDEX idx_geospatial_viz_source_variable
    ON public.geospatial_viz USING btree
    ("Source" COLLATE pg_catalog."default", "Variable" COLLATE pg_catalog."default")
    TABLESPACE pg_default;
CREATE INDEX idx_geospatial_viz_unit
    ON public.geospatial_viz USING btree
    ("Unit" COLLATE pg_catalog."default")
    TABLESPACE pg_default;
CREATE INDEX idx_geospatial_viz_variable
    ON public.geospatial_viz USING btree
    ("Variable" COLLATE pg_catalog."default")
    TABLESPACE pg_default;
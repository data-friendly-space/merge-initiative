-- View: public.country_level_event_merged_geospatial_data_eav_viz

-- DROP MATERIALIZED VIEW IF EXISTS public.country_level_event_merged_geospatial_data_eav_viz;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.country_level_event_merged_geospatial_data_eav_viz
TABLESPACE pg_default
AS
 WITH events_data AS (
         SELECT events.start_date AS "Date",
            events.iso3_code AS "ISO3 (GADM - GID)",
            'EM-DAT & IDMC-GIDD - Event - Disaster Displacement data'::text AS "Source",
            'Disaster Types'::text AS "Category",
            events.disaster_type AS "Variable",
            count(*) AS "Selected Calculation Value",
            sum(events.total_deaths) AS "Total Deaths",
            sum(events.number_injured) AS "Number Injured",
            sum(events.number_affected) AS "Number Affected",
            sum(events.number_homeless) AS "Number Homeless",
            sum(events.total_affected) AS "Total Affected",
            sum(events.total_damage_adjusted) AS "Total Damage, Adjusted (USD)",
            sum(events.reconstruction_costs_adjusted) AS "Reconstruction Costs, Adjusted (USD)",
            sum(events.aid_contribution) AS "AID Contribution (USD)",
            sum(events.disaster_internal_displacements) AS "Event - Disaster Internal Displacements"
           FROM events
          GROUP BY events.start_date, events.iso3_code, events.disaster_type
        ), geospatial_data AS (
         SELECT gd.date AS "Date",
            gd.iso3 AS "ISO3 (GADM - GID)",
            COALESCE(dc."Data Source", gd.source) AS "Source",
            gd.category AS "Category",
            COALESCE(dc."Modified Variable Name/Label", gd.variable::text) AS "Variable",
            gd.selected_value AS "Selected Calculation Value",
            NULL::bigint AS "Total Deaths",
            NULL::bigint AS "Number Injured",
            NULL::bigint AS "Number Affected",
            NULL::bigint AS "Number Homeless",
            NULL::bigint AS "Total Affected",
            NULL::numeric AS "Total Damage, Adjusted (USD)",
            NULL::numeric AS "Reconstruction Costs, Adjusted (USD)",
            NULL::numeric AS "AID Contribution (USD)",
            NULL::bigint AS "Event - Disaster Internal Displacements"
           FROM ( SELECT combined_geospatial.date,
                    combined_geospatial.gid AS iso3,
                    combined_geospatial.source,
                    combined_geospatial.table_source,
                        CASE
                            WHEN combined_geospatial.table_source = 'geospatial_data_idmc'::text THEN 'Displacements'::text
                            WHEN combined_geospatial.table_source = 'geospatial_data_gdl'::text AND (combined_geospatial.variable::text = ANY (ARRAY['surfacetempyear'::character varying::text, 'relhumidityyear'::character varying::text, 'totprecipyear'::character varying::text])) THEN 'Environmental and Climate Factors'::text
                            WHEN combined_geospatial.table_source = 'geospatial_data_gdl'::text THEN 'Socio-demographic Indicators'::text
                            WHEN combined_geospatial.table_source = 'geospatial_data_worldpop'::text THEN 'Socio-demographic Indicators'::text
                            WHEN combined_geospatial.table_source = 'geospatial_data_worldpop_age_sex'::text THEN 'Socio-demographic Indicators'::text
                            WHEN combined_geospatial.table_source = 'geospatial_data_worldpop_pwd'::text THEN 'Socio-demographic Indicators'::text
                            ELSE 'Environmental and Climate Factors'::text
                        END AS category,
                    combined_geospatial.variable,
                        CASE
                            WHEN combined_geospatial.table_source = 'geospatial_data_worldpop'::text AND combined_geospatial.variable::text = 'population_count'::text THEN combined_geospatial.sum
                            WHEN combined_geospatial.table_source = 'geospatial_data_worldpop'::text AND combined_geospatial.variable::text = 'population_density'::text THEN combined_geospatial.mean
                            WHEN combined_geospatial.table_source = 'geospatial_data_worldpop_age_sex'::text AND combined_geospatial.variable::text ~~* 'population_sex_age%'::text THEN combined_geospatial.sum
                            WHEN combined_geospatial.table_source = ANY (ARRAY['geospatial_data_gfed'::text, 'geospatial_data_nvdi'::text, 'geospatial_data_gleam'::text, 'geospatial_data_era5'::text, 'geospatial_data_merra2'::text]) THEN combined_geospatial.mean
                            ELSE combined_geospatial.raw_value
                        END AS selected_value
                   FROM ( SELECT geospatial_data_gdl.date,
                            geospatial_data_gdl.gid,
                            geospatial_data_gdl.source,
                            'geospatial_data_gdl'::text AS table_source,
                            geospatial_data_gdl.variable,
                            NULL::numeric AS mean,
                            NULL::numeric AS sum,
                            geospatial_data_gdl.raw_value
                           FROM geospatial_data_gdl
                          WHERE geospatial_data_gdl.admin_level = 0
                        UNION ALL
                         SELECT geospatial_data_worldpop.date,
                            geospatial_data_worldpop.gid,
                            geospatial_data_worldpop.source,
                            'geospatial_data_worldpop'::text AS table_source,
                            geospatial_data_worldpop.variable,
                            geospatial_data_worldpop.mean,
                            geospatial_data_worldpop.sum,
                            NULL::numeric AS raw_value
                           FROM geospatial_data_worldpop
                          WHERE geospatial_data_worldpop.admin_level = 0
                        UNION ALL
                         SELECT geospatial_data_worldpop_age_sex.date,
                            geospatial_data_worldpop_age_sex.gid,
                            geospatial_data_worldpop_age_sex.source,
                            'geospatial_data_worldpop_age_sex'::text AS table_source,
                            geospatial_data_worldpop_age_sex.variable,
                            NULL::numeric AS mean,
                            geospatial_data_worldpop_age_sex.sum,
                            NULL::numeric AS raw_value
                           FROM geospatial_data_worldpop_age_sex
                          WHERE geospatial_data_worldpop_age_sex.admin_level = 0
                        UNION ALL
                         SELECT geospatial_data_worldpop_pwd.date,
                            geospatial_data_worldpop_pwd.gid,
                            geospatial_data_worldpop_pwd.source,
                            'geospatial_data_worldpop_pwd'::text AS table_source,
                            geospatial_data_worldpop_pwd.variable,
                            NULL::numeric AS mean,
                            NULL::numeric AS sum,
                            geospatial_data_worldpop_pwd.raw_value
                           FROM geospatial_data_worldpop_pwd
                          WHERE geospatial_data_worldpop_pwd.admin_level = 0
                        UNION ALL
                         SELECT geospatial_data_gfed.date,
                            geospatial_data_gfed.gid,
                            geospatial_data_gfed.source,
                            'geospatial_data_gfed'::text AS table_source,
                            geospatial_data_gfed.variable,
                            geospatial_data_gfed.mean,
                            NULL::numeric AS sum,
                            NULL::numeric AS raw_value
                           FROM geospatial_data_gfed
                          WHERE geospatial_data_gfed.admin_level = 0
                        UNION ALL
                         SELECT geospatial_data_nvdi.date,
                            geospatial_data_nvdi.gid,
                            geospatial_data_nvdi.source,
                            'geospatial_data_nvdi'::text AS table_source,
                            geospatial_data_nvdi.variable,
                            geospatial_data_nvdi.mean,
                            NULL::numeric AS sum,
                            NULL::numeric AS raw_value
                           FROM geospatial_data_nvdi
                          WHERE geospatial_data_nvdi.admin_level = 0
                        UNION ALL
                         SELECT geospatial_data_gleam.date,
                            geospatial_data_gleam.gid,
                            geospatial_data_gleam.source,
                            'geospatial_data_gleam'::text AS table_source,
                            geospatial_data_gleam.variable,
                            geospatial_data_gleam.mean,
                            NULL::numeric AS sum,
                            NULL::numeric AS raw_value
                           FROM geospatial_data_gleam
                          WHERE geospatial_data_gleam.admin_level = 0
                        UNION ALL
                         SELECT geospatial_data_era5.date,
                            geospatial_data_era5.gid,
                            geospatial_data_era5.source,
                            'geospatial_data_era5'::text AS table_source,
                            geospatial_data_era5.variable,
                            geospatial_data_era5.mean,
                            NULL::numeric AS sum,
                            NULL::numeric AS raw_value
                           FROM geospatial_data_era5
                          WHERE geospatial_data_era5.admin_level = 0
                        UNION ALL
                         SELECT geospatial_data_landcover.date,
                            geospatial_data_landcover.gid,
                            geospatial_data_landcover.source,
                            'geospatial_data_landcover'::text AS table_source,
                            geospatial_data_landcover.variable,
                            NULL::numeric AS mean,
                            NULL::numeric AS sum,
                            geospatial_data_landcover.raw_value
                           FROM geospatial_data_landcover
                          WHERE geospatial_data_landcover.admin_level = 0
                        UNION ALL
                         SELECT geospatial_data_merra2.date,
                            geospatial_data_merra2.gid,
                            geospatial_data_merra2.source,
                            'geospatial_data_merra2'::text AS table_source,
                            geospatial_data_merra2.variable,
                            geospatial_data_merra2.mean,
                            NULL::numeric AS sum,
                            NULL::numeric AS raw_value
                           FROM geospatial_data_merra2
                          WHERE geospatial_data_merra2.admin_level = 0
                        UNION ALL
                         SELECT geospatial_data_idmc.date,
                            geospatial_data_idmc.gid,
                            geospatial_data_idmc.source,
                            'geospatial_data_idmc'::text AS table_source,
                            geospatial_data_idmc.variable,
                            NULL::numeric AS mean,
                            NULL::numeric AS sum,
                            geospatial_data_idmc.raw_value
                           FROM geospatial_data_idmc
                          WHERE geospatial_data_idmc.admin_level = 0) combined_geospatial) gd
             LEFT JOIN data_catalog dc ON gd.variable::text = dc."Original Variable Name"
        )
 SELECT "Date",
    "ISO3 (GADM - GID)",
    "Source",
    "Category",
    "Variable",
    "Selected Calculation Value",
    "Total Deaths",
    "Number Injured",
    "Number Affected",
    "Number Homeless",
    "Total Affected",
    "Total Damage, Adjusted (USD)",
    "Reconstruction Costs, Adjusted (USD)",
    "AID Contribution (USD)",
    "Event - Disaster Internal Displacements"
   FROM ( SELECT events_data."Date",
            events_data."ISO3 (GADM - GID)",
            events_data."Source",
            events_data."Category",
            events_data."Variable",
            events_data."Selected Calculation Value",
            events_data."Total Deaths",
            events_data."Number Injured",
            events_data."Number Affected",
            events_data."Number Homeless",
            events_data."Total Affected",
            events_data."Total Damage, Adjusted (USD)",
            events_data."Reconstruction Costs, Adjusted (USD)",
            events_data."AID Contribution (USD)",
            events_data."Event - Disaster Internal Displacements"
           FROM events_data
        UNION ALL
         SELECT geospatial_data."Date",
            geospatial_data."ISO3 (GADM - GID)",
            geospatial_data."Source",
            geospatial_data."Category",
            geospatial_data."Variable",
            geospatial_data."Selected Calculation Value",
            geospatial_data."Total Deaths",
            geospatial_data."Number Injured",
            geospatial_data."Number Affected",
            geospatial_data."Number Homeless",
            geospatial_data."Total Affected",
            geospatial_data."Total Damage, Adjusted (USD)",
            geospatial_data."Reconstruction Costs, Adjusted (USD)",
            geospatial_data."AID Contribution (USD)",
            geospatial_data."Event - Disaster Internal Displacements"
           FROM geospatial_data) merged_data
  ORDER BY "Date", "ISO3 (GADM - GID)", "Source", "Category", "Variable"
WITH DATA;


CREATE UNIQUE INDEX idx_country_level_event_merged_geospatial_data_eav_viz
    ON public.country_level_event_merged_geospatial_data_eav_viz USING btree
    ("Date", "ISO3 (GADM - GID)" COLLATE pg_catalog."default", "Source" COLLATE pg_catalog."default", "Category" COLLATE pg_catalog."default", "Variable" COLLATE pg_catalog."default")
    TABLESPACE pg_default;
CREATE INDEX idx_country_level_event_merged_geospatial_data_eav_viz_cat_var
    ON public.country_level_event_merged_geospatial_data_eav_viz USING btree
    ("Category" COLLATE pg_catalog."default", "Variable" COLLATE pg_catalog."default")
    TABLESPACE pg_default;
CREATE INDEX idx_country_level_event_merged_geospatial_data_eav_viz_category
    ON public.country_level_event_merged_geospatial_data_eav_viz USING btree
    ("Category" COLLATE pg_catalog."default")
    TABLESPACE pg_default;
CREATE INDEX idx_country_level_event_merged_geospatial_data_eav_viz_date
    ON public.country_level_event_merged_geospatial_data_eav_viz USING btree
    ("Date")
    TABLESPACE pg_default;
CREATE INDEX idx_country_level_event_merged_geospatial_data_eav_viz_iso3
    ON public.country_level_event_merged_geospatial_data_eav_viz USING btree
    ("ISO3 (GADM - GID)" COLLATE pg_catalog."default")
    TABLESPACE pg_default;
CREATE INDEX idx_country_level_event_merged_geospatial_data_eav_viz_sor_var
    ON public.country_level_event_merged_geospatial_data_eav_viz USING btree
    ("Source" COLLATE pg_catalog."default", "Variable" COLLATE pg_catalog."default")
    TABLESPACE pg_default;
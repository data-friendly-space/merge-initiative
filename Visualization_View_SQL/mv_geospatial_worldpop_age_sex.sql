-- View: public.mv_geospatial_worldpop_age_sex

-- DROP MATERIALIZED VIEW IF EXISTS public.mv_geospatial_worldpop_age_sex;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_geospatial_worldpop_age_sex
TABLESPACE pg_default
AS
 SELECT gid,
    admin_level,
    date,
    max(
        CASE
            WHEN variable = 'population_sex_age_f_0_count'::text THEN sum
            ELSE NULL::numeric
        END) AS "Total Female Population Age <1",
    max(
        CASE
            WHEN variable = 'population_sex_age_f_1_count'::text THEN sum
            ELSE NULL::numeric
        END) AS "Total Female Population Age 1-4",
    max(
        CASE
            WHEN variable = 'population_sex_age_f_5_count'::text THEN sum
            ELSE NULL::numeric
        END) AS "Total Female Population Age 5-9",
    max(
        CASE
            WHEN variable = 'population_sex_age_f_10_count'::text THEN sum
            ELSE NULL::numeric
        END) AS "Total Female Population Age 10-14",
    max(
        CASE
            WHEN variable = 'population_sex_age_f_15_count'::text THEN sum
            ELSE NULL::numeric
        END) AS "Total Female Population Age 15-19",
    max(
        CASE
            WHEN variable = 'population_sex_age_f_20_count'::text THEN sum
            ELSE NULL::numeric
        END) AS "Total Female Population Age 20-24",
    max(
        CASE
            WHEN variable = 'population_sex_age_f_25_count'::text THEN sum
            ELSE NULL::numeric
        END) AS "Total Female Population Age 25-29",
    max(
        CASE
            WHEN variable = 'population_sex_age_f_30_count'::text THEN sum
            ELSE NULL::numeric
        END) AS "Total Female Population Age 30-34",
    max(
        CASE
            WHEN variable = 'population_sex_age_f_35_count'::text THEN sum
            ELSE NULL::numeric
        END) AS "Total Female Population Age 35-39",
    max(
        CASE
            WHEN variable = 'population_sex_age_f_40_count'::text THEN sum
            ELSE NULL::numeric
        END) AS "Total Female Population Age 40-44",
    max(
        CASE
            WHEN variable = 'population_sex_age_f_45_count'::text THEN sum
            ELSE NULL::numeric
        END) AS "Total Female Population Age 45-49",
    max(
        CASE
            WHEN variable = 'population_sex_age_f_50_count'::text THEN sum
            ELSE NULL::numeric
        END) AS "Total Female Population Age 50-54",
    max(
        CASE
            WHEN variable = 'population_sex_age_f_55_count'::text THEN sum
            ELSE NULL::numeric
        END) AS "Total Female Population Age 55-59",
    max(
        CASE
            WHEN variable = 'population_sex_age_f_60_count'::text THEN sum
            ELSE NULL::numeric
        END) AS "Total Female Population Age 60-64",
    max(
        CASE
            WHEN variable = 'population_sex_age_f_65_count'::text THEN sum
            ELSE NULL::numeric
        END) AS "Total Female Population Age 65-69",
    max(
        CASE
            WHEN variable = 'population_sex_age_f_70_count'::text THEN sum
            ELSE NULL::numeric
        END) AS "Total Female Population Age 70-74",
    max(
        CASE
            WHEN variable = 'population_sex_age_f_75_count'::text THEN sum
            ELSE NULL::numeric
        END) AS "Total Female Population Age 75-79",
    max(
        CASE
            WHEN variable = 'population_sex_age_f_80_count'::text THEN sum
            ELSE NULL::numeric
        END) AS "Total Female Population Age 80+",
    max(
        CASE
            WHEN variable = 'population_sex_age_m_0_count'::text THEN sum
            ELSE NULL::numeric
        END) AS "Total Male Population Age <1",
    max(
        CASE
            WHEN variable = 'population_sex_age_m_1_count'::text THEN sum
            ELSE NULL::numeric
        END) AS "Total Male Population Age 1-4",
    max(
        CASE
            WHEN variable = 'population_sex_age_m_5_count'::text THEN sum
            ELSE NULL::numeric
        END) AS "Total Male Population Age 5-9",
    max(
        CASE
            WHEN variable = 'population_sex_age_m_10_count'::text THEN sum
            ELSE NULL::numeric
        END) AS "Total Male Population Age 10-14",
    max(
        CASE
            WHEN variable = 'population_sex_age_m_15_count'::text THEN sum
            ELSE NULL::numeric
        END) AS "Total Male Population Age 15-19",
    max(
        CASE
            WHEN variable = 'population_sex_age_m_20_count'::text THEN sum
            ELSE NULL::numeric
        END) AS "Total Male Population Age 20-24",
    max(
        CASE
            WHEN variable = 'population_sex_age_m_25_count'::text THEN sum
            ELSE NULL::numeric
        END) AS "Total Male Population Age 25-29",
    max(
        CASE
            WHEN variable = 'population_sex_age_m_30_count'::text THEN sum
            ELSE NULL::numeric
        END) AS "Total Male Population Age 30-34",
    max(
        CASE
            WHEN variable = 'population_sex_age_m_35_count'::text THEN sum
            ELSE NULL::numeric
        END) AS "Total Male Population Age 35-39",
    max(
        CASE
            WHEN variable = 'population_sex_age_m_40_count'::text THEN sum
            ELSE NULL::numeric
        END) AS "Total Male Population Age 40-44",
    max(
        CASE
            WHEN variable = 'population_sex_age_m_45_count'::text THEN sum
            ELSE NULL::numeric
        END) AS "Total Male Population Age 45-49",
    max(
        CASE
            WHEN variable = 'population_sex_age_m_50_count'::text THEN sum
            ELSE NULL::numeric
        END) AS "Total Male Population Age 50-54",
    max(
        CASE
            WHEN variable = 'population_sex_age_m_55_count'::text THEN sum
            ELSE NULL::numeric
        END) AS "Total Male Population Age 55-59",
    max(
        CASE
            WHEN variable = 'population_sex_age_m_60_count'::text THEN sum
            ELSE NULL::numeric
        END) AS "Total Male Population Age 60-64",
    max(
        CASE
            WHEN variable = 'population_sex_age_m_65_count'::text THEN sum
            ELSE NULL::numeric
        END) AS "Total Male Population Age 65-69",
    max(
        CASE
            WHEN variable = 'population_sex_age_m_70_count'::text THEN sum
            ELSE NULL::numeric
        END) AS "Total Male Population Age 70-74",
    max(
        CASE
            WHEN variable = 'population_sex_age_m_75_count'::text THEN sum
            ELSE NULL::numeric
        END) AS "Total Male Population Age 75-79",
    max(
        CASE
            WHEN variable = 'population_sex_age_m_80_count'::text THEN sum
            ELSE NULL::numeric
        END) AS "Total Male Population Age 80+",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_f_0_4_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Female Population Aged 0-4 (2021-2022)",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_f_5_9_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Female Population Aged 5-9 (2021-2022)",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_f_10_14_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Female Population Aged 10-14 (2021-2022)",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_f_15_19_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Female Population Aged 15-19 (2021-2022)",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_f_20_24_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Female Population Aged 20-24 (2021-2022)",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_f_25_29_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Female Population Aged 25-29 (2021-2022)",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_f_30_34_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Female Population Aged 30-34 (2021-2022)",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_f_35_39_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Female Population Aged 35-39 (2021-2022)",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_f_40_44_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Female Population Aged 40-44 (2021-2022)",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_f_45_49_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Female Population Aged 45-49 (2021-2022)",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_f_50_54_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Female Population Aged 50-54 (2021-2022)",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_f_55_59_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Female Population Aged 55-59 (2021-2022)",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_f_60_64_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Female Population Aged 60-64 (2021-2022)",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_f_65_69_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Female Population Aged 65-69 (2021-2022)",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_f_70_74_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Female Population Aged 70-74 (2021-2022)",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_f_75_79_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Female Population Aged 75-79 (2021-2022)",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_f_80_84_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Female Population Aged 80-84 (2021-2022)",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_f_85_89_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Female Population Aged 85-89 (2021-2022)",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_f_90_94_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Female Population Aged 90-94 (2021-2022)",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_f_95_99_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Female Population Aged 95-99 (2021-2022)",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_f_100_2022_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Female Population Aged 100+ (2021-2022)",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_m_0_4_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Male Population Aged 0-4 (2021-2022)",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_m_5_9_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Male Population Aged 5-9 (2021-2022)",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_m_10_14_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Male Population Aged 10-14 (2021-2022)",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_m_15_19_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Male Population Aged 15-19 (2021-2022)",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_m_20_24_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Male Population Aged 20-24 (2021-2022)",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_m_25_29_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Male Population Aged 25-29 (2021-2022)",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_m_30_34_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Male Population Aged 30-34 (2021-2022)",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_m_35_39_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Male Population Aged 35-39 (2021-2022)",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_m_40_44_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Male Population Aged 40-44 (2021-2022)",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_m_45_49_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Male Population Aged 45-49 (2021-2022) ",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_m_50_54_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Male Population Aged 50-54 (2021-2022) ",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_m_55_59_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Male Population Aged 55-59 (2021-2022) ",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_m_60_64_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Male Population Aged 60-64 (2021-2022) ",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_m_65_69_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Male Population Aged 65-69 (2021-2022) ",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_m_70_74_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Male Population Aged 70-74 (2021-2022) ",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_m_75_79_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Male Population Aged 75-79 (2021-2022) ",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_m_80_84_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Male Population Aged 80-84 (2021-2022) ",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_m_85_89_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Male Population Aged 85-89 (2021-2022) ",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_m_90_94_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Male Population Aged 90-94 (2021-2022) ",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_m_95_99_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Male Population Aged 95-99 (2021-2022) ",
    max(
        CASE
            WHEN variable = '2021_2022_population_sex_age_m_100_2022_count'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Male Population Aged 100+ (2021-2022) "
   FROM geospatial_data_worldpop_age_sex
  GROUP BY gid, admin_level, date
WITH DATA;


CREATE INDEX idx_mv_geospatial_worldpop_age_sex_gid_admin_level_date
    ON public.mv_geospatial_worldpop_age_sex USING btree
    (gid COLLATE pg_catalog."default", admin_level, date)
    TABLESPACE pg_default;
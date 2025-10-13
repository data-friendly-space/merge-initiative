-- View: public.mv_geospatial_gdl

-- DROP MATERIALIZED VIEW IF EXISTS public.mv_geospatial_gdl;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_geospatial_gdl
TABLESPACE pg_default
AS
 SELECT gid,
    admin_level,
    date,
    max(
        CASE
            WHEN variable::text = 'hwrklow'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Percentage of Employed Men in Lower Nonfarm Jobs",
    max(
        CASE
            WHEN variable::text = 'infmort'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Infant Mortality Rate (per 1000 live births)",
    max(
        CASE
            WHEN variable::text = 'relhumidityyear'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Yearly Average Relative Humidity",
    max(
        CASE
            WHEN variable::text = 'surfacetempyear'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Yearly Average Surface Temperature (C)",
    max(
        CASE
            WHEN variable::text = 'totprecipyear'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Yearly Total Precipitation",
    max(
        CASE
            WHEN variable::text = 'age09'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Population Percentage Age 0-9",
    max(
        CASE
            WHEN variable::text = 'age1019'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Population Percentage Age 10-19",
    max(
        CASE
            WHEN variable::text = 'age2029'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Population Percentage Age 20-29",
    max(
        CASE
            WHEN variable::text = 'age3039'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Population Percentage Age 30-39",
    max(
        CASE
            WHEN variable::text = 'age4049'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Population Percentage Age 40-49",
    max(
        CASE
            WHEN variable::text = 'age5059'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Population Percentage Age 50-59",
    max(
        CASE
            WHEN variable::text = 'age6069'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Population Percentage Age 60-69",
    max(
        CASE
            WHEN variable::text = 'age7079'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Population Percentage Age 70-79",
    max(
        CASE
            WHEN variable::text = 'age8089'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Population Percentage Age 80-89",
    max(
        CASE
            WHEN variable::text = 'age90hi'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Population Percentage Age 90+",
    max(
        CASE
            WHEN variable::text = 'agedifmar'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Partners Avg Age Difference ",
    max(
        CASE
            WHEN variable::text = 'agemarw20'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Avg Age at First Marriage (Women 20-50)",
    max(
        CASE
            WHEN variable::text = 'bmiz'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "BMI for Age Z-Score",
    max(
        CASE
            WHEN variable::text = 'cellphone'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Percentage of Households with a Cellphone",
    max(
        CASE
            WHEN variable::text = 'dtp3age1'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Percentage of 1 year olds with DTP3 Vaccine",
    max(
        CASE
            WHEN variable::text = 'edyr25'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Mean Years Education (Adults 25+)",
    max(
        CASE
            WHEN variable::text = 'electr'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Percentage of Households with Electricity",
    max(
        CASE
            WHEN variable::text = 'hagri'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Percentage of Employed Men in Agriculture",
    max(
        CASE
            WHEN variable::text = 'haz'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Height for Age Z-score",
    max(
        CASE
            WHEN variable::text = 'hhsize'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Average Household Size",
    max(
        CASE
            WHEN variable::text = 'hwrkhigh'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Percentage of Employed Men in Upper Nonfarm Jobs",
    max(
        CASE
            WHEN variable::text = 'internet'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Percentage of Households with Internet",
    max(
        CASE
            WHEN variable::text = 'iwi'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "International Wealth Index Score (IWI)",
    max(
        CASE
            WHEN variable::text = 'iwipov35'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Percentage of Poorest Households (IWI Value under 35)",
    max(
        CASE
            WHEN variable::text = 'iwipov50'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Percentage of Poorest Households (IWI Value under 50)",
    max(
        CASE
            WHEN variable::text = 'iwipov70'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Percentage of Poorest Households (IWI Value under 70)",
    max(
        CASE
            WHEN variable::text = 'measlage1'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Percentage of 1 year olds with Measles Vaccine",
    max(
        CASE
            WHEN variable::text = 'menedyr25'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Average Years of Educations (Men 25+)",
    max(
        CASE
            WHEN variable::text = 'pipedwater'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Percentage of households with Piped Water",
    max(
        CASE
            WHEN variable::text = 'popold'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Percentage of Regional Population Aged 65+",
    max(
        CASE
            WHEN variable::text = 'popshare'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Percentage Share of National Population",
    max(
        CASE
            WHEN variable::text = 'popworkage'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Percentage of Regional Population Aged 15-65",
    max(
        CASE
            WHEN variable::text = 'regpopm'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Area Population",
    max(
        CASE
            WHEN variable::text = 'stunting'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Percentage of Stunted Children (0-59 months)",
    max(
        CASE
            WHEN variable::text = 'tfr'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Total Fertility Rate",
    max(
        CASE
            WHEN variable::text = 'thtbetween'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Wealth Inequality between Groups",
    max(
        CASE
            WHEN variable::text = 'thtwithin'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Wealth Inequality within Groups",
    max(
        CASE
            WHEN variable::text = 'u5mort'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Under 5 Mortality Rate",
    max(
        CASE
            WHEN variable::text = 'urban'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Percentage of Urban Population",
    max(
        CASE
            WHEN variable::text = 'wagri'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Percentage of Employed Women in Agriculture",
    max(
        CASE
            WHEN variable::text = 'waz'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Weight for Age Z-Score",
    max(
        CASE
            WHEN variable::text = 'whz'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Weight for Height Z-Score",
    max(
        CASE
            WHEN variable::text = 'womedyr25'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Average Years of Education (Women 25+)",
    max(
        CASE
            WHEN variable::text = 'workwom'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Percentage of Women in Paid Employment",
    max(
        CASE
            WHEN variable::text = 'wwrkhigh'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Percentage of Women Employed in Upper Nonfarm Jobs",
    max(
        CASE
            WHEN variable::text = 'wwrklow'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Percentage of Women Employed in Lower Nonfarm Jobs",
    max(
        CASE
            WHEN variable::text = 'edindex'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Educational Index",
    max(
        CASE
            WHEN variable::text = 'edindexf'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Educational Index - Female",
    max(
        CASE
            WHEN variable::text = 'edindexm'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Educational Index - Male",
    max(
        CASE
            WHEN variable::text = 'healthindex'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Health Index",
    max(
        CASE
            WHEN variable::text = 'healthindexf'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Health Index - Female",
    max(
        CASE
            WHEN variable::text = 'healthindexm'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Health Index - Male",
    max(
        CASE
            WHEN variable::text = 'incindex'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Income Index",
    max(
        CASE
            WHEN variable::text = 'lgnic'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Log Gross National Income Per Capita ",
    max(
        CASE
            WHEN variable::text = 'lifexp'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Life Expectancy",
    max(
        CASE
            WHEN variable::text = 'lifexpf'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Life Expectancy (Female)",
    max(
        CASE
            WHEN variable::text = 'lifexpm'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Life Expectancy (Male)",
    max(
        CASE
            WHEN variable::text = 'pop'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "GDL - Population Count (pop)",
    max(
        CASE
            WHEN variable::text = 'sgdi'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Subnational Gender Development Index (SGDI)",
    max(
        CASE
            WHEN variable::text = 'shdi'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Subnational Human Development Index (SHDI)",
    max(
        CASE
            WHEN variable::text = 'shdif'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Subnational Human Development Index (SHDI) - Female",
    max(
        CASE
            WHEN variable::text = 'shdim'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Subnational Human Development Index (SHDI) - Male"
   FROM geospatial_data_gdl
  GROUP BY gid, admin_level, date
WITH DATA;


CREATE INDEX idx_mv_geospatial_gdl_gid_admin_level_date
    ON public.mv_geospatial_gdl USING btree
    (gid COLLATE pg_catalog."default", admin_level, date)
    TABLESPACE pg_default;
-- View: public.mv_geospatial_landcover

-- DROP MATERIALIZED VIEW IF EXISTS public.mv_geospatial_landcover;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_geospatial_landcover
TABLESPACE pg_default
AS
 SELECT gid,
    admin_level,
    date,
    max(
        CASE
            WHEN variable = '12_cropland_rainfed_tree_or_shrub_cover'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Land Class - Rainfed Cropland Tree or Shrub Cover",
    max(
        CASE
            WHEN variable = '0_no_data'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Land Class - No Data",
    max(
        CASE
            WHEN variable = '100_mosaic_tree_and_shrub'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Land Class - Mosaic Tree and Shrub",
    max(
        CASE
            WHEN variable = '10_cropland_rainfed'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Land Class - Rainfed Cropland",
    max(
        CASE
            WHEN variable = '110_mosaic_herbaceous'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Land Class - Mosaic Herbaceous",
    max(
        CASE
            WHEN variable = '11_cropland_rainfed_herbaceous_cover'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Land Class - Rainfed Cropland Herbaceous Cover",
    max(
        CASE
            WHEN variable = '120_shrubland'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Land Class - Shrubland",
    max(
        CASE
            WHEN variable = '121_shrubland_evergreen'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Land Class - Evergreen Shrubland",
    max(
        CASE
            WHEN variable = '122_shrubland_deciduous'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Land Class - Deciduous Shrubland",
    max(
        CASE
            WHEN variable = '130_grassland'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Land Class - Grassland",
    max(
        CASE
            WHEN variable = '140_lichens_and_mosses'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Land Class - Lichens and Mosses",
    max(
        CASE
            WHEN variable = '150_sparse_vegetation'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Land Class - Sparse Vegetation",
    max(
        CASE
            WHEN variable = '151_sparse_tree'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Land Class - Sparse Tree",
    max(
        CASE
            WHEN variable = '152_sparse_shrub'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Land Class - Sparse Shrub",
    max(
        CASE
            WHEN variable = '153_sparse_herbaceous'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Land Class - Sparse Herbaceous",
    max(
        CASE
            WHEN variable = '160_tree_cover_flooded_fresh_or_brakish_water'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Land Class - Flooded Fresh or Brackish Water Tree Cover",
    max(
        CASE
            WHEN variable = '170_tree_cover_flooded_saline_water'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Land Class - Flooded Saline Water Tree Cover",
    max(
        CASE
            WHEN variable = '180_shrub_or_herbaceous_cover_flooded'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Land Class - Flooded Shrub or Herbaceous Cover",
    max(
        CASE
            WHEN variable = '190_urban'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Land Class - Urban",
    max(
        CASE
            WHEN variable = '200_bare_areas'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Land Class - Bare Areas",
    max(
        CASE
            WHEN variable = '201_bare_areas_consolidated'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Land Class - Consolidated Bare Areas",
    max(
        CASE
            WHEN variable = '202_bare_areas_unconsolidated'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Land Class - Unconsolidated Bare Areas",
    max(
        CASE
            WHEN variable = '20_cropland_irrigated'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Land Class - Irrigated Cropland",
    max(
        CASE
            WHEN variable = '210_water'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Land Class - Water",
    max(
        CASE
            WHEN variable = '220_snow_and_ice'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Land Class - Snow and Ice",
    max(
        CASE
            WHEN variable = '30_mosaic_cropland'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Land Class - Mosaic Cropland",
    max(
        CASE
            WHEN variable = '40_mosaic_natural_vegetation'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Land Class - Mosaic Natural Vegetation",
    max(
        CASE
            WHEN variable = '50_tree_broadleaved_evergreen_closed_to_open'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Land Class - Broadleaved Evergreen Tree Closed to Open",
    max(
        CASE
            WHEN variable = '60_tree_broadleaved_deciduous_closed_to_open'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Land Class - Broadleaved Deciduous Tree Closed to Open",
    max(
        CASE
            WHEN variable = '61_tree_broadleaved_deciduous_closed'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Land Class - Broadleaved Deciduous Tree Closed",
    max(
        CASE
            WHEN variable = '62_tree_broadleaved_deciduous_open'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Land Class - Broadleaved Decisuous Tree Open",
    max(
        CASE
            WHEN variable = '70_tree_needleleaved_evergreen_closed_to_open'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Land Class - Needleleaved Evergreen Tree Closed to Open",
    max(
        CASE
            WHEN variable = '71_tree_needleleaved_evergreen_closed'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Land Class - Needleleaved Evergreen Tree Closed",
    max(
        CASE
            WHEN variable = '72_tree_needleleaved_evergreen_open'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Land Class - Needleleaved Evergreen Tree Open",
    max(
        CASE
            WHEN variable = '80_tree_needleleaved_deciduous_closed_to_open'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Land Class - Needleleaved Deciduous Tree Closed to Open",
    max(
        CASE
            WHEN variable = '81_tree_needleleaved_deciduous_closed'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Land Class - Needleleaved Deciduous Tree Closed",
    max(
        CASE
            WHEN variable = '82_tree_needleleaved_deciduous_open'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Land Class - Needleleaved Deciduous Tree Open",
    max(
        CASE
            WHEN variable = '90_tree_mixed'::text THEN raw_value
            ELSE NULL::numeric
        END) AS "Land Class - Mixed Tree"
   FROM geospatial_data_landcover
  GROUP BY gid, admin_level, date
WITH DATA;


CREATE INDEX idx_mv_geospatial_landcover_gid_admin_level_date
    ON public.mv_geospatial_landcover USING btree
    (gid COLLATE pg_catalog."default", admin_level, date)
    TABLESPACE pg_default;
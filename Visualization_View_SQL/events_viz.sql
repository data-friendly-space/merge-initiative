-- View: public.events_viz

-- DROP MATERIALIZED VIEW IF EXISTS public.events_viz;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.events_viz
TABLESPACE pg_default
AS
 SELECT event_id AS "Event ID",
    event_name AS "Event Name",
    disaster_group AS "Disaster Group",
    disaster_subgroup AS "Disaster Sub Group",
    disaster_type AS "Disaster Type",
    disaster_subtype AS "Disaster Sub Type",
    iso3_code AS "ISO3 (GADM - GID)",
    admin_level_0 AS "Administrative Name - Level 0",
    admin_level_1 AS "Administrative Name List - Level 1",
    admin_level_2 AS "Administrative Name List - Level 2",
    start_date AS "Date",
    end_date AS "Event End Date",
    total_deaths AS "Total Deaths",
    number_injured AS "Number Injured",
    number_affected AS "Number Affected",
    number_homeless AS "Number Homeless",
    total_affected AS "Total Affected",
    total_damage_adjusted AS "Total Damage, Adjusted (USD)",
    reconstruction_costs_adjusted AS "Reconstruction Costs, Adjusted (USD)",
    aid_contribution AS "AID Contribution (USD)",
    disaster_internal_displacements AS "Event - Disaster Internal Displacements",
    source AS "Source",
    metadata AS "Metadata",
    usgs AS "United States Geological Survey (USGS) Earthquake ID",
    glide AS "GLIDE number",
    dfo AS "Darthmouth Flood Observatory (DFO) Event ID",
    local_identifier AS "Local Identifier",
    ifrc_appeal_id AS "IFRC Appeal ID",
    government_assigned_identifier AS "Government Assigned Identifier"
   FROM events
WITH DATA;


CREATE INDEX "events_viz_Date_idx"
    ON public.events_viz USING btree
    ("Date")
    TABLESPACE pg_default;
CREATE INDEX "events_viz_Disaster Sub Type_idx"
    ON public.events_viz USING btree
    ("Disaster Sub Type" COLLATE pg_catalog."default")
    TABLESPACE pg_default;
CREATE INDEX "events_viz_Disaster Type_idx"
    ON public.events_viz USING btree
    ("Disaster Type" COLLATE pg_catalog."default")
    TABLESPACE pg_default;
CREATE INDEX "events_viz_Event ID_idx"
    ON public.events_viz USING btree
    ("Event ID")
    TABLESPACE pg_default;
CREATE INDEX "events_viz_ISO3 (GADM - GID)_idx"
    ON public.events_viz USING btree
    ("ISO3 (GADM - GID)" COLLATE pg_catalog."default")
    TABLESPACE pg_default;
CREATE INDEX "events_viz_Source_idx"
    ON public.events_viz USING btree
    ("Source" COLLATE pg_catalog."default")
    TABLESPACE pg_default;
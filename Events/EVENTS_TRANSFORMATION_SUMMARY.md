# Events Data Transformation Summary

This table summarizes all transformations applied to EM-DAT and IDMC datasets before merging into the unified `events` table.

> For detailed methodology, processing steps, and usage instructions, see [README.md](README.md).

---

## EM-DAT Transformations

| Original Field                              | Final Field                               | Transformation                                                 | Example Before                                         | Example After                                             |
| ------------------------------------------- | ----------------------------------------- | -------------------------------------------------------------- | ------------------------------------------------------ | --------------------------------------------------------- |
| `Start Year`, `Start Month`, `Start Day`    | `start_date`                              | Combine into datetime; missing month/day default to 1          | `2020`, `3`, `15`                                      | `2020-03-15`                                              |
| `End Year`, `End Month`, `End Day`          | `end_date`                                | Combine into datetime; if end < start, set to start            | `2020`, `3`, `NaN`                                     | `2020-03-01`                                              |
| `External IDs`                              | `GLIDE`, `USGS`, `DFO` (arrays)           | Parse pipe-separated `Type:Value` pairs                        | `"GLIDE:EQ-2007-000033\|USGS:usp000f660"`              | `GLIDE=['EQ-2007-000033']`, `USGS=['usp000f660']`         |
| `Admin Units`                               | `admin_level_1`, `admin_level_2` (arrays) | Parse list of dicts; extract `adm1_name` and `adm2_name`       | `"[{'adm1_name':'Arkansas'},{'adm2_name':'Bolivar'}]"` | `admin_level_1=['Arkansas']`, `admin_level_2=['Bolivar']` |
| `AID Contribution ('000 US$)`               | `aid_contribution`                        | Multiply by 1000                                               | `5000`                                                 | `5000000`                                                 |
| `Reconstruction Costs, Adjusted ('000 US$)` | `reconstruction_costs_adjusted`           | Multiply by 1000                                               | `10000`                                                | `10000000`                                                |
| `Total Damage, Adjusted ('000 US$)`         | `total_damage_adjusted`                   | Multiply by 1000                                               | `25000`                                                | `25000000`                                                |
| `ISO`                                       | `iso3_code`                               | Special mappings (HKG→CHN, MAC→CHN, ANT→NLD, AB9→SDN, XKX→XKO) | `HKG`                                                  | `CHN`                                                     |
| `Country`                                   | `admin_level_0`                           | Lookup from mapping file                                       | `Hong Kong`                                            | `China`                                                   |
| `admin_level_1` values                      | `admin_level_1`                           | Match against GADM main/alt/local names                        | `"Sao Paulo"`                                          | `"São Paulo"`                                             |
| `Disaster Group`                            | `disaster_group`                          | Rename only                                                    | `Natural`                                              | `Natural`                                                 |
| `Disaster Subgroup`                         | `disaster_subgroup`                       | Rename only                                                    | `Climatological`                                       | `Climatological`                                          |
| `Disaster Type`                             | `disaster_type`                           | Rename only                                                    | `Flood`                                                | `Flood`                                                   |
| `Disaster Subtype`                          | `disaster_subtype`                        | Rename only                                                    | `Flash flood`                                          | `Flash flood`                                             |
| `Total Deaths`                              | `total_deaths`                            | Rename only                                                    | `150`                                                  | `150`                                                     |
| `No. Injured`                               | `number_injured`                          | Rename only                                                    | `500`                                                  | `500`                                                     |
| `No. Affected`                              | `number_affected`                         | Rename only                                                    | `10000`                                                | `10000`                                                   |
| `No. Homeless`                              | `number_homeless`                         | Rename only                                                    | `2000`                                                 | `2000`                                                    |
| `Total Affected`                            | `total_affected`                          | Rename only                                                    | `12500`                                                | `12500`                                                   |
| Multiple columns                            | `metadata` (JSONB)                        | Pack into JSON                                                 | `DisNo.`, `Location`, `Latitude`, etc.                 | `{"DisNo.": "2021-0335-USA", "Region": "Americas", ...}`  |
| (new)                                       | `source`                                  | Add constant                                                   | —                                                      | `EMDAT`                                                   |
| (generated)                                 | `event_name`                              | Pattern: `{ISO3}_{type}_{subtype}_{YYYYMMDD}_{uuid8}`          | —                                                      | `USA_Flood_Flash flood_20210607_02a04b47`                 |

---

## IDMC Transformations

| Original Field                    | Final Field                                                                              | Transformation                               | Example Before                                               | Example After                                               |
| --------------------------------- | ---------------------------------------------------------------------------------------- | -------------------------------------------- | ------------------------------------------------------------ | ----------------------------------------------------------- |
| `Event Codes (Code:Type)`         | `GLIDE`, `IFRC_Appeal_ID`, `Local_Identifier`, `Government_Assigned_Identifier` (arrays) | Parse semicolon-separated `Value:Type` pairs | `"FL-2023-000079-BIH:Glide Number; MDRBA015:IFRC Appeal ID"` | `GLIDE=['FL-2023-000079']`, `IFRC_Appeal_ID=['MDRBA015']`   |
| GLIDE values                      | `GLIDE`                                                                                  | Remove ISO3 suffix                           | `FL-2023-000079-BIH`                                         | `FL-2023-000079`                                            |
| `ISO3`                            | `iso3_code`                                                                              | Same mappings as EM-DAT                      | `HKG`                                                        | `CHN`                                                       |
| `Country / Territory`             | `admin_level_0`                                                                          | Lookup from mapping file                     | `Hong Kong`                                                  | `China`                                                     |
| `Event Name`                      | `admin_level_1`, `admin_level_2` (arrays)                                                | Regex matching against GADM location lists   | `"Typhoon - Guangdong - Shenzhen 20200815"`                  | `admin_level_1=['Guangdong']`, `admin_level_2=['Shenzhen']` |
| `Hazard Type`, `Hazard Sub Type`  | `disaster_type`, `disaster_subtype`                                                      | Map via hazard type mapping file             | `Storm`, `Typhoon/Hurricane/Cyclone`                         | `Storm`, `Tropical cyclone`                                 |
| `Date of Event (start)`           | `start_date`                                                                             | Rename only                                  | `2020-08-15`                                                 | `2020-08-15`                                                |
| `Disaster Internal Displacements` | `disaster_internal_displacements`                                                        | Rename only                                  | `50000`                                                      | `50000`                                                     |
| Multiple columns                  | `metadata` (JSONB)                                                                       | Pack into JSON                               | `Event Name`, `Year`, `Hazard Type`, etc.                    | `{"Year": 2023, "Event Name": "Bosnia...", ...}`            |
| (new)                             | `source`                                                                                 | Add constant                                 | —                                                            | `IDMC`                                                      |
| (generated)                       | `event_name`                                                                             | Same pattern as EM-DAT                       | —                                                            | `BIH_Storm_Storm (General)_20230514_11a7e496`               |

---

## ISO3 Mapping Rules (Both Sources)

| Source ISO3   | Target ISO3   | Target Country      | Special Handling                               |
| ------------- | ------------- | ------------------- | ---------------------------------------------- |
| `HKG`         | `CHN`         | China               | Original country name added to `admin_level_1` |
| `MAC`         | `CHN`         | China               | Original country name added to `admin_level_1` |
| `ANT`         | `NLD`         | Netherlands         | Original country name added to `admin_level_1` |
| `AB9`         | `SDN`         | Sudan               | "Abyei Area" added to `admin_level_2`          |
| `XKX` / `XKK` | `XKO`         | Kosovo              | Direct mapping                                 |
| `SCG`         | `SRB` / `MNE` | Serbia / Montenegro | Determined by matching admin locations         |

---

## Merge Process

| Step | Action                   | Details                                                   |
| ---- | ------------------------ | --------------------------------------------------------- |
| 1    | Insert all EM-DAT events | All records from `events_emdat` → `events`                |
| 2    | Match IDMC by GLIDE      | Check if any GLIDE code overlaps with existing events     |
| 3a   | If GLIDE match found     | Update existing event's `disaster_internal_displacements` |
| 3b   | If no GLIDE match        | Insert IDMC event as new record                           |

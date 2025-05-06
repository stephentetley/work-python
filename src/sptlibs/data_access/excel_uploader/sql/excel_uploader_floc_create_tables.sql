-- 
-- Copyright 2025 Stephen Tetley
-- 
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
-- 
-- http://www.apache.org/licenses/LICENSE-2.0
-- 
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
-- 



CREATE SCHEMA IF NOT EXISTS excel_uploader_floc_create;



-- The tables and views follow a pattern - the table contains just the fields 
-- a client needs to fill out, the view corresponds to a sheet in the uploader
-- xlsx file


-- functional_location is just the fields client code needs to fill out...
CREATE OR REPLACE TABLE excel_uploader_floc_create.functional_location (
    functional_location VARCHAR NOT NULL,
    floc_description VARCHAR NOT NULL,
    category VARCHAR,
    str_indicator VARCHAR,
    object_type VARCHAR,
    start_up_date DATETIME,
    maint_plant VARCHAR,
    plant_section VARCHAR,
    cost_center VARCHAR,
    planning_plant VARCHAR,
    maint_work_center VARCHAR,
    plant_work_center VARCHAR,
    PRIMARY KEY (functional_location)
);



CREATE OR REPLACE VIEW excel_uploader_floc_create.vw_functional_location AS
SELECT 
    t.functional_location AS "Functional Location",
    t.floc_description AS "Description",
    t.category AS "FunctLocCat",
    t.str_indicator AS "StrIndicator",
    null AS "Inactive",
    t.object_type AS "Object type",
    null AS "AuthorizGroup",
    null AS "Gross Weight",
    null AS "Unit of weight",
    null AS "Inventory no",
    null AS "Size/dimens",
    strftime(t.start_up_date, '%d.%m.%Y') AS "Start-up date",
    null AS "AcquisitionValue",
    null AS "Currency",
    null AS "Acquistion date",
    null AS "Manufacturer",
    null AS "Model number",
    null AS "ManufPartNo",
    null AS "ManufSerialNo",
    null AS "ManufCountry",
    null AS "ConstructYear",
    null AS "ConstructMth",
    t.maint_plant AS "MaintPlant",
    null AS "Location", 
    null AS "Room",
    t.plant_section AS "Plant section",
    null AS "Work center",
    null AS "ABC indic",
    null AS "Sort field",
    null AS "Business Area",
    null AS "Asset",
    null AS "Sub-number",
    t.cost_center AS "Cost Center",
    null AS "WBS Element",
    null AS "StandgOrder",
    null AS "SettlementOrder",
    t.planning_plant AS "Planning plant",
    null AS "Planner group",
    t.maint_work_center AS "Main WorkCtr",
    t.plant_work_center AS "Plnt WorkCenter",
    null AS "Catalog profile",
    null AS "SupFunctLoc",
    null AS "Position",
    null AS "Ref. Location",
    IF(funct_loc_cat >= 5, 'X', null) AS "Installation Allowed",
    null AS "Construction type",
    null AS "Status Profile",
    null AS "Status of an object",
    null AS "Status without stsno",
    null AS "Begin guarantee(C)",
    null AS "Warranty end(C)",
    null AS "Master Warranty(C)",
    null AS "InheritWarranty(C)",
    null AS "Pass on warranty(C)",
    null AS "Begin guarantee(V)",
    null AS "Warranty end(V)",
    null AS "Master Warranty(V)",
    null AS "InheritWarranty(V)",
    null AS "Pass on warranty(V)",
    null AS "Sales Org",
    null AS "Distr. Channel",
    null AS "Division",
    null AS "Sales Office",
    null AS "Sales Group",
FROM excel_uploader_floc_create.functional_location t
ORDER BY t.category, t.functional_location;

-- No Primary Key - multiples allowed
CREATE OR REPLACE TABLE excel_uploader_floc_create.classification (
    functional_location VARCHAR NOT NULL,
    class_name VARCHAR NOT NULL,
    characteristics VARCHAR NOT NULL,
    char_value VARCHAR,
);

CREATE OR REPLACE VIEW excel_uploader_floc_create.vw_classification AS
SELECT 
    t.functional_location AS "Functional Location",
    t.class_name AS "Class",
    t.characteristics AS "Characteristics",
    t.char_value AS "Char Value",
FROM excel_uploader_floc_create.classification t
ORDER BY t.functional_location, t.class_name, t.characteristics;



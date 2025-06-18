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



CREATE SCHEMA IF NOT EXISTS excel_uploader_equi_change;



-- The tables and views follow a pattern - the table contains just the fields 
-- a client needs to fill out, the view corresponds to a sheet in the uploader
-- xlsx file

CREATE OR REPLACE TABLE excel_uploader_equi_change.change_request_header (
    usmd_crequest VARCHAR,
	change_request_decription VARCHAR NOT NULL,
);

CREATE OR REPLACE VIEW excel_uploader_equi_change.vw_change_request_header AS
SELECT 
    t.usmd_crequest AS "Change Request",
    t.change_request_decription AS "Change Request Description",
    null AS "Priority",
    null AS "Due Date",
FROM excel_uploader_equi_change.change_request_header t;
    
    

CREATE OR REPLACE TABLE excel_uploader_equi_change.change_request_notes (
    usmd_note VARCHAR NOT NULL,
);


-- equipment is just the fields client code needs to fill out...
CREATE OR REPLACE TABLE excel_uploader_equi_change.equipment_data (
    equi VARCHAR NOT NULL,
    category VARCHAR,
    equi_description VARCHAR NOT NULL,
    object_type VARCHAR,
    gross_weight DECIMAL,
    unit_of_weight VARCHAR,
    start_up_date DATETIME,
    manufacturer VARCHAR,
    model_number VARCHAR,
    manuf_part_no VARCHAR,
    manuf_serial_number VARCHAR,
    maint_plant VARCHAR,
    catalog_profile VARCHAR,
    functional_loc VARCHAR,
    superord_equip VARCHAR,
    position INTEGER,
    tech_ident_no VARCHAR,
    status_profile VARCHAR,
    user_status VARCHAR,
    PRIMARY KEY (equi)
);

CREATE OR REPLACE VIEW excel_uploader_equi_change.vw_equipment_data AS
SELECT 
    t.equi AS "Equipment",
    t.category AS "EquipCategory",
    t.equi_description AS "Description (medium)",
    null AS "Valid From",
    null AS "Inactive",
    t.object_type AS "Object type",
    null AS "AuthorizGroup",
    t.gross_weight AS "Gross Weight",
    t.unit_of_weight AS "Unit of weight",
    null AS "Inventory no",
    null AS "Size/dimens",
    strftime(t.start_up_date, '%d.%m.%Y') AS "Start-up date",
    null AS "AcquisitionValue",
    null AS "Currency",
    null AS "Acquistion date",
    t.manufacturer AS "Manufacturer",
    t.model_number AS "Model number",
    t.manuf_part_no AS "ManufPartNo",
    t.manuf_serial_number AS "ManufSerialNo",
    null AS "ManufCountry",
    strftime(t.start_up_date, '%Y') AS "ConstructYear",
    strftime(t.start_up_date, '%m') AS "ConstructMth",
    t.maint_plant AS "MaintPlant",
    null AS "Plant section",
    null AS "Location", 
    null AS "Room",
    null AS "ABC indic",
    null AS "Work center",
    null AS "Sort field",
    null AS "Business Area",
    null AS "Asset",
    null AS "Sub-number",
    null AS "Cost Center",
    null AS "WBS Element",
    null AS "StandgOrder",
    null AS "SettlementOrder",
    null AS "Planning plant",
    null AS "Planner group",
    null AS "Main WorkCtr",
    null AS "Plnt WorkCenter",
    t.catalog_profile AS "Catalog profile",
    t.functional_loc AS "Functional loc.",
    t.superord_equip AS "Superord.Equip.",
    printf('%04d', t.position) AS "Position",
	t.tech_ident_no AS "TechIdentNo.",
	null AS "Construction type Ma",
    null AS "Material",
    null AS "Material Serial Numb",
    null AS "Config.material",
    t.status_profile AS "Status Profile",
    t.user_status AS "Status of an object",
    t.user_status AS "Status without stsno",
    null AS "Sales Org",
    null AS "Distr. Channel",
    null AS "Division",
    null AS "Sales Office",
    null AS "Sales Group",
	null AS "License no.",
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
	null AS "Vendor",
	null AS "Customer",
	null AS "End customer", 
	null AS "CurCustomer", 
	null AS "Operator", 
	null AS "Delivery date",
FROM excel_uploader_equi_change.equipment_data t
ORDER BY t.equi;


-- No Primary Key - multiples allowed
CREATE OR REPLACE TABLE excel_uploader_equi_change.classification (
    equi VARCHAR NOT NULL,
    class_name VARCHAR NOT NULL,
    characteristics VARCHAR NOT NULL,
    char_value VARCHAR,
);

CREATE OR REPLACE VIEW excel_uploader_equi_change.vw_classification AS
SELECT 
    t.equi AS "Functional Location",
    t.class_name AS "Class",
    t.characteristics AS "Characteristics",
    t.char_value AS "Char Value",
FROM excel_uploader_equi_change.classification t
ORDER BY t.equi, t.class_name, t.characteristics;




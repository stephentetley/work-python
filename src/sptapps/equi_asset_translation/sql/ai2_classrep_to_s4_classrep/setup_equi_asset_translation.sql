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

CREATE SCHEMA IF NOT EXISTS equi_asset_translation;

CREATE OR REPLACE TABLE equi_asset_translation.status_lookup (
    ai2_status VARCHAR NOT NULL,
    s4_status VARCHAR NOT NULL,
    PRIMARY KEY (ai2_status)
);

-- TODO - store this in a data file (spreadsheet or whatever...)
INSERT INTO equi_asset_translation.status_lookup
    VALUES ('OPERATIONAL', 'OPER'), ('DISPOSED OF', 'DISP')
    ;
    

-- Note - don't try to discriminate class type here, need more context 
-- than ai2_equipment_type ...
CREATE OR REPLACE TABLE equi_asset_translation.objecttype_lookup (
    ai2_equipment_type VARCHAR NOT NULL,
    s4_category VARCHAR NOT NULL,
    s4_objecttype VARCHAR NOT NULL,
    PRIMARY KEY (ai2_equipment_type)
);

INSERT INTO equi_asset_translation.objecttype_lookup
    VALUES 
        ('EQUIPMENT: BURGLAR ALARM', 'I', 'ALAM'),
        ('EQUIPMENT: CONDUCTIVITY INSTRUMENT', 'I', 'ANAL'),
        ('EQUIPMENT: CONDUCTIVITY LEVEL INSTRUMENT', 'I', 'LSTN'), 
        ('EQUIPMENT: DIRECT ON LINE STARTER', 'E', 'STAR'), 
        ('EQUIPMENT: FIRE ALARM', 'I', 'ALAM'), 
        ('EQUIPMENT: GEARBOX', 'M', 'TRUT'), 
        ('EQUIPMENT: ISOLATING VALVES', 'M', 'VALV'),
        ('EQUIPMENT: NON-IMMERSIBLE MOTOR', 'E', 'EMTR'),
        ('EQUIPMENT: PENSTOCK', 'M', 'VALV'),
        ('EQUIPMENT: SCREW PUMP', 'M', 'PUMP'), 
        ('EQUIPMENT: SPEED/RPM INSTRUMENT', 'I', 'SPTN'), 
        ('EQUIPMENT: ULTRASONIC LEVEL INSTRUMENT', 'I', 'LSTN'),
        
    ;


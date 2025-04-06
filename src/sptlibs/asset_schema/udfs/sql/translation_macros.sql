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



CREATE OR REPLACE MACRO udfx.get_voltage_ac_or_dc(str) AS
(WITH 
    cte1 AS (
        SELECT upper(str) AS stru
        ),
    cte2 AS ( 
        SELECT 
        CASE 
            WHEN stru = 'DIRECT CURRENT' THEN 'VDC'
            WHEN stru = 'ALTERNATING CURRENT' THEN 'VAC'
            ELSE '##ERROR[' || stru || ']'
        END AS answer
        FROM cte1
       ) 
SELECT answer FROM cte2
);
 

CREATE OR REPLACE MACRO udfx.get_s4_asset_status(str) AS
(WITH 
    cte1 AS (
        SELECT upper(str) AS stru
        ),
    cte2 AS ( 
        SELECT 
        CASE 
            WHEN stru = 'ABANDONED' THEN 'ABND'
            WHEN stru = 'ADOPTABLE' THEN '##ADOPTABLE' 
            WHEN stru = 'AWAITING DISPOSAL' THEN 'ADIS'
            WHEN stru = 'CLOSED' THEN '##CLOSED'
            WHEN stru = 'DECOMMISSIONED' THEN 'DCOM'
            WHEN stru = 'DISPOSED OF' THEN 'DISP'
            WHEN stru = 'NON OPERATIONAL' THEN 'NOP'
            WHEN stru = 'OPERATIONAL' THEN 'OPER'
            WHEN stru = 'PLANNED' THEN '##PLANNED'
            WHEN stru = 'SOLD' THEN 'SOLD'
            WHEN stru = 'TO BE COMMISSIONED' THEN 'TBCM'
            WHEN stru = 'TO BE CONSTRUCTED' THEN '##TO_BE_CONSTRUCTED'
            WHEN stru = 'TRANSFERRED' THEN 'TRAN'
            WHEN stru = 'UNDER CONSTRUCTION' THEN 'UCON'
            ELSE '##ERROR[' || stru || ']'
        END AS answer
        FROM cte1
       ) 
SELECT answer FROM cte2
);

CREATE OR REPLACE MACRO udfx.size_to_millimetres(units, value) AS
(WITH 
    cte1 AS (
        SELECT upper(units) AS unitsu
        ),
    cte2 AS ( 
        SELECT 
            CASE 
                WHEN unitsu IN ('MILLIMETRES', 'MM') THEN TRY_CAST(value AS DECIMAL)
                WHEN unitsu IN ('CENTIMETRES', 'CM') THEN TRY_CAST(value AS DECIMAL) * 10.0  
                WHEN unitsu IN ('METRES', 'M') THEN TRY_CAST(value AS DECIMAL) * 1000.0
                WHEN unitsu = 'INCH' THEN TRY_CAST(value AS DECIMAL) * 25.4
                ELSE null
            END AS answer
        FROM cte1
        )
SELECT TRY_CAST(answer AS INTEGER) FROM cte2
);

CREATE OR REPLACE MACRO udfx.weight_to_kilograms(units, value) AS
(WITH 
    cte1 AS (
        SELECT upper(units) AS unitsu
        ),
    cte2 AS ( 
        SELECT 
            CASE 
                WHEN unitsu IN ('KILOGRAMS', 'KG') THEN TRY_CAST(value AS DECIMAL)
                ELSE null
            END AS answer
        FROM cte1
        )
SELECT TRY_CAST(answer AS DECIMAL) FROM cte2
);

-- For kVA use a power factor of 80.0
CREATE OR REPLACE MACRO udfx.power_to_killowatts(units, value) AS 
(WITH 
    cte1 AS (
        SELECT 
            upper(units) AS unitsu, 
            80.0 AS kva_power_factor
        ),
    cte2 AS ( 
        SELECT 
            CASE 
                WHEN unitsu IN ('KILOWATTS', 'KW') THEN TRY_CAST(value AS DECIMAL)
                WHEN unitsu IN ('WATTS', 'W') THEN TRY_CAST(value AS DECIMAL) / 1000.0
                WHEN unitsu = 'KILOVOLT AMP' THEN TRY_CAST(value AS DECIMAL) * kva_power_factor
            ELSE null
        END AS answer
    FROM cte1
    )
SELECT answer FROM cte2
);
            
CREATE OR REPLACE MACRO udfx.get_output_type(str) AS
(WITH 
    cte1 AS (
        SELECT upper(str) AS stru
        ),
    cte2 AS ( 
        SELECT 
        CASE 
            WHEN stru = 'DIGITAL' THEN 'DIGITAL'
            WHEN stru IN ('MA', 'MV') THEN 'ANALOGUE' 
            ELSE null
        END AS answer
    FROM cte1
    )
SELECT answer FROM cte2
);

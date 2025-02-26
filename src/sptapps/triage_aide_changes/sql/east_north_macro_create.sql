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

CREATE OR REPLACE MACRO _osgb36_decode_major(cu) AS
(CASE 
    WHEN cu = 'S' THEN row(0,       0)
    WHEN cu = 'T' THEN row(500_000, 0)
    WHEN cu = 'N' THEN row(0,       500_000)
    WHEN cu = 'O' THEN row(500_000, 500_000)
    WHEN cu = 'H' THEN row(0,       1_000_000)
    ELSE NULL 
END
);
    
CREATE OR REPLACE MACRO _osgb36_decode_minor(cu) AS
(CASE 
    WHEN cu = 'A' THEN row(0,         400_000)
    WHEN cu = 'B' THEN row(100_000,   400_000)
    WHEN cu = 'C' THEN row(200_000,   400_000)
    WHEN cu = 'D' THEN row(300_000,   400_000)
    WHEN cu = 'E' THEN row(400_000,   400_000)
    WHEN cu = 'F' THEN row(0,         300_000)
    WHEN cu = 'G' THEN row(100_000,   300_000)
    WHEN cu = 'H' THEN row(200_000,   300_000)
    WHEN cu = 'J' THEN row(300_000,   300_000)
    WHEN cu = 'K' THEN row(400_000,   300_000)
    WHEN cu = 'L' THEN row(0,         200_000)
    WHEN cu = 'M' THEN row(100_000,   200_000)
    WHEN cu = 'N' THEN row(200_000,   200_000)
    WHEN cu = 'O' THEN row(300_000,   200_000)
    WHEN cu = 'P' THEN row(400_000,   200_000)
    WHEN cu = 'Q' THEN row(0,         100_000)
    WHEN cu = 'R' THEN row(100_000,   100_000)
    WHEN cu = 'S' THEN row(200_000,   100_000)
    WHEN cu = 'T' THEN row(300_000,   100_000)
    WHEN cu = 'U' THEN row(400_000,   100_000)
    WHEN cu = 'V' THEN row(0,         0)
    WHEN cu = 'W' THEN row(100_000,   0)
    WHEN cu = 'X' THEN row(200_000,   0)
    WHEN cu = 'Y' THEN row(300_000,   0)
    WHEN cu = 'Z' THEN row(400_000,   0)
    ELSE null
END
);

CREATE OR REPLACE MACRO get_easting(str) AS 
(WITH cte1 AS (
    SELECT 
        upper(str) AS ustr
        ),
    cte2 AS (
        SELECT         
            ustr[1] AS major_letter,
            ustr[2] AS minor_letter,
            _osgb36_decode_major(major_letter) AS major_struct,
            _osgb36_decode_minor(minor_letter) AS minor_struct,
            try_cast(ustr[3:7] AS INTEGER) AS east1,
            struct_extract(major_struct, 1) + struct_extract(minor_struct, 1) + east1 AS answer
        FROM cte1
    )
    SELECT answer FROM cte2
);


CREATE OR REPLACE MACRO get_northing(str) AS 
(WITH 
    cte1 AS (
        SELECT upper(str) AS ustr
        ),
    cte2 AS (
        SELECT 
            ustr[1] AS major_letter,
            ustr[2] AS minor_letter,
            _osgb36_decode_major(major_letter) AS major_struct,
            _osgb36_decode_minor(minor_letter) AS minor_struct,
            try_cast(ustr[8:12] AS INTEGER) AS north1,
            struct_extract(major_struct, 2) + struct_extract(minor_struct, 2) +  north1 AS answer
        FROM cte1    
    )
    SELECT answer FROM cte2
);


-- Get easting and northing separately this macro induces subquery error/limitation 
--CREATE OR REPLACE MACRO get_east_north(str) AS 
--(WITH 
--    cte1 AS (
--        SELECT upper(str) AS ustr
--        ),
--    cte2 AS (
--        SELECT 
--          ustr[1] AS major_letter,
--            ustr[2] AS minor_letter,
--            _osgb36_decode_major(major_letter) AS major_struct,
--            _osgb36_decode_minor(minor_letter) AS minor_struct,
--            try_cast(ustr[3:7] AS INTEGER) AS east1,
--            try_cast(ustr[8:12] AS INTEGER) AS north1,
--            row(struct_extract(major_struct, 1) + struct_extract(minor_struct, 1) + east1, 
--                struct_extract(major_struct, 2) + struct_extract(minor_struct, 2) + north1) AS answer
--        FROM cte1
--    )
--    SELECT answer FROM cte2
--);

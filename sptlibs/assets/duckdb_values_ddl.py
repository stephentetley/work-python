"""
Copyright 2023 Stephen Tetley

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

asset_values_ddl = """
-- No primary keys, (`item_id` * `field_name`) might not be unique

CREATE OR REPLACE TABLE values_string(
    item_id TEXT NOT NULL,
    field_name TEXT NOT NULL, 
    value TEXT
);

CREATE OR REPLACE TABLE values_date(
    item_id TEXT NOT NULL,
    field_name TEXT NOT NULL, 
    value DATE
);

CREATE OR REPLACE TABLE values_time(
    item_id TEXT NOT NULL,
    field_name TEXT NOT NULL, 
    value TIME
);

CREATE OR REPLACE TABLE values_integer(
    item_id TEXT NOT NULL,
    field_name TEXT NOT NULL, 
    value INTEGER
);


CREATE OR REPLACE TABLE values_decimal(
    item_id TEXT NOT NULL,
    field_name TEXT NOT NULL, 
    value DECIMAL(18, 3)
);

CREATE OR REPLACE TABLE values_wide_decimal(
    item_id TEXT NOT NULL,
    field_name TEXT NOT NULL, 
    value DECIMAL(30, 8)
);

"""

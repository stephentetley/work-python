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

s4_ih_char_values_ddl = """
    CREATE OR REPLACE TABLE s4_ih_char_values (
        entity_id TEXT NOT NULL,
        class_type TEXT NOT NULL,
        class_name TEXT NOT NULL,
        char_name TEXT NOT NULL,
        text_value TEXT,
        numeric_value DECIMAL(26,6)
    );
    """

s4_classes_used_ddl = """
    CREATE OR REPLACE TABLE s4_classes_used (
        class_type TEXT NOT NULL,
        class_name TEXT NOT NULL,
        table_name TEXT NOT NULL,
    );
    """


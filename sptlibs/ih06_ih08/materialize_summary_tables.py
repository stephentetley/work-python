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
import duckdb
import ih06_ih08.materialize_masterdata as materialize_masterdata
import ih06_ih08.unpivot_classes as unpivot_classes

def materialize_summary_tables(con: duckdb.DuckDBPyConnection) -> None:
    materialize_masterdata.materialize_masterdata(con=con)
    unpivot_classes.unpivot_classes(con=con)

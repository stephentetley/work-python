"""
Copyright 2024 Stephen Tetley

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

import os
from typing import Callable, Any
import duckdb
import polars as pl


# TODO SqlStatementRunner class?
class SqlScriptRunner2:
    def __init__(self, filepath: str | None, con: duckdb.DuckDBPyConnection) -> None:
        """Uses __file__ to get path to static `sql` folders."""
        self.con = con
        if filepath:
            pymodule_dir = os.path.dirname(os.path.realpath(filepath))
            _sql_dir = os.path.normpath(os.path.join(pymodule_dir, "./sql"))
            if os.path.exists(_sql_dir):
                self.sql_root_dir = _sql_dir
            else: 
                print("Cannot find `{_sql_dir}`") 
                raise NotADirectoryError(f"Cannot find `{_sql_dir}`")   
        else:
            self.sql_root_dir = None


    def exec_sql_file(self, *, rel_file_path: str) -> None:
        sql_file_path = os.path.normpath(os.path.join(self.sql_root_dir, rel_file_path))
        if os.path.exists(sql_file_path):
            with open(sql_file_path) as file:
                statements = file.read()
                try: 
                    self.con.execute(statements)
                    self.con.commit()
                except Exception as exn: 
                    print(f"SQL script failed:")
                    print(statements)
                    print(exn)
                    raise(exn)
        else: 
            print(f"SQL file does not exist {sql_file_path}")
            raise FileNotFoundError(f"SQL file does not exist {sql_file_path}")


    def exec_sql_generating_file(self, *, rel_file_path: str) -> None:
        """The SQL file should have a single query and contain a column called `sql_text`"""
        sql_file_path = os.path.normpath(os.path.join(self.sql_root_dir, rel_file_path))
        if os.path.exists(sql_file_path):
            with open(sql_file_path) as file:
                statement = file.read()
                df = self.con.execute(statement).pl()
                for row in df.rows(named=True):
                    sql_stmt = row['sql_text']
                    try:
                        self.con.execute(sql_stmt)
                    except Exception as exn: 
                        print(f"SQL script failed:")
                        print(sql_stmt)
                        print(exn)
                        raise(exn)
                self.con.commit()
        else: 
            print(f"SQL file does not exist {sql_file_path}")
            raise FileNotFoundError(f"SQL file does not exist {sql_file_path}")


    def exec_sql_generating_stmt(self, *, sql_query: str) -> None:
        """`sql_query` should be a single query and contain a column called `sql_text`"""
        df = self.con.execute(sql_query).pl()
        for row in df.rows(named=True):
            sql_stmt = row['sql_text']
            try:
                self.con.execute(sql_stmt)
            except Exception as exn: 
                print(f"SQL query failed:")
                print(sql_query)
                print(exn)
                raise(exn)
        self.con.commit()


    def eval_sql_generating_stmt(self, *, sql_query: str, action: Callable[[dict[str, Any], pl.DataFrame], None]) -> None:
        """`sql_query` should be a single query and contain a column called `sql_text`"""
        df = self.con.execute(sql_query).pl()
        for row in df.rows(named=True):
            sql_stmt = row['sql_text']
            row.pop('sql_text')
            try:
                df1 = self.con.execute(sql_stmt).pl()
                action(row, df1)
            except Exception as exn: 
                print(f"SQL query failed:")
                print(sql_query)
                print(exn)
                raise(exn)
        self.con.commit()


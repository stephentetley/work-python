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
import jinja2
import polars as pl

class SqlScriptRunner:
    def __init__(self) -> None:
        """Throws exception if $ASSET_DATA_SQL_DIR is missing."""
        sql_root_dir = os.environ.get('ASSET_DATA_SQL_DIR', '')
        if sql_root_dir:
            if os.path.exists(sql_root_dir):
                self.sql_root_dir = sql_root_dir
                loader = jinja2.FileSystemLoader(searchpath=self.sql_root_dir)
                self.jinja_env = jinja2.Environment(loader=loader)
            else: 
                print("Cannot find $ASSET_DATA_SQL_DIR") 
                raise NotADirectoryError(f"Cannot find $ASSET_DATA_SQL_DIR = `{sql_root_dir}`")
        else:
            print("Environment Variable $ASSET_DATA_SQL_DIR not set")
            raise OSError("Environment Variable $ASSET_DATA_SQL_DIR not set")

    def exec_sql_file(self, *, file_rel_path: str, con: duckdb.DuckDBPyConnection) -> None:
        sql_file_path = os.path.normpath(os.path.join(self.sql_root_dir, file_rel_path))
        if os.path.exists(sql_file_path):
            with open(sql_file_path) as file:
                statements = file.read()
                try: 
                    con.execute(statements)
                    con.commit()
                except Exception as exn: 
                    print(f"SQL script failed:")
                    print(statements)
                    print(exn)
                    raise(exn)
        else: 
            print(f"SQL file does not exist {sql_file_path}")
            raise FileNotFoundError(f"SQL file does not exist {sql_file_path}")

    def exec_jinja_sql_file(self, *, args: dict, file_rel_path: str, con: duckdb.DuckDBPyConnection) -> None:
        template = self.jinja_env.get_template(file_rel_path)
        if template: 
            statement = template.render(args)
            con.execute(statement)
        else: 
            print(f"Jinja SQL template file does not exist {file_rel_path}")
            raise FileNotFoundError(f"Jinja SQL template file does not exist {file_rel_path}")



    def exec_sql_generating_file(self, *, file_rel_path: str, con: duckdb.DuckDBPyConnection) -> None:
        """The SQL file should have a single query and contain a column called `sql_text`"""
        sql_file_path = os.path.normpath(os.path.join(self.sql_root_dir, file_rel_path))
        if os.path.exists(sql_file_path):
            with open(sql_file_path) as file:
                statement = file.read()
                df = con.execute(statement).pl()
                for row in df.rows(named=True):
                    sql_stmt = row['sql_text']
                    try:
                        con.execute(sql_stmt)
                    except Exception as exn: 
                        print(f"SQL script failed:")
                        print(sql_stmt)
                        print(exn)
                        raise(exn)
                con.commit()
        else: 
            print(f"SQL file does not exist {sql_file_path}")
            raise FileNotFoundError(f"SQL file does not exist {sql_file_path}")


    def exec_sql_generating_stmt(self, *, sql_query: str, con: duckdb.DuckDBPyConnection) -> None:
        """`sql_query` should be a single query and contain a column called `sql_text`"""
        df = con.execute(sql_query).pl()
        for row in df.rows(named=True):
            sql_stmt = row['sql_text']
            try:
                con.execute(sql_stmt)
            except Exception as exn: 
                print(f"SQL query failed:")
                print(sql_query)
                print(exn)
                raise(exn)
        con.commit()


    def eval_sql_generating_stmt(self, *, sql_query: str, action: Callable[[dict[str, Any], pl.DataFrame], None],  con: duckdb.DuckDBPyConnection) -> None:
        """`sql_query` should be a single query and contain a column called `sql_text`"""
        df = con.execute(sql_query).pl()
        for row in df.rows(named=True):
            sql_stmt = row['sql_text']
            row.pop('sql_text')
            try:
                df1 = con.execute(sql_stmt).pl()
                action(row, df1)
            except Exception as exn: 
                print(f"SQL query failed:")
                print(sql_query)
                print(exn)
                raise(exn)
        con.commit()        
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
from datetime import date
from typing import Self
import polars as pl
from sptapps.floc_builder.floc import Floc
from sptapps.floc_builder.site import Site
from sptapps.floc_builder.system import System
from sptapps.floc_builder.subsystem import Subsystem

class GenFlocs:
    def __init__(self, source_path: str, site: Site) -> None:
        if os.path.exists(source_path):
            self.lookups = _read_xlsx_source(source_path=source_path)
        else:
            self.lookups = {}
        self.flocs = set()
        self.site = site
        self.context = site.floc_context
    
    def gen_flocs(self) -> list[Floc]:
        self._add_site_floc()
        for sys in self.site.systems.values():
            self._add_function_floc(sys)
            self._add_process_group_floc(sys)
            self._add_process_floc(sys)
            self._add_sys_floc(sys)
            self._add_subsystem_flocs(sys)
        dict1 = {x.functional_location : x for x in self.flocs}
        ls = list(dict1.values())
        ls.sort(key=lambda x: x.functional_location)
        return ls

    def _add_site_floc(self) -> Self:
        floc = Floc(functional_location=self.site.siteid,
                    description=self.site.sitename,
                    startup_date=self.context.startup_date,
                    object_type='SITE',
                    structure_indicator='YW-GS', 
                    status='OPER')
        self.flocs.add(floc)
        return self

    def _add_function_floc(self, sys: System) -> Self:
        otype = sys.path[0:3]
        descr = self.get_description(otype)
        floc = Floc(functional_location=f'{self.site.siteid}-{otype}',
                    description=descr,
                    startup_date=self.context.startup_date,
                    object_type=otype,
                    structure_indicator='YW-GS', 
                    status='OPER')
        self.flocs.add(floc)
        return self
    
    def _add_process_group_floc(self, sys: System) -> Self:
        otype = sys.path[4:7]
        descr = self.get_description(otype)
        floc = Floc(functional_location=f'{self.site.siteid}-{sys.path[0:7]}',
                    description=descr,
                    startup_date=self.context.startup_date,
                    object_type=otype,
                    structure_indicator='YW-GS', 
                    status='OPER')
        self.flocs.add(floc)
        return self

    def _add_process_floc(self, sys: System) -> Self:
        otype = sys.path[8:11]
        descr = self.get_description(otype)
        floc = Floc(functional_location=f'{self.site.siteid}-{sys.path}',
                    description=descr,
                    startup_date=self.context.startup_date,
                    object_type=otype,
                    structure_indicator='YW-GS', 
                    status='OPER')
        self.flocs.add(floc)
        return self
    
    def _add_sys_floc(self, sys: System) -> Self:
        floc = Floc(functional_location=f'{self.site.siteid}-{sys.key}',
                    description=sys.name,
                    startup_date=self.context.startup_date,
                    object_type=sys.otype,
                    structure_indicator='YW-GS', 
                    status='OPER', 
                    class_type=sys.ctype)
        self.flocs.add(floc)
        return self
        
    def _add_subsystem_flocs(self, sys:System) -> Self:
        for ss in sys.subsystems.values():
            self._add_subsystem_floc(ss, sys.key)

    def _add_subsystem_floc(self, subsys:Subsystem, path: str) -> Self:
        floc = Floc(functional_location=f'{self.site.siteid}-{path}-{subsys.key}',
                    description=subsys.name,
                    object_type=subsys.otype,
                    startup_date=self.context.startup_date,
                    structure_indicator='YW-GS', 
                    status='OPER')
        self.flocs.add(floc)
        return self

    def get_description(self, obj_type: str) -> str | None:
        return self.lookups[obj_type]

def _read_xlsx_source(source_path: str) -> dict[str, str]:
    df = pl.read_excel(source=source_path, sheet_name='Sheet1', engine='calamine', drop_empty_rows=True)
    return {row[0]: row[1] for row in df.iter_rows()}


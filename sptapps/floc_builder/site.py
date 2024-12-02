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

from typing import Self, Sequence
from datetime import date
import itertools

from sptapps.floc_builder.system import System
from sptapps.floc_builder.floc import Floc

class Site:
    def __init__(self) -> None:
        self.sitename = ''
        self.siteid = ''
        self.otype = 'SITE'
        self.structure_indicator = 'YW-GS'
        self.status = 'OPER'
        self.systems: dict[str, System] = dict()
    
    def __repr__(self): 
        s1 = repr(self.systems)
        return f'Site({self.sitename}, {self.siteid}, {s1})'
    
    def site_name(self, s: str) -> Self:
        self.sitename = s
        return self

    def site_id(self, id: str) -> Self:
        self.siteid = id
        return self
    
    def add_system(self, sys: System) -> Self:
        self.systems.update({sys.key: sys})
        return self
    
    def add_systems(self, systems: Sequence[System]) -> Self:
        for x in systems:
            self.add_system(x)
        return self
    
    def _gen_site_floc(self, startup_date: date, easting: int, northing: int) -> Floc:
        floc_chars = []
        return Floc(function_location=self.siteid,
                    description=self.sitename,
                    startup_date=startup_date,
                    object_type='SITE',
                    structure_indicator=self.structure_indicator, 
                    status=self.status, 
                    floc_chars=floc_chars)
    
    def gen_flocs(self, startup_date: date, easting: int, northing: int) -> list[Floc]:
        floc1 = self._gen_site_floc(startup_date=startup_date,
                                    easting=easting,
                                    northing=northing)
        fn1 = lambda ky, sys: sys.gen_flocs(site_floc=self.siteid, startup_date=startup_date, easting=easting, northing=northing)
        sub_flocs = [fn1(ky, sys) for ky, sys in self.systems.items()]
        return list(itertools.chain(*[[floc1]] + sub_flocs))





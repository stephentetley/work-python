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


from sptapps.floc_builder.system import System
from sptapps.floc_builder.context import Context

class Site:
    def __init__(self) -> None:
        self.sitename = ''
        self.siteid = ''
        self.otype = 'SITE'
        self.structure_indicator = 'YW-GS'
        self.status = 'OPER'
        self.systems: dict[str, System] = dict()
        self.context = Context()
    
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






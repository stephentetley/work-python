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

from typing import Self
from datetime import date

from sptapps.floc_builder.subsystem import Subsystem
from sptapps.floc_builder.floc import Floc

class System:
    def __init__(self, 
                 *, 
                 name: str, 
                 path: str, 
                 otype: str, 
                 ctype: str, 
                 subsystems: list[Subsystem] = [], 
                 index: int=1, 
                 prefix: str='SYS') -> None:
        self.name = name
        self.index = index
        self.path = path
        self.otype = otype
        self.ctype = ctype
        self.structure_indicator = 'YW-GS'
        self.status = 'OPER'
        self.prefix = prefix
        self.subsystems = dict()
        for ss in subsystems:
            self.subsystems.update({ss.name: ss})

    def __repr__(self): 
        s1 = repr(self.subsystems)
        return f'System({self.key}, {self.name}, {s1})'
    
    @property
    def key(self) -> str:
        return f'{self.path}-{self.prefix}{self.index:02d}'

    def set_name(self, name: str) -> Self:
        self.name = name
        return self

    def add_subsystem(self, *, ss: Subsystem) -> Self:
        self.subsystems.update({ss.name: ss})
        return self



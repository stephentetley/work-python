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

# Can we have dictionaries for levels making remove easier?

class Subsystem:
    def __init__(self, *, name: str, ty: str) -> None:
        self.name = name
        self.ty = ty
        self.structure_indicator = 'YW-GS'
        self.status = 'OPER'

    def __repr__(self): 
        return f'Subsystem({self.name}, {self.ty}, {self.status})'

class System:
    def __init__(self, *, name: str, path: str, otype: str, ctype: str, subsystems: dict[str, Self]) -> None:
        self.name = name
        self.path = path
        self.otype = otype
        self.ctype = ctype
        self.structure_indicator = 'YW-GS'
        self.status = 'OPER'
        self.subsystems: dict[str, Subsystem] = subsystems

    def set_name(self, name: str) -> Self:
        self.name = name
        return self

    def add_subsystem(self, *, ss: Subsystem) -> Self:
        self.subsystems[ss.name] = ss
        return self

    def __repr__(self): 
        s1 = repr(self.subsystems)
        return f'System({self.name}, {s1})'

class Site:
    def __init__(self) -> None:
        self.sitename = ''
        self.siteid = ''
        self.otype = 'SITE'
        self.structure_indicator = 'YW-GS'
        self.systems: dict[str, Self] = {}

    def site_name(self, s: str) -> Self:
        self.sitename = s
        return self

    def site_id(self, id: str) -> Self:
        self.siteid = id
        return self
    
    def add_system(self, sys: System) -> Self:
        self.systems[sys.path] = sys
        return self


    def __repr__(self): 
        s1 = repr(self.systems)
        return f'Site({self.sitename}, {self.siteid}, {s1})'



def telemetry_system() -> System: 
    return System(name='Telemetry System', 
                  path='CAA-NET-TEL', 
                  otype='CTOS', 
                  ctype='CTOSSY', 
                  subsystems={})

def fixed_lifting_system() -> System: 
    return System(name='Fixed Lifting',
                  path='SMS-LFT-FLT', 
                  otype='SLFS',
                  ctype='SLFSSY',
                  subsystems={})

def portable_lifting_system() -> System: 
    return System(name='Portable Lifting System',
                  path='SMS-LFT-PRL',
                  otype='SLFS',
                  ctype='SLFSSY',
                  subsystems={})

def edc_outfall_system() -> System: 
    return System(name='Outfall System', 
                  path='EDC-LQD-OUT', 
                  otype='SOTF', 
                  ctype='SOTFSY', 
                  subsystems={})


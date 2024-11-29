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

from sptapps.floc_builder.system import System

class Site:
    def __init__(self) -> None:
        self.sitename = ''
        self.siteid = ''
        self.otype = 'SITE'
        self.structure_indicator = 'YW-GS'
        self.systems: dict[str, System] = {}

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


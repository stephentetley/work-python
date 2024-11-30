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

from sptapps.floc_builder.system import System
from sptapps.floc_builder.subsystem import Subsystem

# Pre-built systems

# System is not SYSdd - e.g. SIT01-ACH-ACH-ACH-NON01
def nop_archive(prefix: str) -> System: 
    return System(name=f'{prefix} Non Op Equip',
                  path='ACH-ACH-ACH',
                  otype='ACH',
                  ctype='',
                  subsystems={}, 
                  index=1,
                  prefix='NON')

def plc_control(name='PLC Control System', *, index=1) -> System: 
    return System(name=name, 
                  path='CAA-NET-LOC', 
                  otype='SCNT', 
                  ctype='SCNTSY', 
                  index=index,
                  subsystems={})


def telemetry(name='Telemetry System', *, index=1) -> System: 
    return System(name=name, 
                  path='CAA-NET-TEL', 
                  otype='CTOS', 
                  ctype='CTOSSY', 
                  index=index,
                  subsystems={})

def edc_outfall(name='Outfall System', *, index=1, subsystems: list[Subsystem]=[]) -> System: 
    return System(name=name, 
                  path='EDC-LQD-OUT', 
                  otype='SOTF', 
                  ctype='SOTFSY',
                  index=index, 
                  subsystems=subsystems)

def kiosk(name='Kiosk System', *, index=1, subsystems: list[Subsystem]=[]) -> System: 
    return building(name=name, index=index, subsystems=subsystems)

def building(name: str, *, index=1, subsystems: list[Subsystem]=[]) -> System: 
    return System(name=name, 
                  path='SIF-STM-BLG', 
                  otype='SKIO', 
                  ctype='SKIOSY', 
                  index=index, 
                  subsystems=subsystems)

def fixed_lifting() -> System: 
    return System(name='Fixed Lifting',
                  path='SMS-LFT-FLT', 
                  otype='SLFS',
                  ctype='SLFSSY',
                  index=1,
                  subsystems={})

def portable_lifting() -> System: 
    return System(name='Portable Lifting System',
                  path='SMS-LFT-PRL',
                  otype='SLFS',
                  ctype='SLFSSY',
                  index=1,
                  subsystems={})

def mobile_plant_storage_area(*, subsystems: list[Subsystem]=[]) -> System: 
    return System(name='Storage Area',
                  path='SMS-MBP-MBR',
                  otype='SSTA',
                  ctype='SSTASY',
                  index=1,
                  subsystems=subsystems)

def site_intruder_alarm(*, name='Intruder Alarm System', index=1, subsystems: list[Subsystem]=[]) -> System: 
    return System(name=name,
                  path='SSS-SEC-ASY',
                  otype='SINT',
                  ctype='SINTSY',
                  index=index,
                  subsystems=subsystems)

def site_lighting(*, name='Site Lighting System', index=1, subsystems: list[Subsystem]=[]) -> System:
    return System(name=name,
                  path='SSS-SLM-LTG',
                  otype='SELI',
                  ctype='SELISY',
                  index=index,
                  subsystems=subsystems)



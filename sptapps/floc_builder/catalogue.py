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
def nop_archive_system(*, prefix: str) -> System: 
    return System(name=f'{prefix} Non Op Equip',
                  path='ACH-ACH-ACH',
                  otype='ACH',
                  ctype='',
                  subsystems={})

def plc_control_system(name='PLC Control System') -> System: 
    return System(name=name, 
                  path='CAA-NET-LOC', 
                  otype='SCNT', 
                  ctype='SCNTSY', 
                  subsystems={})


def telemetry_system(*, name='Telemetry System') -> System: 
    return System(name=name, 
                  path='CAA-NET-TEL', 
                  otype='CTOS', 
                  ctype='CTOSSY', 
                  subsystems={})

def edc_outfall_system() -> System: 
    return System(name='Outfall System', 
                  path='EDC-LQD-OUT', 
                  otype='SOTF', 
                  ctype='SOTFSY', 
                  subsystems={})

def kiosk_system(name='Kiosk System') -> System: 
    return System(name=name, 
                  path='SIF-STM-BLG', 
                  otype='SKIO', 
                  ctype='SKIOSY', 
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

def mobile_plant_storage_area() -> System: 
    return System(name='Storage Area',
                  path='SMS-MBP-MBR',
                  otype='SSTA',
                  ctype='SSTASY',
                  subsystems={})

def lighting_system(name: str) -> System:
    return System(name=name,
                  path='SSS-SLM-LTG',
                  otype='SELI',
                  ctype='SELISY',
                  subsystems={})

def intruder_alarm_system() -> System: 
    return System(name='Intruder Alarm System',
                  path='SSS-SEC-ASY',
                  otype='SINT',
                  ctype='SINTSY',
                  subsystems={})

# Pre-built subsystems
def kiosk(*, name='Kiosk', index=1) -> Subsystem:
    return Subsystem(name=name,
                     otype='KISK',
                     prefix='KIS',
                     index=index
                     )
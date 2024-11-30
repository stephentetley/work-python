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

from sptapps.floc_builder.subsystem import Subsystem

# Pre-built subsystems

def air_dryer(name, *, index=1) -> Subsystem:
    return Subsystem(name=name,
                     otype='AIRD',
                     prefix='AIR',
                     index=index
                     )

def air_dryers(names: list[str], *, start_index:int=1) -> list[Subsystem]: 
    return [air_dryer(n, index=start_index+i) for i, n in enumerate(names)] 

def alarm(name, *, index=1) -> Subsystem:
    return Subsystem(name=name,
                     otype='ALAM',
                     prefix='ALM',
                     index=index
                     )

def alarms(names: list[str], *, start_index:int=1) -> list[Subsystem]: 
    return [alarm(n, index=start_index+i) for i, n in enumerate(names)] 

def blower(name, *, index=1) -> Subsystem:
    return Subsystem(name=name,
                     otype='BLOW',
                     prefix='BLO',
                     index=index
                     )

def blowers(names: list[str], *, start_index:int=1) -> list[Subsystem]: 
    return [blower(n, index=start_index+i) for i, n in enumerate(names)] 

def building(name, *, index=1) -> Subsystem:
    return Subsystem(name=name,
                     otype='BLDG',
                     prefix='BLD',
                     index=index
                     )

def buildings(names: list[str], *, start_index:int=1) -> list[Subsystem]: 
    return [building(n, index=start_index+i) for i, n in enumerate(names)] 

def chamber(name, *, index=1) -> Subsystem:
    return Subsystem(name=name,
                     otype='CHAM',
                     prefix='CHB',
                     index=index
                     )

def chambers(names: list[str], *, start_index:int=1) -> list[Subsystem]: 
    return [chamber(n, index=start_index+i) for i, n in enumerate(names)] 

def compressor(name, *, index=1) -> Subsystem:
    return Subsystem(name=name,
                     otype='COMP',
                     prefix='CMP',
                     index=index
                     )

def compressors(names: list[str], *, start_index:int=1) -> list[Subsystem]: 
    return [compressor(n, index=start_index+i) for i, n in enumerate(names)] 

def conveyor(name, *, index=1) -> Subsystem:
    return Subsystem(name=name,
                     otype='CVYR',
                     prefix='CVY',
                     index=index
                     )

def conveyors(names: list[str], *, start_index:int=1) -> list[Subsystem]: 
    return [conveyor(n, index=start_index+i) for i, n in enumerate(names)] 

def dam(name, *, index=1) -> Subsystem:
    return Subsystem(name=name,
                     otype='DAMS',
                     prefix='DAM',
                     index=index
                     )

def dams(names: list[str], *, start_index:int=1) -> list[Subsystem]: 
    return [dam(n, index=start_index+i) for i, n in enumerate(names)] 

def destructor(name, *, index=1) -> Subsystem:
    return Subsystem(name=name,
                     otype='DEST',
                     prefix='DES',
                     index=index
                     )

def destructors(names: list[str], *, start_index:int=1) -> list[Subsystem]: 
    return [destructor(n, index=start_index+i) for i, n in enumerate(names)] 

def drain(name, *, index=1) -> Subsystem:
    return Subsystem(name=name,
                     otype='DRAN',
                     prefix='DRN',
                     index=index
                     )

def drains(names: list[str], *, start_index:int=1) -> list[Subsystem]: 
    return [drain(n, index=start_index+i) for i, n in enumerate(names)] 


def fan(name, *, index=1) -> Subsystem:
    return Subsystem(name=name,
                     otype='FANS',
                     prefix='FAN',
                     index=index
                     )

def fans(names: list[str], *, start_index:int=1) -> list[Subsystem]: 
    return [fan(n, index=start_index+i) for i, n in enumerate(names)] 

def process_filter(name, *, index=1) -> Subsystem:
    return Subsystem(name=name,
                     otype='PROF',
                     prefix='FIL',
                     index=index
                     )

def process_filters(names: list[str], *, start_index:int=1) -> list[Subsystem]: 
    return [process_filter(n, index=start_index+i) for i, n in enumerate(names)] 

def heater(name, *, index=1) -> Subsystem:
    return Subsystem(name=name,
                     otype='HEAT',
                     prefix='HGT',
                     index=index
                     )

def heaters(names: list[str], *, start_index:int=1) -> list[Subsystem]: 
    return [heater(n, index=start_index+i) for i, n in enumerate(names)] 

def heat_exchanger(name, *, index=1) -> Subsystem:
    return Subsystem(name=name,
                     otype='HEEX',
                     prefix='HEX',
                     index=index
                     )

def heat_exchangers(names: list[str], *, start_index:int=1) -> list[Subsystem]: 
    return [heat_exchanger(n, index=start_index+i) for i, n in enumerate(names)] 

def kiosk(name, *, index=1) -> Subsystem:
    return Subsystem(name=name,
                     otype='KISK',
                     prefix='KIS',
                     index=index
                     )

def kiosks(names: list[str], *, start_index:int=1) -> list[Subsystem]: 
    return [kiosk(n, index=start_index+i) for i, n in enumerate(names)] 


def lighting_unit(name, *, index=1) -> Subsystem:
    return Subsystem(name=name,
                     otype='LIDE',
                     prefix='LGT',
                     index=index
                     )

def lighting_units(names: list[str], *, start_index:int=1) -> list[Subsystem]: 
    return [lighting_unit(n, index=start_index+i) for i, n in enumerate(names)] 

def lightning_conductor(name, *, index=1) -> Subsystem:
    return Subsystem(name=name,
                     otype='LIPO',
                     prefix='LPO',
                     index=index
                     )

def lightning_conductors(names: list[str], *, start_index:int=1) -> list[Subsystem]: 
    return [lightning_conductor(n, index=start_index+i) for i, n in enumerate(names)] 

def mixer(name, *, index=1) -> Subsystem:
    return Subsystem(name=name,
                     otype='MIXR',
                     prefix='MXR',
                     index=index
                     )

def mixers(names: list[str], *, start_index:int=1) -> list[Subsystem]: 
    return [mixer(n, index=start_index+i) for i, n in enumerate(names)] 

def pump(name, *, index=1) -> Subsystem:
    return Subsystem(name=name,
                     otype='PUMP',
                     prefix='PMP',
                     index=index
                     )

def pumps(names: list[str], *, start_index:int=1) -> list[Subsystem]: 
    return [pump(n, index=start_index+i) for i, n in enumerate(names)] 


def fine_screen(name, *, index=1) -> Subsystem:
    return Subsystem(name=name,
                     otype='SCRF',
                     prefix='SCN',
                     index=index
                     )

def fine_screens(names: list[str], *, start_index:int=1) -> list[Subsystem]: 
    return [fine_screen(n, index=start_index+i) for i, n in enumerate(names)] 

def tank(name, *, index=1) -> Subsystem:
    return Subsystem(name=name,
                     otype='TANK',
                     prefix='TNK',
                     index=index
                     )

def tanks(names: list[str], *, start_index:int=1) -> list[Subsystem]: 
    return [tank(n, index=start_index+i) for i, n in enumerate(names)] 

def valve(name, *, index=1) -> Subsystem:
    return Subsystem(name=name,
                     otype='VALV',
                     prefix='VAL',
                     index=index
                     )

def valves(names: list[str], *, start_index:int=1) -> list[Subsystem]: 
    return [valve(n, index=start_index+i) for i, n in enumerate(names)] 

def pressure_vessel(name, *, index=1) -> Subsystem:
    return Subsystem(name=name,
                     otype='VEPR',
                     prefix='PVS',
                     index=index
                     )

def pressure_vessels(names: list[str], *, start_index:int=1) -> list[Subsystem]: 
    return [pressure_vessel(n, index=start_index+i) for i, n in enumerate(names)] 

def vibrator(name, *, index=1) -> Subsystem:
    return Subsystem(name=name,
                     otype='VIBR',
                     prefix='VIB',
                     index=index
                     )

def vibrators(names: list[str], *, start_index:int=1) -> list[Subsystem]: 
    return [vibrator(n, index=start_index+i) for i, n in enumerate(names)] 

def well(name, *, index=1) -> Subsystem:
    return Subsystem(name=name,
                     otype='WELL',
                     prefix='WEL',
                     index=index
                     )

def wells(names: list[str], *, start_index:int=1) -> list[Subsystem]: 
    return [well(n, index=start_index+i) for i, n in enumerate(names)] 

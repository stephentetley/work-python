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

def alarm(*, name, index=1) -> Subsystem:
    return Subsystem(name=name,
                     otype='ALAM',
                     prefix='ALM',
                     index=index
                     )

def kiosk(*, name='Kiosk', index=1) -> Subsystem:
    return Subsystem(name=name,
                     otype='KISK',
                     prefix='KIS',
                     index=index
                     )

def lighting(*, name, index=1) -> Subsystem:
    return Subsystem(name=name,
                     otype='LIDE',
                     prefix='LGT',
                     index=index
                     )

def pump(*, name, index=1) -> Subsystem:
    return Subsystem(name=name,
                     otype='PUMP',
                     prefix='PMP',
                     index=index
                     )

def tank(*, name, index=1) -> Subsystem:
    return Subsystem(name=name,
                     otype='TANK',
                     prefix='TNK',
                     index=index
                     )

def valve(*, name, index=1) -> Subsystem:
    return Subsystem(name=name,
                     otype='VALV',
                     prefix='VAL',
                     index=index
                     )

def well(*, name, index=1) -> Subsystem:
    return Subsystem(name=name,
                     otype='WELL',
                     prefix='WEL',
                     index=index
                     )
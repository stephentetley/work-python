"""
Copyright 2023 Stephen Tetley

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
import re

class EastNorth:
    def __init__(self, *, easting:int, northing:int):
        self.easting = easting
        self.northing = northing

    def to_osgb36(easting : int, northing: int) -> str: 
        try:
            major_char = _find_major(easting, northing)
            minor_char = _find_minor(easting % 500000, northing % 500000)
            small_e = easting % 100000
            small_n = northing % 100000
            return print(f'{major_char}{minor_char}{small_e:05}{small_n:05}')
        except (TypeError, ValueError) as exn: 
            print(exn)
            return None

def _find_major(e: int, n: int) -> str:
    if   e >= 0       and e < 500_000   and n >= 0         and n < 500_000:
        return 'S'
    elif e >= 500_000 and e < 1_000_000 and n >= 0         and n < 500_000:
        return 'T'
    elif e >= 0       and e < 500_000   and n >= 500_000   and n < 1_000_000:
         return 'N'
    elif e >= 500_000 and e < 1_000_000 and n >= 500_000   and n < 1_000_000:
        return 'O'
    elif e >= 0       and e < 500_000   and n >= 1_000_000 and n < 1_500_000:
        return 'H'
    elif e >= 500_000 and e < 1_000_000 and n >= 1_000_000 and n < 1_500_000:
        return 'J'
    else:
        None 

def _find_minor(e: int, n: int) -> str:
    if   e >= 0       and e < 100_000     and n >= 0       and n < 100_000: 
        return 'V'
    elif e >= 100_000 and e < 200_000     and n >= 0       and n < 100_000:
        return 'W'
    elif e >= 200_000 and e < 300_000     and n >= 0       and n < 100_000:
        return 'X'
    elif e >= 300_000 and e < 400_000     and n >= 0       and n < 100_000:
        return 'Y'
    elif e >= 400_000 and e < 500_000     and n >= 0       and n < 100_000:
        return 'Z'
    elif e >= 0       and e < 100_000     and n >= 100_000 and n < 200_000:
        return 'Q'
    elif e >= 100_000 and e < 200_000     and n >= 100_000 and n < 200_000:
        return 'R'
    elif e >= 200_000 and e < 300_000     and n >= 100_000 and n < 200_000:
        return 'S'
    elif e >= 300_000 and e < 400_000     and n >= 100_000 and n < 200_000:
        return 'T'
    elif e >= 400_000 and e < 500_000     and n >= 100_000 and n < 200_000:
        return 'U'
    elif e >= 0       and e < 100_000     and n >= 200_000 and n < 300_000:
        return 'L'
    elif e >= 100_000 and e < 200_000     and n >= 200_000 and n < 300_000:
        return 'M'
    elif e >= 200_000 and e < 300_000     and n >= 200_000 and n < 300_000:
        return 'N'
    elif e >= 300_000 and e < 400_000     and n >= 200_000 and n < 300_000:
        return 'O'
    elif e >= 400_000 and e < 500_000     and n >= 200_000 and n < 300_000:
        return 'P'
    elif e >= 0       and e < 100_000     and n >= 300_000 and n < 400_000:
        return 'F'
    elif e >= 100_000 and e < 200_000     and n >= 300_000 and n < 400_000:
        return 'G'
    elif e >= 200_000 and e < 300_000     and n >= 300_000 and n < 400_000:
        return 'H'
    elif e >= 300_000 and e < 400_000     and n >= 300_000 and n < 400_000:
        return 'J'
    elif e >= 400_000 and e < 500_000     and n >= 300_000 and n < 400_000:
        return 'K'
    elif e >= 0       and e < 100_000     and n >= 400_000 and n < 500_000:
        return 'A'
    elif e >= 100_000 and e < 200_000     and n >= 400_000 and n < 500_000:
        return 'B'
    elif e >= 200_000 and e < 300_000     and n >= 400_000 and n < 500_000:
        return 'C'
    elif e >= 300_000 and e < 400_000     and n >= 400_000 and n < 500_000:
        return 'D'
    elif e >= 400_000 and e < 500_000     and n >= 400_000 and n < 500_000:
        return 'E'
    else:
        None


class Osgb36():
    regex = re.compile(r'[HJNOST][A-Z]\d{10}')

    def __init__(self, grid_ref: str):
        if Osgb36.regex.match(grid_ref):
            self.grid_ref = grid_ref
        else:
            # print(f"bad: {grid_ref}")
            None

    def to_east_north(self) -> EastNorth | None:
        try: 
            major = _decode_major(self.grid_ref[0])
            minor = _decode_minor(self.grid_ref[1])
            major_e, major_n = major
            minor_e, minor_n = minor
            east1 = int(self.grid_ref[2:7])
            north1 = int(self.grid_ref[7:12])
            return EastNorth(easting=major_e + minor_e + east1, northing=major_n + minor_n + north1)
        except (TypeError, ValueError) as exn: 
            return None
    
def _decode_major(c):
    cu = c.upper()
    if cu == 'S':
        return (0,       0)
    elif cu == 'T':
        return (500_000, 0)
    elif cu == 'N': 
        return(0,        500_000)
    elif cu == 'O':
        return (500_000, 500_000)
    elif cu == 'H':
        return (0,       1_000_000)
    else:
        None

def _decode_minor(c):
    cu = c.upper()
    if cu == 'A':
        return (0,       400_000)
    elif cu == 'B':
        return (100_000, 400_000)
    elif cu == 'C':
        return (200_000, 400_000)
    elif cu == 'D':
        return (300_000, 400_000)
    elif cu == 'E':
        return (400_000, 400_000)
    elif cu == 'F':
        return (0,       300_000)
    elif cu == 'G':
        return (100_000, 300_000)
    elif cu == 'H':
        return (200_000, 300_000)
    elif cu == 'J':
        return (300_000, 300_000)
    elif cu == 'K':
        return (400_000, 300_000)
    elif cu == 'L':
        return (0,       200_000)
    elif cu == 'M':
        return (100_000, 200_000)
    elif cu == 'N':
        return (200_000, 200_000)
    elif cu == 'O':
        return (300_000, 200_000)
    elif cu == 'P':
        return (400_000, 200_000)
    elif cu == 'Q':
        return (0,       100_000)
    elif cu == 'R':
        return (100_000, 100_000)
    elif cu == 'S':
        return (200_000, 100_000)
    elif cu == 'T':
        return (300_000, 100_000)
    elif cu == 'U':
        return (400_000, 100_000)
    elif cu == 'V':
        return (0,       0)
    elif cu == 'W':
        return (100_000, 0)
    elif cu == 'X':
        return (200_000, 0)
    elif cu == 'Y':
        return (300_000, 0)
    elif cu == 'Z':
        return (400_000, 0)
    else:
        None

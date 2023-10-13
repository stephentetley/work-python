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

def toOSGB36(easting, northing): 
    try:
        print("A1")
        majorChar = findMajor(easting, northing)
        print("A2: "  + majorChar)
        minorChar = findMinor(easting % 500000, northing % 500000)
        print("A3: " + minorChar)
        smallE = easting % 100000
        print("A4: " + str(smallE))
        smallN = northing % 100000
        print("A5: " + str(smallN))
        return print(f'{majorChar}{minorChar}{smallE:05}{smallN:05}')
    except (TypeError, ValueError) as exn: 
        print(exn)
        return None

def findMajor(e, n):
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

def findMinor(e, n):
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

def toEastNorth(gridref):
    try: 
        major = decodeMajor(gridref[0])
        minor = decodeMinor(gridref[1])
        majorE, majorN = major
        minorE, minorN = minor
        east1 = int(gridref[2:7])
        north1 = int(gridref[7:12])
        return {"easting": majorE + minorE + east1, "northing": majorN + minorN + north1}
    except (TypeError, ValueError) as exn: 
        return None
    
def decodeMajor(c):
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

def decodeMinor(c):
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

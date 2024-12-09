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

import datetime
from datetime import date
from dataclasses import dataclass

@dataclass
class Context:
    easting: int = 0
    northing: int = 0
    startup_date: date = datetime.date(1970, 1, 1)
    sai_reference: str = ''
    
def merge(outer: Context, inner: Context) -> Context:
    Context(easting= outer.easting if inner.easting == 0 else inner.easting, 
            northing= outer.northing if inner.northing == 0 else inner.northing, 
            startup_date= outer.startup_date if inner.startup_date == datetime.date(1970, 1, 1) else inner.startup_date,
            sai_reference= outer.sai_reference if inner.sai_reference == '' else inner.sai_reference
            )

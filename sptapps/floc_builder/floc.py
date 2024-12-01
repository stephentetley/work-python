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
from datetime import date

class Floc:
    def __init__(self, 
                 *, 
                 function_location: str, 
                 description: str,
                 object_type: str, 
                 startup_date: date=date.today(),
                 structure_indicator: str='YW-GS',
                 status: str='OPER') -> None:
        self.function_location = function_location
        self.description = description
        self.structure_indicator = structure_indicator
        self.otype = object_type
        self.startup_date = startup_date
        self.status = status

    @property
    def construction_year(self) -> int:
        return self.startup_date.year
    
    @property
    def construction_month(self) -> int:
        return self.startup_date.month
    
    @property
    def category(self) -> int:
        return 1 + self.function_location.count('-')
        
    @property
    def superior_function_location(self) -> int:
        end = self.function_location.rfind('-')
        return self.function_location[0:end]
    
    @property
    def equipment_install(self) -> int:
        return self.category >=5
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

class FlocClassification:
    def __init__(self, 
                 *, 
                 function_location: str, 
                 class_name: str,
                 characteristic_name: str, 
                 characteristic_value: str) -> None:
        self.function_location = function_location
        self.class_name = class_name
        self.characteristic_name = characteristic_name
        self.characteristic_value = characteristic_value

    def __repr__(self): 
        s1 = self.startup_date.strftime('%d.%m.%Y')
        return f'FlocClassification({self.function_location}, {self.class_name}, {self.characteristic_name}, {self.characteristic_value})'
    
    @property
    def class_type(self) -> str:
        return '003'
    
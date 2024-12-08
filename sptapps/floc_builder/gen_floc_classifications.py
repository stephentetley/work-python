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
from sptapps.floc_builder.floc import Floc
from sptapps.floc_builder.floc_classification import FlocClassification

class GenFlocClassifications:
    def __init__(self, flocs: list[Floc]) -> None:
        self.flocs = flocs
        self.chars = set()
        
    def gen_floc_classifications(self) -> list[FlocClassification]:
        for x in self.flocs:
            self._add_aib_reference(x)
            self._add_east_north(x)
            self._add_sys_classification(x)
            self._add_uniclass(x)

        ls = list(self.chars)
        ls.sort(key=lambda x: f'{x.functional_location.ljust(30, "-")}{x.class_name}')
        return ls
    
    def _add_aib_reference(self, floc: Floc) -> Self:
        char = FlocClassification(functional_location=floc.functional_location,
                                  class_name='AIB_REFERENCE', 
                                  characteristic_name='AI2_AIB_REFERENCE',
                                  characteristic_value='')
        self.chars.add(char)
        return self

    def _add_east_north(self, floc: Floc) -> Self:
        easting = FlocClassification(functional_location=floc.functional_location,
                                     class_name='EAST_NORTH', 
                                     characteristic_name='EASTING',
                                     characteristic_value='0')
        self.chars.add(easting)
        northing = FlocClassification(functional_location=floc.functional_location,
                                      class_name='EAST_NORTH', 
                                      characteristic_name='NORTHING',
                                      characteristic_value='0')
        self.chars.add(northing)
        return self
    
    def _add_sys_classification(self, floc: Floc) -> Self:
        if floc.category == 5 and floc.class_type:
            char = FlocClassification(functional_location=floc.functional_location,
                                      class_name=floc.class_type, 
                                      characteristic_name='SYSTEM_TYPE',
                                      characteristic_value='TODO')
            self.chars.add(char)
        return self

    def _add_uniclass(self, floc: Floc) -> Self:
        if floc.category != 5:
            ucode = FlocClassification(functional_location=floc.functional_location,
                                       class_name='UNICLASS_CODE', 
                                       characteristic_name='UNICLASS_CODE',
                                       characteristic_value='')
            self.chars.add(ucode)
            udesc = FlocClassification(functional_location=floc.functional_location,
                                       class_name='UNICLASS_CODE', 
                                       characteristic_name='UNICLASS_DESC',
                                       characteristic_value='')
            self.chars.add(udesc)
        return self

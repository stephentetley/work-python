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
            self._add_sys_classification(x)

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
    
    def _add_sys_classification(self, floc: Floc) -> Self:
        if floc.category == 5 and floc.class_type:
            char = FlocClassification(functional_location=floc.functional_location,
                                      class_name=floc.class_type, 
                                      characteristic_name='SYSTEM_TYPE',
                                      characteristic_value='TODO'
                                      )
            self.chars.add(char)
        return self

    # def _add_function_floc(self, sys: System) -> Self:
    #     otype = sys.path[0:3]
    #     descr = self.get_description(otype)
    #     floc = Floc(functional_location=f'{self.site.siteid}-{otype}',
    #                 description=descr,
    #                 startup_date=self.startup_date,
    #                 object_type=otype,
    #                 structure_indicator='YW-GS', 
    #                 status='OPER')
    #     self.flocs.add(floc)
    #     return self
    
    # def _add_process_group_floc(self, sys: System) -> Self:
    #     otype = sys.path[4:7]
    #     descr = self.get_description(otype)
    #     floc = Floc(functional_location=f'{self.site.siteid}-{sys.path[0:7]}',
    #                 description=descr,
    #                 startup_date=self.startup_date,
    #                 object_type=otype,
    #                 structure_indicator='YW-GS', 
    #                 status='OPER')
    #     self.flocs.add(floc)
    #     return self

    # def _add_process_floc(self, sys: System) -> Self:
    #     otype = sys.path[8:11]
    #     descr = self.get_description(otype)
    #     floc = Floc(functional_location=f'{self.site.siteid}-{sys.path}',
    #                 description=descr,
    #                 startup_date=self.startup_date,
    #                 object_type=otype,
    #                 structure_indicator='YW-GS', 
    #                 status='OPER')
    #     self.flocs.add(floc)
    #     return self
    
    # def _add_sys_floc(self, sys: System) -> Self:
    #     floc = Floc(functional_location=f'{self.site.siteid}-{sys.key}',
    #                 description=sys.name,
    #                 startup_date=self.startup_date,
    #                 object_type=sys.otype,
    #                 structure_indicator='YW-GS', 
    #                 status='OPER', 
    #                 class_type=sys.ctype)
    #     self.flocs.add(floc)
    #     return self
        
    # def _add_subsystem_flocs(self, sys:System) -> Self:
    #     for ss in sys.subsystems.values():
    #         self._add_subsystem_floc(ss, sys.key)

    # def _add_subsystem_floc(self, subsys:Subsystem, path: str) -> Self:
    #     floc = Floc(functional_location=f'{self.site.siteid}-{path}-{subsys.key}',
    #                 description=subsys.name,
    #                 object_type=subsys.otype,
    #                 startup_date=self.startup_date,
    #                 structure_indicator='YW-GS', 
    #                 status='OPER')
    #     self.flocs.add(floc)
    #     return self


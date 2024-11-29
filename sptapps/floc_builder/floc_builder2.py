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

# Can we have dictionaries for levels making remove easier?

class Level:
    def __init__(self, *, name: str, components: dict[str, Self]) -> None:
        self.name = name
        self.components: dict[str, Self] = components

    def __repr__(self): 
        s1 = repr(self.components)
        return f'Level({self.name}, {s1})'

def telemetry_system() -> dict[str, Level]: 
    return {'CAA': Level(
        name='Control and Automation',
        components={'NET': Level(
            name='Networks',
            components={'TEL': Level(
                name='Telemetry',
                components={'SYS01': Level(
                    name='Telemetry System',
                    components={}
                )})
            })
        }
    )}

def fixed_lifing_system() -> dict[str, Level]: 
    return {'SMS': Level(
        name='Site Maintenance Services',
        components={'LFT': Level(
            name='Site Lifting',
            components={'FLT': Level(
                name='Fixed Lifting',
                components={'SYS01': Level(
                    name='Fixed Lifting System',
                    components={}
                )})
            })
        }
    )}

def portable_lifing_system() -> dict[str, Level]: 
    return {'SMS': Level(
        name='Site Maintenance Services',
        components={'LFT': Level(
            name='Site Lifting',
            components={'PRL': Level(
                name='Portable Lifting',
                components={'SYS01': Level(
                    name='Portable Lifting System',
                    components={}
                )})
            })
        }
    )}

        # ).add_process_group(ProcessGroup(
        #     id='NET', 
        #     name='Networks'
        #     ).add_process(Process(
        #         id='TEL', 
        #         name='Telemetry'
        #         ).add_system(System(
        #             index=1, 
        #             name='Telemetry System')))
        #     )


# class Subystem:
#     def __init__(self, *, id: str, name: str) -> None:
#         self.name = name
#         self.id = id
        

#     def output(self, *, prefix: str) -> None: 
#         print(f"{prefix}-{self.id} {self.name}")
        

# class System:
#     # todo - auto deduce index by ordering...
#     def __init__(self, *, index: str, name: str) -> None:
#         self.name = name
#         self.id = f'SYS{index:02}'
#         self.subsystems = []
    

#     def output(self, *, prefix: str) -> None: 
#         print(f"{prefix}-{self.id} {self.name}")
#         for x in self.subsystems:
#             x.output(prefix=f"{prefix}-{self.id}")

# class Process:
#     def __init__(self, *, id: str, name: str) -> None:
#         self.name = name
#         self.id = id
#         self.systems: list[System] = []
    
#     def add_system(self, x: type[System]) -> Self: 
#         self.systems.append(x)
#         return self
    

#     def output(self, *, prefix: str) -> None: 
#         print(f"{prefix}-{self.id} {self.name}")
#         for x in self.systems:
#             x.output(prefix=f"{prefix}-{self.id}")


# class ProcessGroup:
#     def __init__(self, *, id: str, name: str) -> None:
#         self.name = name
#         self.id = id
#         self.processes: list[Process] = []
    
#     def add_process(self, p: type[Process]) -> Self: 
#         self.processes.append(p)
#         return self

#     def output(self, *, prefix: str) -> None: 
#         print(f"{prefix}-{self.id} {self.name}")
#         for x in self.processes:
#             x.output(prefix=f"{prefix}-{self.id}")

# class Function:
#     def __init__(self, *, id: str, name: str) -> None:
#         self.name = name
#         self.id = id
#         self.process_groups: list[ProcessGroup] = []

#     def add_process_group(self, pg: type[ProcessGroup]) -> Self: 
#         self.process_groups.append(pg)
#         return self
    
#     def output(self, *, prefix: str) -> None: 
#         print(f"{prefix}-{self.id} {self.name}")
#         for x in self.process_groups:
#             x.output(prefix=f"{prefix}-{self.id}")

# class Function:
#     def __init__(self, *, id: str, name: str) -> None:
#         self.name = name
#         self.id = id
#         self.process_groups = []

#     def add_process_group(self, pg: type[ProcessGroup]) -> Self: 
#         self.process_groups.append(pg)
#         return self
    
#     def output(self, *, prefix: str) -> None: 
#         print(f"{prefix}-{self.id} {self.name}")
#         for x in self.process_groups:
#             x.output(prefix=f"{prefix}-{self.id}")

# class Site:
#     def __init__(self, *, id: str, name: str) -> None:
#         self.name = name
#         self.id = id
#         self.functions = []

#     def with_telemetry(self) -> Self:
#         self.functions.append(telemetry())
#         return self
    
#     def output(self) -> None: 
#         print(f"{self.id} {self.name}")
#         for x in self.functions:
#             x.output(prefix=self.id)


# def telemetry() -> Function: 
#     return Function(
#         id='CAA', 
#         name='Control and Automation'
#         ).add_process_group(ProcessGroup(
#             id='NET', 
#             name='Networks'
#             ).add_process(Process(
#                 id='TEL', 
#                 name='Telemetry'
#                 ).add_system(System(
#                     index=1, 
#                     name='Telemetry System')))
#             )


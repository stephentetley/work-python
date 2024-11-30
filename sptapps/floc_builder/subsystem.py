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


class Subsystem:
    def __init__(self, *, name: str, otype: str, prefix: str, index:int=1) -> None:
        self.name = name
        self.index = index
        self.otype = otype
        self.prefix = prefix
        self.structure_indicator = 'YW-GS'
        self.status = 'OPER'

    def __repr__(self): 
        return f'Subsystem({self.name}, {self.ty}, {self.status})'


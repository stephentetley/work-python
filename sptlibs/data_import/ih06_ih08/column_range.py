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


class ColumnRange:
    def __init__(self, *, range_name: str, start: int) -> None:
        self.range_name = range_name
        self.range_start = start
        self.range_end = start + 1

    def __str__(self) -> str:
        return 'ColumnRange `%s` start:%d end:%d' % (self.range_name, self.range_start, self.range_end)



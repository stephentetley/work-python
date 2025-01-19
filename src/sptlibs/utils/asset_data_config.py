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

import os


class AssetDataConfig:
    def __init__(self) -> None:
        """Defaults to $HOME if $ASSET_DATA_FACT_ROOT_DIR is missing."""
        self.facts_path = os.environ.get('ASSET_DATA_FACT_ROOT_DIR', '')
        if not self.facts_path:
            self.facts_path = os.path.expanduser("~")
        if not os.path.exists(self.facts_path): 
            print(f"Cannot find facts root directory: `{self.facts_path}`")
            self.facts_path = None
        
    def get_classlists_db(self) -> str | None:
        if self.facts_path:
            return os.path.normpath(os.path.join(self.facts_path, 's4_classlist_latest.duckdb'))
        else: 
            return None
    
    def get_ztables_db(self) -> str | None:
        if self.facts_path:
            return os.path.normpath(os.path.join(self.facts_path, 's4_ztables_latest.duckdb'))
        else: 
            return None




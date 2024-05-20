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
import tomllib
import typing

# Class?
class AssetDataConfig:
    def __init__(self) -> None:
        """Defaults to $HOME if $ASSET_DATA_CONFIG_DIR is missing."""
        self.config = None
        self.focus = None
        config_dir = os.environ.get('ASSET_DATA_CONFIG_DIR', '')
        if not config_dir:
            config_dir = os.path.expanduser("~")
        config_path = os.path.normpath(os.path.join(config_dir, 'py_asset_data.toml'))
        if os.path.exists(config_path): 
            with open(config_path) as fileObj:
                txt = fileObj.read()
                self.config = tomllib.loads(txt)
        else:
            print("Cannot find `py_asset_data.toml`")
            
    def set_focus(self, group_name: str) -> None:
            self.focus = self.config.get(group_name, None)


    def get(self, field_name: str, alt: typing.Any) -> str | typing.Any:
        return self.focus.get(field_name, None)
        
    def get_expanded_path(self, field_name: str) -> str | None:
        path = self.focus.get(field_name, None)
        if path:
            return os.path.normpath(os.path.expandvars(path))
        else: 
            return None




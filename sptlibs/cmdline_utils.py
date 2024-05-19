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

# Class?

def get_asset_data_config() -> dict | None:
    """Deafults to $HOME if $ASSET_DATA_CONFIG_DIR is missing."""
    config_dir = os.environ.get('ASSET_DATA_CONFIG_DIR', '')
    if not config_dir:
        config_dir = os.path.expanduser("~")
    config_path = os.path.normpath(os.path.join(config_dir, 'py_asset_data.toml'))
    if os.path.exists(config_path): 
        with open(config_path) as fileObj:
            txt = fileObj.read()
            config = tomllib.loads(txt)
        return config
    else:
        print("Cannot find `py_asset_data.toml`")
        return None

def get_expanded_path(field_name: str, config: dict) -> str | None:
    path = config.get(field_name, None)
    if path:
        return os.path.normpath(os.path.expandvars(path))
    else: 
        return None




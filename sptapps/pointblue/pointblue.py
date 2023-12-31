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

import argparse
import json
import sys

## Maybe it is too annoying to read from Json and we should just edit `run_point_blue.py`...

def parse_cmdline():
    parser = argparse.ArgumentParser(
            prog='point-blue-updates',
            description='Generate Point Blue update report',
            epilog='')

    parser.add_argument('filename') 
    args = parser.parse_args()
    return args


def load_config(path): 
    json.load(path)


def main() -> int:
    args = parse_cmdline()
    try:
        print(f'filename: {args.filename}')
        config_path = args.filename
        with open(config_path, 'r') as fp:
            config = json.load(fp)
        print(config['output_directory'])
        return 0
    except AttributeError as exn:
        print(f'Attribute not specified: {exn.name}')
        return 1

if __name__ == '__main__':
    sys.exit(main())

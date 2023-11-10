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

def _notEmpty1(x): 
    if x == None: 
        return False
    elif x == '':
        return False
    else: 
        return True

def _strip_nulls(ls): 
    return list(filter(_notEmpty1, ls))

def pp_value(jvalue) -> str:
    '''Expects scalars or arrays of scalars. Strings unquoted, len(1) arrays printed as singletons.'''
    if jvalue == None:
        return ''
    elif isinstance(jvalue, str): 
        return jvalue
    elif isinstance(jvalue, list):
        xs = _strip_nulls(jvalue)
        xs.sort()
        return ', '.join(map(pp_value, xs))
    else:
        return str(jvalue)
    

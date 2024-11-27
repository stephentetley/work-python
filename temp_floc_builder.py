# temp_floc_builder.py


import sptapps.floc_builder.floc_builder as floc_builder

def sps_with_outstation(*, name: str, id: str) -> floc_builder.Site:
    return floc_builder.Site(name = name, id = id).with_telemetry()

boo01 = sps_with_outstation(name='Boomtown', id='BOO01')

boo01.output()


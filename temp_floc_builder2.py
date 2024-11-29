# temp_floc_builder.py


import sptapps.floc_builder.floc_builder2 as floc_builder2



boo01 = floc_builder2.telemetry_system()
boo01.update(floc_builder2.portable_lifing_system())
boo01.update(floc_builder2.fixed_lifing_system())

print(boo01)


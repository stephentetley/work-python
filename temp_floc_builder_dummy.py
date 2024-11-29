# temp_floc_builder.py


import sptapps.floc_builder.dummy.floc_builder1 as floc_builder1
import sptapps.floc_builder.dummy.floc_builder2 as floc_builder2
import sptapps.floc_builder.dummy.floc_builder3 as floc_builder3

# floc_builder1 
def sps_with_outstation(*, name: str, id: str) -> floc_builder1.Site:
    return floc_builder1.Site(name = name, id = id).with_telemetry()

boo01 = sps_with_outstation(name='Boomtown', id='BOO01')

boo01.output()

# floc_builder2 
boo02 = floc_builder2.telemetry_system()
boo02.update(floc_builder2.portable_lifing_system())
boo02.update(floc_builder2.fixed_lifing_system())

print(boo02)

# floc_builder3
boo03 = floc_builder3.Site().site_name('Boomtown')

boo03.site_id('BOO01').add_system(floc_builder3.telemetry_system().set_name('Spill Point Outstation System'))
boo03.add_system(floc_builder3.portable_lifting_system())
boo03.add_system(floc_builder3.fixed_lifting_system())
boo03.add_system(floc_builder3.edc_outfall_system())

print(boo03)
# temp_floc_builder.py


import sptapps.floc_builder.floc_builder3 as floc_builder3



boo01 = floc_builder3.Site().site_name('Boomtown')

boo01.site_id('BOO01').add_system(floc_builder3.telemetry_system().set_name('Spill Point Outstation System'))
boo01.add_system(floc_builder3.portable_lifting_system())
boo01.add_system(floc_builder3.fixed_lifting_system())
boo01.add_system(floc_builder3.edc_outfall_system())


print(boo01)


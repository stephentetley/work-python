# temp_floc_builder.py

import sptapps.floc_builder.catalogue as catalogue
from sptapps.floc_builder.site import Site


# floc_builder3
boo03 = Site().site_name('Boomtown')

boo03.site_id('BOO01').add_system(catalogue.telemetry_system())
boo03.add_system(catalogue.portable_lifting_system())
boo03.add_system(catalogue.fixed_lifting_system())
boo03.add_system(catalogue.edc_outfall_system())


kisk01 = catalogue.kiosk(index=1)
kisk02 = catalogue.kiosk(index=2)
boo03.add_system(catalogue.kiosk_system(subsystems=[kisk01, kisk02]))
print(boo03)
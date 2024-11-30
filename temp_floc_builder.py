# temp_floc_builder.py

import sptapps.floc_builder.systems as systems
import sptapps.floc_builder.subsystems as subsystems
from sptapps.floc_builder.site import Site


# floc_builder3
boo03 = Site().site_name('Boomtown')

boo03.site_id('BOO01').add_system(systems.telemetry())
boo03.add_system(systems.portable_lifting())
boo03.add_system(systems.fixed_lifting())
boo03.add_system(systems.edc_outfall())


kisk01 = subsystems.kiosk(index=1)
kisk02 = subsystems.kiosk(index=2)
boo03.add_system(systems.kiosk(subsystems=[kisk01, kisk02]))
print(boo03)
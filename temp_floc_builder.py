# temp_floc_builder.py

import sptapps.floc_builder.systems as sys
import sptapps.floc_builder.subsystems as subsys
from sptapps.floc_builder.site import Site
from sptapps.floc_builder.floc import Floc

# floc_builder3
boo03 = Site().site_name('Boomtown')

boo03.site_id('BOO01').add_system(sys.telemetry(index=2))
boo03.add_system(sys.portable_lifting())
boo03.add_system(sys.fixed_lifting())
boo03.add_system(sys.edc_outfall())
boo03.add_system(sys.kiosk(subsystems=subsys.kiosks(['Kiosk-1', 'Kiosk-2'])))
print(boo03)

floc01 = Floc(function_location='BOO01-CAA-NET-TEL-SYS01',
              description='Telemetry System',
              structure_indicator='YW-GS',
              object_type='CTOS')

print(floc01)
print(floc01.superior_function_location)
print(floc01.equipment_install)

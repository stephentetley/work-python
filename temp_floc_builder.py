# temp_floc_builder.py

import datetime as datetime
import sptapps.floc_builder.systems as sys
import sptapps.floc_builder.subsystems as subsys
from sptapps.floc_builder.site import Site
from sptapps.floc_builder.context import Context
from sptapps.floc_builder.gen_flocs import GenFlocs
from sptapps.floc_builder.gen_floc_classifications import GenFlocClassifications


# floc_builder3
cx = Context(easting=47600, northing=56400, 
             startup_date=datetime.date(2023, 11, 1))

cx1 = cx.with_east_north(easting=5, northing=10).with_solution_ids(["001256789"])
print(cx)
print(cx1)

boo03 = Site().site_name('Boomtown').context(cx)

boo03.site_id('BOO01').add_system(sys.telemetry(index=1))
boo03.add_system(sys.portable_lifting())
boo03.add_system(sys.fixed_lifting())
boo03.add_system(sys.edc_outfall())
boo03.add_system(sys.kiosk(subsystems=subsys.kiosks(['Kiosk-1', 'Kiosk-2'])))
print(boo03)


gf = GenFlocs(source_path='g:/work/2024/asset_data_facts/s4_ztables/zte_0343_flocdes.XLSX', 
              site=boo03)

flocs = gf.gen_flocs()
chars = GenFlocClassifications(flocs=flocs).gen_floc_classifications()

for x in flocs:
    print(x)

for x in chars:
    print(x)


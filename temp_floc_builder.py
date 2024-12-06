# temp_floc_builder.py

from datetime import date
import polars as pl
import sptapps.floc_builder.systems as sys
import sptapps.floc_builder.subsystems as subsys
from sptapps.floc_builder.site import Site
from sptapps.floc_builder.floc import Floc
from sptapps.floc_builder.gen_flocs import GenFlocs


# floc_builder3
boo03 = Site().site_name('Boomtown')

boo03.site_id('BOO01').add_system(sys.telemetry(index=2))
boo03.add_system(sys.portable_lifting())
boo03.add_system(sys.fixed_lifting())
boo03.add_system(sys.edc_outfall())
boo03.add_system(sys.kiosk(subsystems=subsys.kiosks(['Kiosk-1', 'Kiosk-2'])))
print(boo03)


gf = GenFlocs(source_path='g:/work/2024/asset_data_facts/s4_ztables/zte_0343_flocdes.XLSX', 
              site=boo03,
              startup_date=date.today())

flocs = gf.gen_flocs()

for x in flocs:
    print(x)


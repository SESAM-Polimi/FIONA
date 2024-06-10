#%%
from core.db_builder import DB_builder

# sut_path = 'tests/conceptual/test_SUT.xlsx'
# sut_format = 'xlsx'
# sut_mode = 'coefficients'
# master_file_path = 'tests/conceptual/master.xlsx'

sut_path = 'tests/exiobase/coefficients'
sut_format = 'txt'
sut_mode = 'coefficients'
master_file_path = 'tests/exiobase/master.xlsx'

db = DB_builder(
    sut_path=sut_path,
    sut_mode=sut_mode,
    master_file_path=master_file_path,
    sut_format=sut_format,
    read_master_file=True,
)

#%%
db.read_master_template(master_file_path)

#%%
db.read_inventories(master_file_path,check_errors=True)

#%%
db.add_inventories('excel')

# %%

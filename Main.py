#%%
from fiona.core.db_builder import DB_builder

sut_path = '/Users/lorenzorinaldi/Library/CloudStorage/OneDrive-SharedLibraries-PolitecnicodiMilano/Gabriele Casella - Lorenzo_ROSSI/Exiobase Hybrid 3.3.18 with VA/flows'
sut_format = 'txt'
sut_mode = 'flows'
master_file_path = 'master.xlsx'

db = DB_builder(
    sut_path=sut_path,
    sut_mode=sut_mode,
    master_file_path=master_file_path,
    sut_format=sut_format,
    read_master_file=True,
)

#%%
db.read_master_template(master_file_path,get_inventories=True)

#%%
db.read_inventories(master_file_path,check_errors=False)

#%%
db.add_inventories('excel')

# %%

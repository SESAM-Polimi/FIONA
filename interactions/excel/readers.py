import pandas as pd
from mario.tools.constants import _MASTER_INDEX as MI

def read_fiona_master_template(path,master_name,reg_map_name):
    
    master_file = pd.read_excel(path,sheet_name=None,header=0,)
    master_sheet = master_file[master_name]
    regions_maps_sheet = master_file[reg_map_name]

    return master_sheet, regions_maps_sheet

def read_fiona_inventory_templates(instance,path):

    inventories = pd.read_excel(path,sheet_name=None,header=0,)
    keys = list(inventories.keys())

    for i in keys:
        if i not in instance.master_sheet['Sheet name'].unique():
            del inventories[i] # drop all sheets that don't contain inventory data
        else:
            inventories[instance.master_sheet.query(f'`Sheet name` == "{i}"')[MI['a']].values[0]] = inventories.pop(i) # rename key from sheet to activity name   
    
    return inventories

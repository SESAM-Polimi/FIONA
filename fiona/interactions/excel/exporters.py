import pandas as pd

from mario.tools.constants import _MASTER_INDEX as MI

def get_fiona_master_template(
        instance,
        master_name,
        master_columns,
        reg_map_name,
        reg_map_columns,
        path
    ):

    master_sheet = pd.DataFrame(columns=master_columns)
    regions_maps_sheet = pd.DataFrame(instance.sut.get_index(MI['r']), columns=reg_map_columns) 

    with pd.ExcelWriter(path) as writer:
        master_sheet.to_excel(writer, sheet_name=master_name, index=False)
        regions_maps_sheet.to_excel(writer, sheet_name=reg_map_name, index=False)

    # add data validation...

def get_fiona_inventory_templates(
        new_sheets,
        units,
        inv_columns,
        overwrite,
        path
    ):

    inventory_sheet = pd.DataFrame(columns=inv_columns)
    units_sheet = pd.DataFrame()
    for item,unit in units.items():

        unit_index = pd.MultiIndex.from_arrays([[item for i in range(unit.shape[0])],list(unit.index)])
        unit.index = unit_index
        units_sheet = pd.concat([units_sheet,unit],axis=0)
    
    if overwrite:
        mode = 'replace'
    else:
        mode = 'error'

    with pd.ExcelWriter(path, mode='a', engine='openpyxl', if_sheet_exists=mode) as writer:
        for sheet in new_sheets:
            inventory_sheet.to_excel(writer, sheet_name=sheet, index=False)
        units_sheet.to_excel(writer, sheet_name='DB units',merge_cells=False)
    

    # add data validation...


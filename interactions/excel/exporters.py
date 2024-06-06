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
        inv_columns,
        overwrite,
        path
    ):

    inventory_sheet = pd.DataFrame(columns=inv_columns)
    if overwrite:
        mode = 'replace'
    else:
        mode = 'error'

    with pd.ExcelWriter(path, mode='a', engine='openpyxl', if_sheet_exists=mode) as writer:
        for sheet in new_sheets:
            inventory_sheet.to_excel(writer, sheet_name=sheet, index=False)

    # add data validation...


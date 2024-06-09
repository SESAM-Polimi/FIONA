import pandas as pd
import pint
from mario.tools.constants import _MASTER_INDEX as MI

def read_fiona_master_template(instance,path,master_name,reg_map_name):
    
    master_file = pd.read_excel(path,sheet_name=None,header=0)
    master_sheet = master_file[master_name]

    regions_maps = {k:master_file[reg_map_name][k].dropna().to_list() for k in master_file[reg_map_name].columns}

    check_for_errors_in_region_maps(instance,regions_maps)
    check_for_errors_in_master_sheet(instance,master_sheet,regions_maps)

    return master_sheet, regions_maps

def read_fiona_inventory_templates(instance,path):

    inventories = pd.read_excel(path,sheet_name=None,header=0,)
    keys = list(inventories.keys())

    for i in keys:
        if i not in instance.master_sheet['Sheet name'].unique():
            del inventories[i] # drop all sheets that don't contain inventory data
        else:
            inventories[instance.master_sheet.query(f'`Sheet name` == "{i}"')[MI['a']].values[0]] = inventories.pop(i) # rename key from sheet to activity name   

    check_for_errors_in_inventories(instance,inventories,instance.regions_maps)

    return inventories


def check_for_errors_in_master_sheet(instance,master_sheet,regions_maps):

    if master_sheet.empty: # check if master sheet is empty
        raise ValueError("Master sheet is empty. Please fill it")

    # check if all regions in master sheet are allowed
    allowed_regions = instance.sut.get_index(MI['r'])
    for k in regions_maps.keys():
        allowed_regions += [k]
    err_msg = []
    for region in master_sheet[MI['r']].unique():
        if region not in allowed_regions:
            err_msg.append(region)
    if err_msg != []:
        raise ValueError(f"Error in Master excel sheet | Not allowed regions found: {err_msg}")

    # check if any FU quantity is not provided
    if master_sheet['FU quantity'].isnull().values.any():
        raise ValueError("Error in Master excel sheet | Some FU quantities are missing")
    
    # check if all FU quantities are float or int
    if not master_sheet['FU quantity'].apply(lambda x: isinstance(x,(float,int))).all():
        raise ValueError("Error in Master excel sheet | Some FU quantities are not provided as numerical values")

    # check if any FU unit is not provided
    if master_sheet['FU unit'].isnull().values.any():
        raise ValueError("Error in Master excel sheet | Some FU units are missing")
    
    # check if total outputs provided are float or int
    err_msg = []
    for i in master_sheet['Total output']:
        if not pd.isna(i):
            if not isinstance(i,(float,int)):
                err_msg.append(i)
    if err_msg != []:
        raise ValueError(f"Error in Master excel sheet | Some Total outputs are not provided as numerical values: {err_msg}")
    
    # check if all consumption categories in master sheet are allowed
    err_msg = []
    for i in master_sheet.index:
        if not pd.isna(master_sheet.loc[i,'Total output']):
            if master_sheet.loc[i,MI['n']] not in instance.sut.get_index(MI['n']):
                err_msg.append(master_sheet.loc[i,MI['n']])
    if err_msg != []:
        raise ValueError(f"Error in Master excel sheet | Not allowed consumption categories found: {err_msg}")

    # check if all parent activities in master sheet are allowed
    err_msg = []
    for i in master_sheet.index:
        if not pd.isna(master_sheet.loc[i,f'Parent {MI["a"]}']):
            if master_sheet.loc[i,f'Parent {MI["a"]}'] not in instance.sut.get_index(MI['a']):
                err_msg.append(master_sheet.loc[i,f'Parent {MI["a"]}'])
    if err_msg != []:
        raise ValueError(f"Error in Master excel sheet | Not allowed parent activities found: {err_msg}")

    # check if all sheet names are provided
    if master_sheet['Sheet name'].isnull().values.any():
        raise ValueError("Error in Master excel sheet | Missing sheet names for some activities")

    # check if leave empty column is nan or true or false
    err_msg = []
    for i in master_sheet['Leave empty']:
        if not pd.isna(i):
            if i != True and i != False:
                err_msg.append(i)
    if err_msg != []:
        raise ValueError(f"Error in Master excel sheet | Not acceptable values found for 'Leave empty': {err_msg}")

def check_for_errors_in_region_maps(instance,regions_maps):

    allowed_regions = instance.sut.get_index(MI['r'])
    # check if all regions in each regions cluster are allowed
    err_msg = {}
    for k,v in regions_maps.items():
        err_msg[k] = []
        for region in v:
            if region not in allowed_regions:
                err_msg[k] += [region]
    if any([err_msg[k] != [] for k in err_msg.keys()]):
        raise ValueError(f"Error in Region maps | Not allowed regions found: {err_msg}")

def check_for_errors_in_inventories(instance,inventories,regions_maps):

    for inventory,df in inventories.items():
        if df.empty:
            raise ValueError(f"Error in Inventory sheet for {inventory} | Empty sheet")
    
        # check if any quantity is left empty
        if df['Quantity'].isnull().values.any():
            raise ValueError(f"Error in Inventory sheet for {inventory} | Some quantities are missing")
    
        # check if any unit is left empty
        if df['Unit'].isnull().values.any():
            raise ValueError(f"Error in Inventory sheet for {inventory} | Some units are missing")
    
        # check whether any element of the 'Item' column is not in [MI['c'],MI['f'],MI['k']]
        err_msg = []
        for i in df['Item']:
            if i not in [MI['c'],MI['f'],MI['k']]:
                err_msg.append(i)
        if err_msg != []:
            raise ValueError(f"Error in Inventory sheet for {inventory} | Not allowed items found: {err_msg}")
        
        # check whether any element of the 'DB Item' column is not in instance.sut.get_index(MI['c'])   
        err_msg = []
        item = MI['c']
        for i in df.query(f"Item=='{item}'")['DB Item']:
            if i not in instance.sut.get_index(MI['c']):
                err_msg.append(i)
        if err_msg != []:
            raise ValueError(f"Error in Inventory sheet for {inventory} | Not allowed commodities found: {err_msg}")
    
        # check whether any element of the 'DB Item' column is not in instance.sut.get_index(MI['f'])
        err_msg = []
        item = MI['f']
        for i in df.query(f"Item=='{item}'")['DB Item']:
            if i not in instance.sut.get_index(MI['f']):
                err_msg.append(i)
        if err_msg != []:
            raise ValueError(f"Error in Inventory sheet for {inventory} | Not allowed activities found: {err_msg}")
        
        # check whether any element of the 'DB Item' column is not in instance.sut.get_index(MI['k'])
        err_msg = []
        item = MI['k']
        for i in df.query(f"Item=='{item}'")['DB Item']:
            if i not in instance.sut.get_index(MI['k']):
                err_msg.append(i)
        if err_msg != []:
            raise ValueError(f"Error in Inventory sheet for {inventory} | Not allowed consumption categories found: {err_msg}")
        
        # check whether any element of the 'DB Region' column is not allowed
        allowed_regions = instance.sut.get_index(MI['r'])
        for k in regions_maps.keys():
            allowed_regions += [k]
        item = MI['c']
        err_msg = []
        for i in df.query(f"Item=='{item}'")['DB Region']:
            if i not in allowed_regions:
                err_msg.append(i)
        if err_msg != []:
            raise ValueError(f"Error in Inventory sheet for {inventory} | Not allowed regions found: {err_msg}")

        # check whether any element of the 'Type' column is not in ['Update','Percentage','Absolute']
        err_msg = []
        for i in df['Type']:
            if i not in ['Update','Percentage','Absolute']:
                err_msg.append(i)
        if err_msg != []:
            raise ValueError(f"Error in Inventory sheet for {inventory} | Not allowed types found: {err_msg}")
        
        # check for errors in unit of measures
        err_msg = []
        for i in df.index:
            unit = df.loc[i,'Unit']
            item = df.loc[i,'Item']
            input = df.loc[i,'Input']
            db_item = df.loc[i,'DB Item']
            db_unit = instance.sut.units[item].loc[db_item,'unit']

            msg = check_unit_of_measure(input,unit,db_unit)
            if msg is not None:
                err_msg += [msg]

        if err_msg != []:
            raise ValueError(f"Error in Inventory sheet for {inventory} | Non convertible units found: {err_msg}")

def check_unit_of_measure(input,unit,db_unit):

    ureg = pint.UnitRegistry()

    if unit == db_unit:
        return
    
    try:
        if ureg(unit).is_compatible_with(db_unit):
            return
        else:
            msg = f"'{input}' from {unit} to {db_unit}"
            return msg
    except:
        msg = f"{unit} unit provided for '{input}' is not acceptable by pint"
        return msg
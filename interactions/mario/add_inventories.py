import pandas as pd
from copy import deepcopy
from mario.tools.constants import _MASTER_INDEX as MI

def add_inventories_from_master_template(
        instance,
        matrices:dict,
    ):

    regions = instance.sut.get_index(MI['r'])
    new_commodities = instance.new_commodities
    new_activities = instance.new_activities

    units = instance.sut.units
    is_hybrid = instance.sut.is_hybrid
    master_sheet = instance.master_sheet

    # initialize indices and units of new cols and rows for activities and commodities in a dict
    new_indices = {}
    for item in [MI['c'],MI['a']]:
        
        if item == MI['c']: 
            new_items = deepcopy(new_commodities)
        if item == MI['a']:
            new_items = deepcopy(new_activities)

        region_ind = []
        item_ind = []
        new_item_ind = []
        for r in regions:
            for i in range(len(new_items)):
                region_ind.append(r)
                item_ind.append(item)
                new_item_ind.append(new_items[i])

        new_indices[item] = pd.MultiIndex.from_arrays([region_ind,item_ind,new_item_ind])    
        units[item] = get_new_units(master_sheet,item,new_items,units,is_hybrid)
    
    
    # initialize new database slices
    for item in [MI['c'],MI['a']]:
        new_z_cols = pd.DataFrame(0,index=matrices['z'].index,columns=new_indices[item])
        new_e_cols = pd.DataFrame(0,index=matrices['e'].index,columns=new_indices[item])
        new_v_cols = pd.DataFrame(0,index=matrices['v'].index,columns=new_indices[item])

        

        z = pd.concat([matrices['z'],new_z_cols],axis=1)
        e = pd.concat([matrices['e'],new_e_cols],axis=1)
        v = pd.concat([matrices['v'],new_v_cols],axis=1)    


    new_matrices = {}

    return new_matrices,units

def get_new_units(master_sheet,item,new_items,old_units,is_hybrid):

    if item==MI['c']:
        df = master_sheet.query(f"{MI['c']}==@new_items").loc[:,[item,'FU unit']].set_index(item)
        df.columns = ['unit']
    
    if item == MI['a']:
        if is_hybrid:
            act_unit = old_units[MI['a']]['unit'].unique()[0]
            df = pd.DataFrame(act_unit,index=new_items,columns=['unit'])

    units = deepcopy(old_units[item]).append(df)

    return units
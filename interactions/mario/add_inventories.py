import pandas as pd
import pint 

from copy import deepcopy
from mario.tools.constants import _MASTER_INDEX as MI

sn = slice(None)

class inventories_adder:

    def __init__(
            self,
            builder,
            matrices,
    ):
        
        self.builder = builder

        self.matrices = matrices
        self.regions = builder.sut.get_index(MI['r'])
        self.units = builder.sut.units

        self.new_activities = builder.new_activities
        self.new_commodities = builder.new_commodities
        self.parented_activities = builder.parented_activities


    def add_from_master(
            self,
    ):
        self.new_indices = {}
        self.new_indices[MI['c']] = self.get_indices(MI['c'],self.regions)
        self.new_indices[MI['a']] = self.get_indices(MI['a'],self.regions)

        self.add_new_units(MI['c'])
        self.add_new_units(MI['a'])

        slices = self.get_empty_table_slices()



    def get_indices(
            self,
            item,
            regions
    ): 
        if item == MI['c']: 
            new_items = deepcopy(self.new_commodities)
        if item == MI['a']:
            new_items = deepcopy(self.activities)

        region_ind = []
        item_ind = []
        new_item_ind = []
        for r in regions:
            for i in range(len(new_items)):
                region_ind.append(r)
                item_ind.append(item)
                new_item_ind.append(new_items[i])

        new_indices = pd.MultiIndex.from_arrays([region_ind,item_ind,new_item_ind])    

        return new_indices

    def add_new_units(
            self,
            item,
    ):
        if item==MI['c']:
            df = self.builder.master_sheet.query(f"{MI['c']}==@new_items").loc[:,[item,'FU unit']].set_index(item)
            df.columns = ['unit']
        
        if item == MI['a']:
            if self.builder.sut.is_hybrid:
                act_unit = self.units[MI['a']]['unit'].unique()[0]
                df = pd.DataFrame(act_unit,index=new_items,columns=['unit'])

        self.units[item].append(df)
    
    def get_empty_table_slices(
            self,
    ):
        matrix_slices_map = {
            'z':['rows','cols'],
            'e':['cols'],
            'v':['cols'],
            'Y':['rows'],
            }
        
        empty_slices = {}

        for item in [MI['c'],MI['a']]: # for commodities and activities
            empty_slices[item] = {}
            for m in matrix_slices_map: # for each matrix where the slices will be added
                empty_slices[item][m] = {}
                for s in matrix_slices_map[m]: # for each type of slice (rows or cols) 
                    if s == 'rows':  
                        empty_slices[item][m][s] = pd.DataFrame(0,index=self.new_indices[item],columns=self.matrices[m].columns)
                    if s == 'cols':
                        if 'rows' in matrix_slices_map[m]: # if, both rows and cols slices needs to be added to the same matrix
                            dummy_df = pd.concat([self.matrices[m],empty_slices[item][m]['rows']],axis=0) # get a dummy df with the new rows just to use its index
                            empty_slices[item][m][s] = pd.DataFrame(0,index=dummy_df.index,columns=self.new_indices[item]) # the new cols must have the same index as the matrix + the one of the rows slice
                        else:
                            empty_slices[item][m][s] = pd.DataFrame(0,index=self.matrices[m].index,columns=self.new_indices[item])
        
        return empty_slices

    def fill_slices(
            self,
            activity,
            slices,
    ):
        
        if self.leave_empty(activity):
            return
        
        region = self.builder.master_sheet.query(f"{MI['a']}==@activity")[MI['r']].values[0]
        if region in self.builder.sut.get_index(MI['r']):
            target_regions = [region]
        elif region not in self.builder.sut.get_index(MI['r']):
            if region in self.builder.regions_maps:
                target_regions = self.builder.regions_maps[region] 
            else:
                raise ValueError(f"Activity {activity} is added in region {region} which is not in the SUT nor in the regions map")

        inventory = self.builder.inventories[activity]

        if activity in self.parented_activities: # if the activity must be structured based on a parent activity, possibly in another region
            parent_activity = self.builder.master_sheet.query(f"{MI['a']}==@activity")[f'Parent {MI["a"]}'].values[0]

            parent_region = self.builder.master_sheet.query(f"{MI['a']}==@activity")[f'Parent {MI["r"]}'].values[0]
            if parent_region not in self.builder.sut.get_index(MI['r']):
                raise ValueError(f"Parent region {parent_region} of {activity} is not in the SUT. For now it is necessary to add indicate an existing region in the SUT")
            # if parent_region == '': # check if parent region is empty
            #     parent_region = region # if it is, then the parent region is the same assumed to be the target region

            for region in target_regions: # the same activity can be added in multiple regions...
                slices[MI['a']]['z']['cols'].loc[(sn,MI['c'],sn),(region,MI['a'],activity)] = self.builder.sut.z.loc[(sn,MI['c'],sn),(parent_region,MI['a'],parent_activity)].values
                slices[MI['a']]['v']['cols'].loc[(sn,MI['c'],sn),(region,MI['a'],activity)] = self.builder.sut.v.loc[(sn,MI['c'],sn),(parent_region,MI['a'],parent_activity)].values
                slices[MI['a']]['e']['cols'].loc[(sn,MI['c'],sn),(region,MI['a'],activity)] = self.builder.sut.e.loc[(sn,MI['c'],sn),(parent_region,MI['a'],parent_activity)].values
            
        inventory = self.make_units_consistent_to_database(inventory) 


    def make_units_consistent_to_database(
            self,
            inventory,
            cqc = 'Converted quantity', # converted quantity column
    ):
        self.converted_quantity_column = cqc

        inventory[cqc] = ""
        ureg = pint.UnitRegistry()

        for i in inventory.index: # for each input in the inventory
            if inventory.loc[i,'Unit'] == inventory.loc[i,'DB Unit']: # if the unit of the input is the same as the database unit
                inventory.loc[i,cqc] = inventory.loc[i,'Quantity']

            elif ureg(inventory.loc[i,'Unit']).is_compatible_with(inventory.loc[i,'DB Unit']): # if the unit of the input is convertible to the database unit using pint 
                inventory.loc[i,cqc] = ureg(inventory.loc[i,'Quantity']).to(inventory.loc[i,'DB Unit']).magnitude

            else:
                raise NotImplementedError(f"Unit {inventory.loc[i,'Unit']} is not convertible to {inventory.loc[i,'DB Unit']} without using LUCA (not implemented yet)")

        return inventory

    def fill_commodities_inputs(
            self,
            inventory,
    ):
        comm_inventory = inventory.query(f"Item=={MI['c']}") 


    def leave_empty(
            self,
            activity,
    ):
        empty = self.builder.master_sheet.query(f"{MI['a']}==@activity")['Leave empty'].values[0]
        if isinstance(empty,bool):
            if empty:
                return True
            else:
                return False
        else:
            raise ValueError(f"'Leave empty' column for {activity} in the master file must be boolean or left empty, got {empty} instead")


    def add_slices(
            self,
            slices,
    ):
        for item in [MI['c'],MI['a']]:
            for m in slices[item]:
                for s in slices[item][m]:
                    if s == 'rows':
                        self.matrices[m] = pd.concat([self.matrices[m],slices[item][m][s]],axis=0)
                    if s == 'cols':
                        self.matrices[m] = pd.concat([self.matrices[m],slices[item][m][s]],axis=1)







import pandas as pd
import pint 

from fiona.rules import setup_logger
from fiona.rules import LOG_MESSAGES as logmsg

from copy import deepcopy
from mario.tools.constants import _MASTER_INDEX as MI

logger = setup_logger('Inventories')
sn = slice(None)

_matrix_slices_map = {
    'u':{0:MI['c'],1:MI['a'],'concat':1},
    's':{0:MI['a'],1:MI['c'],'concat':0},
    'e':{0:MI['k'],1:MI['a'],'concat':1},
    'v':{0:MI['f'],1:MI['a'],'concat':1},
    'Y':{0:MI['c'],1:MI['n'],'concat':0},
}

class Inventories:

    def __init__(
            self,
            builder,
            matrices:dict,
    ):
        """
        Initialize the AddInventories class.

        Args:
            builder (Builder): The DB_builder object.
            matrices (list): The MARIO matrices to be used.

        Attributes:
            builder (Builder): The builder object.
            matrices (list): The matrices to be used.
            regions (list): The regions from the builder's system under test.
            units (list): The units from the builder's system under test.
            new_activities (list): The new activities from the builder.
            new_commodities (list): The new commodities from the builder.
            parented_activities (list): The parented activities from the builder.
        """
        self.builder = builder
        self.matrices = matrices
        self.regions = builder.sut.get_index(MI['r'])
        self.units = builder.sut.units
        self.new_activities = builder.new_activities
        self.new_commodities = builder.new_commodities
        self.parented_activities = builder.parented_activities

    def add_from_master(
            self
    ):
        """
        Adds new units from the master inventory to the current inventory.

        This method retrieves the indices for 'c' and 'a' regions from the master inventory
        and adds the new units to the current inventory using the 'add_new_units' method.
        It also initializes the 'slices' attribute with empty table slices.
        """
             
        self.add_new_units(MI['c'])
        self.add_new_units(MI['a'])
        logger.info(f"{logmsg['dm']} | Units of new activities and commodities added to the SUT database")

        self.matrices['u'] = self.matrices['z'].loc[(sn,MI['c'],sn),(sn,MI['a'],sn)]
        self.matrices['s'] = self.matrices['z'].loc[(sn,MI['a'],sn),(sn,MI['c'],sn)]

        empty_slices = self.get_empty_table_slices()
        logger.info(f"{logmsg['dm']} | Empty slices created'")

        self.filled_slices = deepcopy(empty_slices)
        for activity in self.new_activities:
            self.fill_slices(activity)

        logger.info(f"{logmsg['dm']} | Adding slices to matrices")
        self.add_slices()
        logger.info(f"{logmsg['dm']} | Slices for added to matrices")
        
        new_act_indices = self.matrices['s'].loc[(sn,MI['a'],self.new_activities),:].index
        new_com_indices = self.matrices['u'].loc[(sn,MI['c'],self.new_commodities),:].index
        self.matrices['Y'] = pd.concat([self.matrices['Y'],pd.DataFrame(0, index=new_act_indices, columns=self.matrices['Y'].columns)],axis=0)
        self.matrices['v'] = pd.concat([self.matrices['v'],pd.DataFrame(0, index=self.matrices['v'].index, columns=new_com_indices)],axis=1)
        self.matrices['e'] = pd.concat([self.matrices['e'],pd.DataFrame(0, index=self.matrices['e'].index, columns=new_com_indices)],axis=1)

        self.matrices['z'] = pd.concat([self.matrices['u'],self.matrices['s']],axis=1).fillna(0)

        logger.info(f"{logmsg['dm']} | Sorting indices of all matrices in the SUT database")
        self.reindex_matrices()
        logger.info(f"{logmsg['dm']} | Indices of all matrices sorted")

        self.get_mario_indices() # to be deprecated when mario will allow to initialize database in coefficients

    def add_new_units(
            self,
            item:str
        ):
        """
        Add new units for the given item.

        Parameters:
            item (str): The item for which new units are being added.

        Returns:
            None
        """
        if item == MI['c']:
            new_items = self.new_commodities
            df = self.builder.master_sheet.query(f"{MI['c']}==@new_items").loc[:,[item,'FU unit']].set_index(item)
            df.columns = ['unit']
            df = df.reset_index().drop_duplicates().set_index(item)

        if item == MI['a']:
            new_items = self.new_activities
            if self.builder.sut.is_hybrid:
                act_unit = self.units[MI['a']]['unit'].unique()[0]
                df = pd.DataFrame(act_unit, index=new_items, columns=['unit'])
            else:
                df = self.builder.master_sheet.query(f"{MI['a']}==@new_items").loc[:,[item,'FU unit']].set_index(item)
                df.columns = ['unit']
                df = df.drop_duplicates()
                
        self.units[item] = pd.concat([self.units[item],df],axis=0)
    
    def get_empty_table_slices(self):
        """
        Returns a dictionary containing empty table slices for each matrix and axis.

        Returns:
            dict: A dictionary containing empty table slices for each matrix and item.
                  The keys of the dictionary are the matrix names, and the values are
                  dictionaries containing empty table slices for each axis. Each axis
                  dictionary contains empty pd.DataFrames structured as empty slices according
                  to which axis they will be then concatenated to the original matrices.
        """
        empty_slices = {}

        for matrix in _matrix_slices_map:
            new_index,new_columns = self.get_slice_indices(matrix)
            empty_slices[matrix] = pd.DataFrame(0, index=new_index, columns=new_columns) 
            
        return empty_slices

    def get_slice_indices(
            self,
            matrix:str,
        )->pd.MultiIndex:
        """
        Get the index and columns for a given matrix.

        Args:
            matrix (str): The matrix to which the slices will be added.
        Returns:
            pd.MultiIndex: The multi-level index and columns of the slice
        
        """
        
        concat = _matrix_slices_map[matrix]['concat']
        item_row = _matrix_slices_map[matrix][0]
        item_col = _matrix_slices_map[matrix][1]

        if concat == 0:
            empty_index = [[],[],[]]  # will always be 3 levels
            items_to_add_on_rows = self.builder.new_commodities if item_row == MI['c'] else self.builder.new_activities
            for region in self.regions:
                for item in items_to_add_on_rows:
                    empty_index[0] += [region]
                    empty_index[1] += [item_row]
                    empty_index[2] += [item]
            
            if matrix == 'Y':   # it is possible that a new activity produces an old commodity and an extra final demand could come from that. Therefore, Y slice should have all indices 
                new_index = pd.MultiIndex.from_arrays([
                    self.matrices[matrix].index.get_level_values(0).tolist() + empty_index[0],
                    self.matrices[matrix].index.get_level_values(1).tolist() + empty_index[1],
                    self.matrices[matrix].index.get_level_values(2).tolist() + empty_index[2],
                ])
            else:
                new_index = pd.MultiIndex.from_arrays(empty_index)

            empty_extra_columns = [[],[],[]] # will always be 3 levels
            items_to_add_on_cols = self.builder.new_activities if item_col == MI['a'] else self.builder.new_commodities
            for region in self.regions:
                for item in items_to_add_on_cols:
                    empty_extra_columns[0] += [region]
                    empty_extra_columns[1] += [item_col]
                    empty_extra_columns[2] += [item]
            new_columns = pd.MultiIndex.from_arrays([
                self.matrices[matrix].columns.get_level_values(0).tolist() + empty_extra_columns[0],
                self.matrices[matrix].columns.get_level_values(1).tolist() + empty_extra_columns[1],
                self.matrices[matrix].columns.get_level_values(2).tolist() + empty_extra_columns[2],
            ])
            if matrix == 'Y':
                new_columns = self.matrices[matrix].columns
                
        if concat == 1:
            if matrix in ['v','e']:
                new_index = self.matrices[matrix].index
            else: 
                empty_extra_index = [[],[],[]]  # will always be 3 levels if not v or e
                items_to_add_on_rows = self.builder.new_activities if item_row == MI['a'] else self.builder.new_commodities
                for region in self.regions:
                    for item in items_to_add_on_rows:
                        empty_extra_index[0] += [region]
                        empty_extra_index[1] += [item_row]
                        empty_extra_index[2] += [item]
                new_index = pd.MultiIndex.from_arrays([
                    self.matrices[matrix].index.get_level_values(0).tolist() + empty_extra_index[0],
                    self.matrices[matrix].index.get_level_values(1).tolist() + empty_extra_index[1],
                    self.matrices[matrix].index.get_level_values(2).tolist() + empty_extra_index[2],
                ])

            empty_columns = [[],[],[]] # will always be 3 levels
            items_to_add_on_cols = self.builder.new_activities if item_col == MI['a'] else self.builder.new_commodities
            for region in self.regions:
                for item in items_to_add_on_cols:
                    empty_columns[0] += [region]
                    empty_columns[1] += [item_col]
                    empty_columns[2] += [item]
            new_columns = pd.MultiIndex.from_arrays(empty_columns)

        return new_index, new_columns
    
    def fill_slices(
            self,
            activity:str,
    ):
        """
        Fills the slices for the given activity.

        Args:
            activity (str): The activity for which the slices need to be filled.

        Raises:
            ValueError: If the activity is added in a region that is not in the SUT nor in the regions map.
            ValueError: If the parent region of the activity is not in the SUT.
        """

        slices = self.get_empty_table_slices()

        # get the inventory for the activity
        inventories = self.builder.inventories[activity]

        # the same activity could be initialized in a different way in different regions (ergo: different inventories)
        for sheet_name,inventory in inventories.items():

            if self.leave_empty(sheet_name):
                logger.info(f"{logmsg['dm']} | 'Inventory {sheet_name}' for activity {activity} not added to matrices because 'Leave empty' is True")
                return
            
            # get the region where to add the activity
            region = self.builder.master_sheet.query(f"`Sheet name`==@sheet_name")[MI['r']].values[0]

            # check if the region is in the SUT or in the regions maps
            if region in self.builder.sut.get_index(MI['r']):
                target_regions = [region]
            elif region not in self.builder.sut.get_index(MI['r']):
                if region in self.builder.regions_maps:
                    target_regions = self.builder.regions_maps[region] 
                else:
                    raise ValueError(f"Activity {activity} is added in region {region} which is not in the SUT nor in the regions map")

            # in case the activity has a parent to be initialized from
            parent_activity = self.builder.master_sheet.query(f"`Sheet name`==@sheet_name")[f'Parent {MI["a"]}'].values[0]
            if pd.isna(parent_activity) == False: 
                slices = self.copy_from_parent(activity,parent_activity,target_regions,slices,inventory)
                logger.info(f"{logmsg['dm']} | Activity '{activity}' initialized equal to parent activity '{parent_activity}' in region '{region}'")

            logger.info(f"{logmsg['dm']} | Converting units of inventory of activity '{activity}' consistently with the units of the SUT database")        
            inventory = self.make_units_consistent_to_database(inventory) 
            logger.info(f"{logmsg['dm']} | Units converted for activity '{activity}'")
            
            logger.info(f"{logmsg['dm']} | Filling slices for '{activity}'")
            for region_to in target_regions:
                slices = self.fill_commodities_inputs(inventory,region_to,activity,slices)
                slices = self.fill_fact_sats_inputs(inventory,region_to,activity,'v',slices)
                slices = self.fill_fact_sats_inputs(inventory,region_to,activity,'e',slices)
                slices = self.fill_market_shares(activity,region_to,region,slices)
                slices = self.fill_final_demand(activity,region_to,region,slices)
            logger.info(f"{logmsg['dm']} | Slices for '{activity}' filled")

            for matrix in slices:
                self.filled_slices[matrix].update(slices[matrix])

    def reindex_matrices(
            self,
    ):
        matrices_levels = {
            'z': {0:3,1:3},
            'e': {0:1,1:3},
            'v': {0:1,1:3},
            'Y': {0:3,1:3},
            }

        for matrix in matrices_levels:
            for ax in matrices_levels[matrix]:
                levels = list(range(matrices_levels[matrix][ax]))
                self.matrices[matrix].sort_index(axis=ax, level=levels, inplace=True)
            

    def make_units_consistent_to_database(
            self, 
            inventory:pd.DataFrame, 
            cqc:str = 'Converted quantity'
        ):
        """
        Converts the units of the inventory to be consistent with the database unit.

        Args:
            inventory (pd.DataFrame): The inventory data as a pandas DataFrame.
            cqc (str, optional): The name of the column to store the converted quantity. Defaults to 'Converted quantity'.

        Returns:
            pd.DataFrame: The modified inventory DataFrame with consistent units.

        Raises:
            NotImplementedError: If the unit of an input is not convertible to the database unit without using LUCA.
        """
        self.converted_quantity_column = cqc
        inventory[cqc] = ""
        ureg = pint.UnitRegistry()

        for i in inventory.index:
            item = inventory.loc[i, 'Item']
            if item == MI['c']:
                DB_unit = self.units[MI['c']].loc[inventory.loc[i, 'DB Item'],'unit']
            elif item == MI['a']:
                raise ValueError(f"Item {item} is not recognized: activities cannot be supplied to other activities")
            elif item == MI['k']:
                DB_unit = self.units[MI['k']].loc[inventory.loc[i, 'DB Item'],'unit']
            elif item == MI['f']:
                DB_unit = self.units[MI['f']].loc[inventory.loc[i, 'DB Item'],'unit']
            else:
                raise ValueError(f"Item {item} is not recognized")

            if inventory.loc[i, 'Unit'] == DB_unit:
                inventory.loc[i, cqc] = inventory.loc[i, 'Quantity']
            elif ureg(inventory.loc[i, 'Unit']).is_compatible_with(DB_unit):
                inventory.loc[i, cqc] = inventory.loc[i, 'Quantity']*ureg(inventory.loc[i, 'Unit']).to(DB_unit).magnitude                
            else:
                raise NotImplementedError(f"Unit {inventory.loc[i, 'Unit']} is not convertible to {DB_unit} without using LUCA (not implemented yet)")

        return inventory

    def copy_from_parent(
        self,
        activity:str,
        parent_activity:str,
        target_regions:list,
        slices:dict,
        inventory:pd.DataFrame
    )->dict:
        """
        Copy the parent activity in the target region into the new activity inventory on u, v and e.

        Args:
            activity (str): activity to be filled starting from the parent activity.
            parent_activity (str): parent activity to be copied.
            target_regions (list): list of regions where the activity must be filled.
            slices (dict): dictionary containing the slices to be filled.
            inventory (pd.DataFrame): inventory of the activity containing the information to be updated later, so those of the parent activity must be nullified.

        Returns:
            dict: dictionary containing the updated slices.
        """

        # copy the parent activity in the target region into the new activity inventory on u, v and e
        for region in target_regions: 
            slices['u'].loc[self.matrices['u'].index,(region,MI['a'],activity)] = self.matrices['u'].loc[:,(region,MI['a'],parent_activity)].values
            slices['v'].loc[:,(region,MI['a'],activity)] = self.matrices['v'].loc[:,(region,MI['a'],parent_activity)].values
            slices['e'].loc[:,(region,MI['a'],activity)] = self.matrices['e'].loc[:,(region,MI['a'],parent_activity)].values

            # nullify the values that must be updated according to the activity's inventory
            commodities_to_nullify = [(c,r) for c,r in zip(inventory.query(f"Item=='{MI['c']}' & Type=='Update'")['DB Item'].values,inventory.query(f"Item=='{MI['c']}' & Type=='Update'")['DB Region'].values)]
            satellites_to_nullify = inventory.query(f"Item=='{MI['k']}' & Type=='Update'")['DB Item'].values
            factors_to_nullify = inventory.query(f"Item=='{MI['f']}' & Type=='Update'")['DB Item'].values

            for tup in commodities_to_nullify:
                c = tup[0]
                r = tup[1]
                if r in self.builder.sut.get_index(MI['r']):
                    slices['u'].loc[(r,MI['c'],c),(region,MI['a'],activity)] = 0
                elif r in self.builder.regions_maps:
                    slices['u'].loc[(self.builder.regions_maps[r],MI['c'],c),(region,MI['a'],activity)] = 0
                
            for k in satellites_to_nullify:
                slices['e'].loc[k,(region,MI['a'],activity)] = 0
            
            for f in factors_to_nullify:
                slices['v'].loc[f,(region,MI['a'],activity)] = 0
        
        return slices


    def fill_commodities_inputs(
        self,
        full_inventory:pd.DataFrame,
        region_to:str,
        activity:str,
        slices:dict,
    )->dict:
        """
        Fills the commodities inputs for a given region and activity.

        Args:
            full_inventory (pandas.DataFrame): The full inventory data.
            region_to (str): The target region.
            activity (str): The target activity.
            slices (dict): The slices to fill.

        Returns:
            dict: The updated slices.
        """
        inventory = full_inventory.query(f"Item=='{MI['c']}'") 
        
        for i in inventory.index:
            
            # get all the necessary information of the input item
            input_item = inventory.loc[i,"DB Item"]
            if input_item in self.builder.sut.get_index(MI['c']):
                is_new = False
            else:
                is_new = True

            quantity = inventory.loc[i,self.converted_quantity_column]
            region_from = inventory.loc[i,f"DB {MI['r']}"]
            change_type = inventory.loc[i,'Type']

            if change_type == 'Update':
                if region_from in self.builder.sut.get_index(MI['r']):
                    slices['u'].loc[(region_from,MI['c'],input_item),(region_to,MI['a'],activity)] += quantity
            
                elif region_from in self.builder.regions_maps:
                    if not is_new:
                        com_use = self.builder.sut.u.loc[(self.builder.regions_maps[region_from],sn,input_item),(region_to,sn,sn)]                    
                        u_share = com_use.sum(1)/com_use.sum().sum()*quantity
                        if isinstance(u_share,pd.Series):
                            u_share = u_share.to_frame()
                        u_share.columns = pd.MultiIndex.from_arrays([[region_to],[MI['a']],[activity]])
                        slices['u'].loc[u_share.index,u_share.columns] += u_share.values
                    else:
                        slices['u'].loc[(region_to,MI['c'],input_item),(region_to,MI['a'],activity)] += quantity

        return slices

    def fill_fact_sats_inputs(
        self,
        full_inventory: pd.DataFrame,
        region_to: str,
        activity: str,
        matrix: str,
        slices:str
    )->dict:
        """
        Fills the fact sats inputs based on the given parameters.

        Args:
            full_inventory (pd.DataFrame): The full inventory dataframe.
            region_to (str): The region to fill the fact sats inputs for.
            activity (str): The activity to fill the fact sats inputs for.
            matrix (str): The matrix type ('v' or 'e').
            slices (dict): The slices to fill.

        Returns:
            dict: The updated slices.
        """
        if matrix == 'v':
            inventory = full_inventory.query(f"Item=='{MI['f']}'")
        if matrix == 'e':
            inventory = full_inventory.query(f"Item=='{MI['k']}'")

        for i in inventory.index:
            input_item = inventory.loc[i, "DB Item"]
            quantity = inventory.loc[i, self.converted_quantity_column]
            change_type = inventory.loc[i, 'Type']

            if change_type == 'Update':
                slices[matrix].loc[input_item, (region_to, MI['a'], activity)] += quantity
            if change_type == 'Percentage':
                if activity in self.parented_activities:
                    parent_activity = self.builder.master_sheet.query(f"{MI['a']}==@activity")[f'Parent {MI["a"]}'].values[0]
                    old_value = self.matrices[matrix].loc[input_item, (region_to, MI['a'], parent_activity)]
                    slices[matrix].loc[input_item, (region_to, MI['a'], activity)] += old_value*(1+quantity)
                else:
                    raise ValueError(f"It's not possible to apply a percentage change to activity {activity} because it has no parent activity")
        
        return slices

    def fill_market_shares(
        self,
        activity:str,
        region:str,
        cluster_region:str,
        slices:dict
    )->dict:
        """
        Fills the market shares for a given activity and region.

        Parameters:
        - activity (str): The activity for which market shares need to be filled.
        - region (str): The region for which market shares need to be filled.
        - cluster_region (str): The cluster region for which market shares need to be filled.
        - slices (dict): The slices to fill.

        Returns:
        - dict: The updated slices.
        """

        market_shares = self.builder.master_sheet.query(f"{MI['a']}==@activity & {MI['r']}==@cluster_region")['Market share'].values
        for i in range(len(market_shares)):
            if pd.isna(market_shares[i]):
                market_shares[i] = 0
        
        commodities = self.builder.master_sheet.query(f"{MI['a']}==@activity & {MI['r']}==@cluster_region")[MI['c']].values
        for i in range(len(commodities)):
            slices['s'].loc[(region,MI['a'],activity),(region,MI['c'],commodities[i])] = market_shares[i]
        
        return slices

    def fill_final_demand(
        self,
        activity:str,
        region:str,
        cluster_region:str,
        slices:dict
    )->dict:
        """
        Fills the final demand for a given activity and region.

        Parameters:
            activity (str): The activity for which the final demand needs to be filled.
            region (str): The region for which the final demand needs to be filled.
            cluster_region (str): The cluster region for which the final demand needs to be filled.
            slices (dict): The slices to fill.

        Returns:
            dict: The updated slices.
        """
        total_outputs = self.builder.master_sheet.query(f"{MI['a']}==@activity & {MI['r']}==@cluster_region")['Total output'].values
        for i in range(len(total_outputs)):
            if pd.isna(total_outputs[i]):
                total_outputs[i] = 0

        cons_categories = self.builder.master_sheet.query(f"{MI['a']}==@activity & {MI['r']}==@cluster_region")[MI['n']].values
        new_cons_categories = []
        
        for i in range(len(cons_categories)):
            if pd.isna(cons_categories[i]):
                new_cons_categories += [self.matrices['Y'].columns.get_level_values(-1)[0]]
            else:
                new_cons_categories += [cons_categories[i]]

        cons_region = region # could be easily changed by adding a new column in the master file
        commodities = self.builder.master_sheet.query(f"{MI['a']}==@activity & {MI['r']}==@cluster_region")[MI['c']].values

        for i in range(len(commodities)):
            slices['Y'].loc[(region,MI['c'],commodities[i]),(cons_region,MI['n'],new_cons_categories[i])] += total_outputs[i]

        return slices   

    def leave_empty(
            self, 
            sheet_name:str
    ):
        """
        Check if the 'Leave empty' column for a given activity in the master file is True or False.

        Parameters:
        - activity (str): The activity to check.

        Returns:
        - bool: True if the 'Leave empty' column is True, False otherwise.

        Raises:
        - ValueError: If the 'Leave empty' column is not a boolean or left empty.
        """
        empty = self.builder.master_sheet.query(f"`Sheet name`==@sheet_name")['Leave empty'].values[0]
        if empty == True or empty == False:
            return empty

        elif isinstance(empty, float):
            if empty == 1:
                return True
            if empty == 0:
                return False
            if pd.isna(empty):
                return False
        else:
            raise ValueError(f"'Leave empty' column for inventory {sheet_name} in the master file must be boolean or left empty, got {empty} instead")

    def add_slices(self):
        """
        Add slices to the matrices.

        This method iterates over a list of items and for each item, it iterates over the slices
        associated with that item. For each slice, it checks if it is a row slice or a column slice.
        If it is a row slice, it concatenates the slice with the corresponding matrix along the row axis.
        If it is a column slice, it concatenates the slice with the corresponding matrix along the column axis.
        """
        
        for matrix in _matrix_slices_map:
            concat = _matrix_slices_map[matrix]['concat']
            self.matrices[matrix] = pd.concat([self.matrices[matrix],self.filled_slices[matrix]],axis=concat)
            self.matrices[matrix] = self.matrices[matrix].groupby(level=list(range(self.matrices[matrix].index.nlevels)),axis=0).sum()
            self.matrices[matrix] = self.matrices[matrix].groupby(level=list(range(self.matrices[matrix].columns.nlevels)),axis=1).sum()

    def get_mario_indices(
            self
    ):
        """
        Retrieves the Mario indices based on different items. (TO BE DEPRECATED)

        Returns:
            dict: A dictionary containing the Mario indices for each item.
                The keys of the dictionary represent the items, and the values
                are dictionaries with the 'main' key containing a sorted list
                of the corresponding indices.

        Example:
            >>> mario = Mario()
            >>> mario.get_mario_indices()
            {'r': {'main': [1, 2, 3]}, 'a': {'main': [4, 5, 6]}, ...}
        """       
        mario_indices = {}

        for item in MI.vars:
            if item == 'r':
                mario_indices[item] = {'main': sorted(list(set(self.matrices['z'].index.get_level_values(0))))}
            if item == 'a' or item == 's':
                mario_indices[item] = {'main': sorted(list(set(self.matrices['z'].loc[(sn,MI['a'],sn),:].index.get_level_values(2))))}
            if item == 'c':
                mario_indices[item] = {'main': sorted(list(set(self.matrices['z'].loc[(sn,MI['c'],sn),:].index.get_level_values(2))))}
            if item == 'n':
                mario_indices[item] = {'main': sorted(list(set(self.matrices['Y'].columns.get_level_values(2))))}
            if item == 'k':
                mario_indices[item] = {'main': sorted(list(set(self.matrices['e'].index)))}
            if item == 'f':
                mario_indices[item] = {'main': sorted(list(set(self.matrices['v'].index)))}
    
        self.mario_indices = mario_indices

                                    





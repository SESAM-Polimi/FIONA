import pandas as pd
import pint 

from copy import deepcopy
from mario.tools.constants import _MASTER_INDEX as MI

sn = slice(None)

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
        self.new_indices = {}
        self.new_indices[MI['c']] = self.get_new_indices(MI['c'], self.regions)
        self.new_indices[MI['a']] = self.get_new_indices(MI['a'], self.regions)
        self.new_indices[MI['n']] = self.get_new_indices(MI['n'], self.regions)

        self.add_new_units(MI['c'])
        self.add_new_units(MI['a'])

        self.slices = self.get_empty_table_slices()

        for activity in self.new_activities:
            self.fill_slices(activity)
        
        self.get_mario_indices() # to be deprecated when mario will allow to initialize database in coefficients

    def get_new_indices(
            self, 
            item:str, 
            regions:list
    ):
        """
        Returns a new multi-index based on the given item and regions.

        Parameters:
        - item: The item to be used for indexing (Commodity or Activity).
        - regions: A list of regions to be used for indexing.

        Returns:
        - new_indices: A new multi-index created from the combination of region, item, and new_items.
        """
        if item == MI['c']: 
            new_items = deepcopy(self.new_commodities)
        if item == MI['a']:
            new_items = deepcopy(self.new_activities)
        if item == MI['n']:
            new_items = deepcopy(self.builder.sut.get_index(MI['n']))

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

        if item == MI['a']:
            new_items = self.new_activities
            if self.builder.sut.is_hybrid:
                act_unit = self.units[MI['a']]['unit'].unique()[0]
                df = pd.DataFrame(act_unit, index=new_items, columns=['unit'])
            else:
                df = self.builder.master_sheet.query(f"{MI['a']}==@new_items").loc[:,[item,'FU unit']].set_index(item)
                df.columns = ['unit']

        self.units[item].append(df)
    
    def get_empty_table_slices(self):
        """
        Returns a dictionary containing empty slices for different matrices.

        The method creates empty slices for different matrices based on the provided matrix_slices_map.
        The empty slices are returned as a dictionary with the following structure:
        {
            'Commodity': {
                'z': {
                    'rows': <empty DataFrame>,
                    'cols': <empty DataFrame>
                },
                'e': {
                    'cols': <empty DataFrame>
                },
                'v': {
                    'cols': <empty DataFrame>
                },
                'Y': {
                    'rows': <empty DataFrame>
                }
            },
            'Activity': {
                ...
            }
        }

        Returns:
            empty_slices (dict): A dictionary containing empty slices for different matrices.
        """
        matrix_slices_map = {
            'z': ['rows', 'cols'],
            'e': ['cols'],
            'v': ['cols'],
            'Y': ['rows'],
        }
        empty_slices = {}

        for item in [MI['c'], MI['a']]:  # for commodities and activities
            empty_slices[item] = {}
            for m in matrix_slices_map:  # for each matrix where the slices will be added
                empty_slices[item][m] = {}
                for s in matrix_slices_map[m]:  # for each type of slice (rows or cols)
                    if s == 'rows':
                        empty_slices[item][m][s] = pd.DataFrame(0, index=self.new_indices[item], columns=self.matrices[m].columns)
                    if s == 'cols':
                        if 'rows' in matrix_slices_map[m]:  # if, both rows and cols slices needs to be added to the same matrix
                            dummy_df = pd.concat([self.matrices[m], empty_slices[item][m]['rows']], axis=0)  # get a dummy df with the new rows just to use its index
                            empty_slices[item][m][s] = pd.DataFrame(0, index=dummy_df.index, columns=self.new_indices[item])  # the new cols must have the same index as the matrix + the one of the rows slice
                        else:
                            empty_slices[item][m][s] = pd.DataFrame(0, index=self.matrices[m].index, columns=self.new_indices[item])

        return empty_slices
    
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
                self.slices[MI['a']]['z']['cols'].loc[(sn,MI['c'],sn),(region,MI['a'],activity)] = self.builder.sut.z.loc[(sn,MI['c'],sn),(parent_region,MI['a'],parent_activity)].values
                self.slices[MI['a']]['v']['cols'].loc[(sn,MI['c'],sn),(region,MI['a'],activity)] = self.builder.sut.v.loc[(sn,MI['c'],sn),(parent_region,MI['a'],parent_activity)].values
                self.slices[MI['a']]['e']['cols'].loc[(sn,MI['c'],sn),(region,MI['a'],activity)] = self.builder.sut.e.loc[(sn,MI['c'],sn),(parent_region,MI['a'],parent_activity)].values
            
        inventory = self.make_units_consistent_to_database(inventory) 

        for region_to in target_regions:
            self.fill_commodities_inputs(inventory,region_to,activity)
            self.fill_fact_sats_inputs(inventory,region_to,activity,'v')
            self.fill_fact_sats_inputs(inventory,region_to,activity,'e')
            self.fill_market_shares(activity,region_to)
            self.fill_final_demand(activity,region_to)

        self.add_slices()

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

    def fill_commodities_inputs(
        self,
        full_inventory:pd.DataFrame,
        region_to:str,
        activity:str,
    ):
        """
        Fills the commodities inputs for a given region and activity.

        Args:
            full_inventory (pandas.DataFrame): The full inventory data.
            region_to (str): The target region.
            activity (str): The target activity.

        Returns:
            None
        """
        inventory = full_inventory.query(f"Item=='{MI['c']}'") 
        
        for i in inventory.index:
            
            region_from = inventory.loc[i,f"DB {MI['r']}"]
            input_item = inventory.loc[i,"DB Item"]
            quantity = inventory.loc[i,self.converted_quantity_column]

            if region_from in self.builder.sut.get_index(MI['r']):
                self.slices[MI['a']]['z']['cols'].loc[(region_from,MI['c'],input_item),(region_to,MI['a'],activity)] = quantity
            else:
                com_use = self.builder.sut.u.loc[(self.builder.regions_maps[region_from],sn,input_item),(region_to,sn,sn)]
                u_share = com_use.sum(1)/com_use.sum().sum()*quantity
                if isinstance(u_share,pd.Series):
                    u_share = u_share.to_frame()
                u_share.columns = pd.MultiIndex.from_arrays([[region_to],[MI['a']],[activity]])

                dummy_df = deepcopy(self.slices[MI['a']]['z']['cols'])
                dummy_df.update(u_share)
                self.slices[MI['a']]['z']['cols'].loc[:,(region_to,MI['a'],activity)] += dummy_df.loc[:,(region_to,MI['a'],activity)].values

    def fill_fact_sats_inputs(
        self,
        full_inventory: pd.DataFrame,
        region_to: str,
        activity: str,
        matrix: str,
    ):
        """
        Fills the fact sats inputs based on the given parameters.

        Args:
            full_inventory (pd.DataFrame): The full inventory dataframe.
            region_to (str): The region to fill the fact sats inputs for.
            activity (str): The activity to fill the fact sats inputs for.
            matrix (str): The matrix type ('v' or 'e').

        Returns:
            None
        """
        if matrix == 'v':
            inventory = full_inventory.query(f"Item=='{MI['f']}'")
        if matrix == 'e':
            inventory = full_inventory.query(f"Item=='{MI['k']}'")

        for i in inventory.index:
            input_item = inventory.loc[i, "DB Item"]
            quantity = inventory.loc[i, self.converted_quantity_column]
            self.slices[MI['a']][matrix]['cols'].loc[input_item, (region_to, MI['a'], activity)] = quantity

    def fill_market_shares(
        self,
        activity:str,
        region:str,
    ):
        """
        Fills the market shares for a given activity and region.

        Parameters:
        - activity (str): The activity for which market shares need to be filled.
        - region (str): The region for which market shares need to be filled.

        Returns:
        None
        """
        market_share = self.builder.master_sheet.query(f"{MI['a']}==@activity")['Market share'].values[0]
        commodity = self.builder.master_sheet.query(f"{MI['a']}==@activity")[MI['c']].values[0]
        self.slices[MI['a']]['z']['rows'].loc[(region,MI['a'],activity),(region,MI['c'],commodity)] = market_share

    def fill_final_demand(
            self,
            activity:str,
            region:str,
    ):
        """
        Fills the final demand for a given activity and region.

        Parameters:
            activity (str): The activity for which the final demand needs to be filled.
            region (str): The region for which the final demand needs to be filled.

        Returns:
            None
        """
        total_output = self.builder.master_sheet.query(f"{MI['a']}==@activity")['Total output'].values[0]
        cons_category = self.builder.master_sheet.query(f"{MI['a']}==@activity")[MI['n']].values[0]
        cons_region = region # could be easily changed by adding a new column in the master file

        self.slices[MI['a']]['Y']['rows'].loc[(region,MI['a'],activity),(cons_region,MI['n'],cons_category)] = total_output

    def leave_empty(
            self, 
            activity:str
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
        empty = self.builder.master_sheet.query(f"{MI['a']}==@activity")['Leave empty'].values[0]
        if isinstance(empty, bool):
            if empty:
                return True
            else:
                return False
        elif pd.isna(empty):
            return False
        else:
            raise ValueError(f"'Leave empty' column for {activity} in the master file must be boolean or left empty, got {empty} instead")

    def add_slices(self):
        """
        Add slices to the matrices.

        This method iterates over a list of items and for each item, it iterates over the slices
        associated with that item. For each slice, it checks if it is a row slice or a column slice.
        If it is a row slice, it concatenates the slice with the corresponding matrix along the row axis.
        If it is a column slice, it concatenates the slice with the corresponding matrix along the column axis.

        Parameters:
            None

        Returns:
            None
        """
        for item in [MI['c'], MI['a']]:
            for m in self.slices[item]:
                for s in self.slices[item][m]:
                    if s == 'rows':
                        self.matrices[m] = pd.concat([self.matrices[m], self.slices[item][m][s]], axis=0)
                    if s == 'cols':
                        self.matrices[m] = pd.concat([self.matrices[m], self.slices[item][m][s]], axis=1)
        for m in self.matrices:
            self.matrices[m].fillna(0, inplace=True) # to check why nans are present

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

        for item in MI:
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

                                    





import pandas as pd
import pint 

from rules import setup_logger
from rules import LOG_MESSAGES as logmsg

from copy import deepcopy
from mario.tools.constants import _MASTER_INDEX as MI

logger = setup_logger('Inventories')
sn = slice(None)

_matrix_slices_map = {
    'u':{MI['a']:[1],MI['c']:[0]},
    's':{MI['a']:[0],MI['c']:[1]},
    'e':{MI['a']:[1]},
    'v':{MI['a']:[1]},
    'Y':{MI['c']:[0]},
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

        self.slices = {}
        # create a dictionary with errors to retry

        for activity in self.new_activities:
            self.slices[activity] = self.get_empty_table_slices(activity)
            logger.info(f"{logmsg['dm']} | Empty slices created for activity '{activity}'")
            
            self.fill_slices(activity)
        
        # retry errors
        
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

        if item == MI['a']:
            new_items = self.new_activities
            if self.builder.sut.is_hybrid:
                act_unit = self.units[MI['a']]['unit'].unique()[0]
                df = pd.DataFrame(act_unit, index=new_items, columns=['unit'])
            else:
                df = self.builder.master_sheet.query(f"{MI['a']}==@new_items").loc[:,[item,'FU unit']].set_index(item)
                df.columns = ['unit']

        self.units[item] = pd.concat([self.units[item],df],axis=0)
    
    def get_empty_table_slices(self,activity):
        """
        Returns a dictionary containing empty table slices for each matrix and item.

        Returns:
            dict: A dictionary containing empty table slices for each matrix and item.
                  The keys of the dictionary are the matrix names, and the values are
                  dictionaries containing empty table slices for each item. Each item
                  dictionary contains empty table slices for 'c', 'a', and 'cross'.
        """
        empty_slices = {}

        for matrix in _matrix_slices_map:
            empty_slices[matrix] = {}
            for item in _matrix_slices_map[matrix]:
                empty_slices[matrix][item] = {}
                for s in _matrix_slices_map[matrix][item]:
                    new_index = self.get_slice_index(item,activity)
                    if s == 0:
                        empty_slices[matrix][item][s] = pd.DataFrame(0, index=new_index, columns=self.matrices[matrix].columns) 
                    if s == 1:
                        empty_slices[matrix][item][s] = pd.DataFrame(0, index=self.matrices[matrix].index, columns=new_index)
            
            if len(_matrix_slices_map[matrix]) == 2:
                if matrix == 's':
                    empty_slices[matrix]['cross'] = pd.DataFrame(0, index=empty_slices[matrix][MI['a']][0].index, columns=empty_slices[matrix][MI['c']][1].columns)
                if matrix == 'u':
                    empty_slices[matrix]['cross'] = pd.DataFrame(0, index=empty_slices[matrix][MI['c']][0].index, columns=empty_slices[matrix][MI['a']][1].columns)

        return empty_slices

    def get_slice_index(
            self, 
            item:str,
            activity:str,
        ):
        """
        Returns a new index for creating multi-indexed DataFrame slices.

        Parameters:
        - item (str): The item type ('c' for commodities or 'a' for activities).

        Returns:
        - new_index (pd.MultiIndex): The new multi-index for creating DataFrame slices.

        """
        if item == MI['c']:
            new_items = self.builder.master_sheet.query(f"{MI['a']}==@activity")[MI['c']].values
        if item == MI['a']:
            new_items = [deepcopy(activity)]
        
        region_ind = []
        item_ind = []
        new_items_ind = []
        for r in self.regions:
            for i in range(len(new_items)):
                region_ind.append(r)
                item_ind.append(item)
                new_items_ind.append(new_items[i])
        
        new_index = pd.MultiIndex.from_arrays([region_ind,item_ind,new_items_ind])
        return new_index
    
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
            self.add_slices(activity) # if the activity is left empty, just add empty slices
            logger.info(f"{logmsg['dm']} | Empty slices of activity '{activity}' added to matrices")
            return
        
        # get the region where to add the activity
        region = self.builder.master_sheet.query(f"{MI['a']}==@activity")[MI['r']].values[0]

        # check if the region is in the SUT or in the regions maps
        if region in self.builder.sut.get_index(MI['r']):
            target_regions = [region]
        elif region not in self.builder.sut.get_index(MI['r']):
            if region in self.builder.regions_maps:
                target_regions = self.builder.regions_maps[region] 
            else:
                raise ValueError(f"Activity {activity} is added in region {region} which is not in the SUT nor in the regions map")

        # get the inventory for the activity
        inventory = self.builder.inventories[activity]

        # if the activity must be structured based on a parent activity
        if activity in self.parented_activities: 
            parent_activity = self.builder.master_sheet.query(f"{MI['a']}==@activity")[f'Parent {MI["a"]}'].values[0]

            # copy the parent activity in the target region into the new activity inventory on u, v and e
            for region in target_regions: 
                self.slices[activity]['u'][MI['a']][1].loc[:,(region,MI['a'],activity)] = self.matrices['u'].loc[:,(region,MI['a'],parent_activity)].values
                self.slices[activity]['v'][MI['a']][1].loc[:,(region,MI['a'],activity)] = self.matrices['v'].loc[:,(region,MI['a'],parent_activity)].values
                self.slices[activity]['e'][MI['a']][1].loc[:,(region,MI['a'],activity)] = self.matrices['e'].loc[:,(region,MI['a'],parent_activity)].values
                logger.info(f"{logmsg['dm']} | Activity '{activity}' initialized equal to parent activity '{parent_activity}' in region '{region}'")

        logger.info(f"{logmsg['dm']} | Converting units of inventory of activity '{activity}' consistently with the units of the SUT database")        
        inventory = self.make_units_consistent_to_database(inventory) 
        logger.info(f"{logmsg['dm']} | Units converted for activity '{activity}'")
        
        logger.info(f"{logmsg['dm']} | Filling slices for '{activity}'")
        for region_to in target_regions:
            self.fill_commodities_inputs(inventory,region_to,activity)
            self.fill_fact_sats_inputs(inventory,region_to,activity,'v')
            self.fill_fact_sats_inputs(inventory,region_to,activity,'e')
            self.fill_market_shares(activity,region_to)
            self.fill_final_demand(activity,region_to)
        logger.info(f"{logmsg['dm']} | Slices for '{activity}' filled")

        logger.info(f"{logmsg['dm']} | Adding slices for '{activity}' to matrices")
        self.add_slices(activity)
        logger.info(f"{logmsg['dm']} | Slices for '{activity}' added to matrices")

        # store eventual errors in a dictionary to retry

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
            
            # get all the necessary information of the input item
            input_item = inventory.loc[i,"DB Item"]
            if input_item == self.builder.master_sheet.query(f"{MI['a']}==@activity")[MI['c']].values[0]:
                is_new = True
            else:
                is_new = False
            quantity = inventory.loc[i,self.converted_quantity_column]
            region_from = inventory.loc[i,f"DB {MI['r']}"]
            change_type = inventory.loc[i,'Type']

            if region_from in self.builder.sut.get_index(MI['r']):
                if change_type == 'Update':
                    if is_new:
                        if region_from != region_to:
                            raise ValueError(f"Self-consumption of new commodity {input_item} by activity {activity} is allowed only if coming from the region where the activity is added ({region_to})")
                        else:                    
                            self.slices[activity]['u']['cross'].loc[(region_from,MI['c'],input_item),(region_to,MI['a'],activity)] += quantity
                    else:
                        try:
                            self.slices[activity]['u'][MI['a']][1].loc[(region_from,MI['c'],input_item),(region_to,MI['a'],activity)] += quantity
                        except:
                            raise ValueError("ERRORE: COMMODITY NON ANCORA DEFINITA")
            
            elif region_from in self.builder.regions_maps:
                if is_new:
                    raise ValueError(f"Self-consumption of new commodity {input_item} by activity {activity} is allowed only if coming from the region where the activity is added ({region_to})")

                if change_type == 'Update':
                    if input_item in self.new_commodities:
                        try:
                            com_use = self.slices[activity]['u'][MI['c']][0].loc[(self.builder.regions_maps[region_from],sn,input_item),(region_to,sn,sn)]
                        except:
                            raise ValueError("ERRORE: COMMODITY NON ANCORA DEFINITA")
                    else:
                        com_use = self.builder.sut.u.loc[(self.builder.regions_maps[region_from],sn,input_item),(region_to,sn,sn)]
                    
                    u_share = com_use.sum(1)/com_use.sum().sum()*quantity
                    if isinstance(u_share,pd.Series):
                        u_share = u_share.to_frame()
                    u_share.columns = pd.MultiIndex.from_arrays([[region_to],[MI['a']],[activity]])
                    self.slices[activity]['u'][MI['a']][1].loc[u_share.index,u_share.columns] += u_share.values

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
            change_type = inventory.loc[i, 'Type']

            if change_type == 'Update':
                self.slices[activity][matrix][MI['a']][1].loc[input_item, (region_to, MI['a'], activity)] += quantity

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
        if pd.isna(market_share):
            return
        commodity = self.builder.master_sheet.query(f"{MI['a']}==@activity")[MI['c']].values[0]

        self.slices[activity]['s']['cross'].loc[(region,MI['a'],activity),(region,MI['c'],commodity)] = market_share

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
        if pd.isna(total_output):
            return
        cons_category = self.builder.master_sheet.query(f"{MI['a']}==@activity")[MI['n']].values[0]
        cons_region = region # could be easily changed by adding a new column in the master file
        commodity = self.builder.master_sheet.query(f"{MI['a']}==@activity")[MI['c']].values[0]

        self.slices[activity]['Y'][MI['c']][0].loc[(region,MI['c'],commodity),(cons_region,MI['n'],cons_category)] += total_output

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
            raise ValueError(f"'Leave empty' column for {activity} in the master file must be boolean or left empty, got {empty} instead")

    def add_slices(self,activity):
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
        for matrix in self.slices[activity]:
            for item in self.slices[activity][matrix]:
                if item != 'cross':
                    for s in self.slices[activity][matrix][item]:
                        self.matrices[matrix] = pd.concat([self.matrices[matrix],self.slices[activity][matrix][item][s]],axis=s)

            self.matrices[matrix].fillna(0,inplace=True)
            if matrix in ['u','s']:
                self.matrices[matrix] = self.matrices[matrix].groupby(level=[0,1,2],axis=0).sum()
                self.matrices[matrix] = self.matrices[matrix].groupby(level=[0,1,2],axis=1).sum()
            if matrix in ['v','e']:
                self.matrices[matrix] = self.matrices[matrix].groupby(level=[0,1,2],axis=1).sum()
            if matrix == 'Y':
                self.matrices[matrix] = self.matrices[matrix].groupby(level=[0,1,2],axis=0).sum()

        for matrix in self.slices[activity]:
            self.matrices[matrix].fillna(0,inplace=True)
            if matrix in ['u','s']:
                self.matrices[matrix].loc[self.slices[activity][matrix]['cross'].index,self.slices[activity][matrix]['cross'].columns] += self.slices[activity][matrix]['cross'].values

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

                                    





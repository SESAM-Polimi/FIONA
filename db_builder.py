#%%
import mario

from interactions.excel.exporters import get_fiona_master_template,get_fiona_inventory_templates
from interactions.excel.readers import read_fiona_master_template,read_fiona_inventory_templates
from interactions.mario.add_inventories import add_inventories_from_master_template
from mario.tools.constants import _MASTER_INDEX as MI

from rules import setup_logger
from rules import LOG_MESSAGES as logmsg
from rules import _MASTER_SHEET_NAME as MS_name
from rules import _REGIONS_MAPS_SHEET_NAME as RMS_name
from rules import _MASTER_SHEET_COLUMNS as MS_cols
from rules import _REGIONS_MAPS_SHEET_COLUMNS as RMS_cols
from rules import _INVENTORY_SHEET_COLUMNS as InvS_cols
from rules import _ACCEPTABLES

logger = setup_logger('DB_builder')


class DB_builder():

    def __init__(
        self,
        sut_path:str,
        sut_mode:str,
        master_file_path:str,
        sut_format:str = 'txt',
        read_master_file:bool = False,
    ):
        if sut_mode not in _ACCEPTABLES['sut_modes']:
            raise ValueError(f"Mode {sut_mode} not in {_ACCEPTABLES}")
        if sut_format not in _ACCEPTABLES['sut_formats']:
            raise ValueError(f"Wrong value for sut_format. Acceptable formats: {_ACCEPTABLES['sut_formats']}")

        logger.info(f"{logmsg['r']} | Parsing SUT from {sut_path}")
        if sut_format == 'txt':
            self.sut = mario.parse_from_txt(path=sut_path,table='SUT',mode=sut_mode,)
        if sut_format == 'xlsx':
            self.sut = mario.parse_from_excel(path=sut_path,table='SUT',mode=sut_mode,)
        
        if not read_master_file:
            self.get_master_template(path=master_file_path)
        else:
            self.read_master_template(path=master_file_path)
        
        if sut_mode=='flows':
            logger.info(f"{logmsg['dm']} | Resetting SUT to coefficients")
            self.sut.reset_to_coefficients(self.sut.scenarios[0])
        
    def get_master_template(
        self,
        path:str,
    ):
        logger.info(f"{logmsg['w']} | Generating master template in {path}")
        get_fiona_master_template(self,MS_name,MS_cols,RMS_name,RMS_cols,path)

    def read_master_template(
        self,
        path:str,
        get_inventories:bool = False,
    ):
        logger.info(f"{logmsg['r']} | Reading master template from {path}")
        master_sheet, self.regions_maps_sheet = read_fiona_master_template(path,MS_name,RMS_name)
        if master_sheet.empty:
            raise ValueError("Master sheet is empty. Please fill it")
        self.master_sheet = master_sheet
        self.get_new_sets()

        if get_inventories:
            self.get_inventory_templates(path=path)
       
    def add_inventories(
        self,
        source:str,
        scenario:str = 'baseline',
    ):        
        
        if source not in _ACCEPTABLES['inventory_sources']:
            raise ValueError(f"Source {source} not in {_ACCEPTABLES}")
        
        logger.info(f"{logmsg['a']} | Erasing all scenarios but {scenario}")
        
        matrices = {
            'z': self.sut.get_data(matrices=['z'],scenarios=[scenario])[scenario][0],
            'e': self.sut.get_data(matrices=['e'],scenarios=[scenario])[scenario][0],
            'v': self.sut.get_data(matrices=['v'],scenarios=[scenario])[scenario][0],
            'Y': self.sut.get_data(matrices=['Y'],scenarios=[scenario])[scenario][0],
        }
        
        if source == 'excel':
            if not hasattr(self, 'inventories'):
                raise AttributeError("Inventories not parsed yet. Use read_inventories() first")

            if self.new_commodities != []:
                matrices,units = add_inventories_from_master_template(self,matrices,MI['c'])                    
            matrices,units = add_inventories_from_master_template(self,matrices,MI['a'])

        if source == 'FIONA':
            raise NotImplementedError("FIONA inventories not implemented yet")

        matrices['EY'] = self.sut.get_data(matrices=['EY'],scenarios=[scenario])[scenario][0]
    
        # initialize new mario instance
        self.sut = mario.Database(z=matrices['z'],e=matrices['e'],v=matrices['v'],Y=matrices['Y'],EY=matrices['EY'],units=units,table='SUT')       

    def get_new_sets(
            self,
    ):
        self.new_activities = self.master_sheet[MI['a']].unique()
        new_commodities = self.master_sheet[MI['c']].unique()

        # excluding already existing commodities
        self.new_commodities = [com for com in new_commodities if com not in self.sut.get_index(MI['c'])]

        # listing activities that have a parent
        parented_activities = []
        for act in self.new_activities:
            parent = self.master_sheet.query(f'{MI["a"]} == "{act}"')['Parent'].values[0]
            if isinstance(parent, str):
                parented_activities.append(act)
        
        # listing activities that don't have a parent
        non_parented_activites = []
        for act in self.new_activities:
            if act not in parented_activities:
                non_parented_activites.append(act)

        self.parented_activities = parented_activities
        self.non_parented_activites = non_parented_activites
        self.new_activities = list(self.new_activities)

    def get_inventory_templates(
        self,
        path:str,
        overwrite:bool=True,
    ):
        new_sheets = self.master_sheet['Sheet name'].unique()
        get_fiona_inventory_templates(new_sheets,InvS_cols,overwrite,path)    

    def read_inventories(
        self,
        path:str,
    ):
        self.inventories = read_fiona_inventory_templates(self,path)


#%%
if __name__ == '__main__':
    # sut_path = 'tests/test_SUT.xlsx'
    sut_path = 'tests/test_SUT.xlsx'
    sut_mode = 'flows'
    master_file_path = 'tests/master.xlsx'

    db = DB_builder(
        sut_path=sut_path,
        sut_mode=sut_mode,
        master_file_path=master_file_path,
        sut_format='xlsx',
        read_master_file=True,
    )

#%%
db.read_master_template(path=master_file_path,get_inventories=True)

#%%
db.read_inventories(path=master_file_path)

#%%
db.add_empty_inventories()

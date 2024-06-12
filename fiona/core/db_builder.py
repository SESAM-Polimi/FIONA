#%%
import mario

from fiona.interactions.excel.exporters import get_fiona_master_template,get_fiona_inventory_templates
from fiona.interactions.excel.readers import read_fiona_master_template,read_fiona_inventory_templates
from fiona.core.add_inventories import Inventories
from mario.tools.constants import _MASTER_INDEX as MI

from fiona.rules import setup_logger
from fiona.rules import LOG_MESSAGES as logmsg
from fiona.rules import _MASTER_SHEET_NAME as MS_name
from fiona.rules import _REGIONS_MAPS_SHEET_NAME as RMS_name
from fiona.rules import _MASTER_SHEET_COLUMNS as MS_cols
from fiona.rules import _REGIONS_MAPS_SHEET_COLUMNS as RMS_cols
from fiona.rules import _INVENTORY_SHEET_COLUMNS as InvS_cols
from fiona.rules import _ACCEPTABLES

logger = setup_logger('DB_builder')


class DB_builder():

    def __init__(
        self,
        sut_path:str or mario.Database,
        sut_mode:str,
        master_file_path:str,
        sut_format:str = 'txt',
        read_master_file:bool = False,
    ):
        """
        Initialize the DB builder object.

        Args:
            sut_path (str or mario.Database): The path to the SUT file or a mario.Database object.
            sut_mode (str): The mode of the SUT.
            master_file_path (str): The path to the master file.
            sut_format (str, optional): The format of the SUT file. Defaults to 'txt'.
            read_master_file (bool, optional): Whether to read the master file. Defaults to False.

        Raises:
            ValueError: If the sut_mode or sut_format is not acceptable.
        """

        if sut_mode not in _ACCEPTABLES['sut_modes']:
            raise ValueError(f"Mode {sut_mode} not in {_ACCEPTABLES}")
        if sut_format not in _ACCEPTABLES['sut_formats']:
            raise ValueError(f"Wrong value for sut_format. Acceptable formats: {_ACCEPTABLES['sut_formats']}")

        logger.info(f"{logmsg['r']} | Parsing SUT from {sut_path}")
        if sut_format == 'txt':
            self.sut = mario.parse_from_txt(path=sut_path,table='SUT',mode=sut_mode,)
        if sut_format == 'xlsx':
            self.sut = mario.parse_from_excel(path=sut_path,table='SUT',mode=sut_mode,)
        if sut_format == 'mario':
            self.sut = sut_path
        logger.info(f"{logmsg['r']} | SUT parsed successfully")

        if not read_master_file:
            self.get_master_template(path=master_file_path)
        else:
            self.read_master_template(path=master_file_path)

        if sut_mode=='flows':
            logger.info(f"{logmsg['dm']} | It is required to reset the SUT to coefficients")
            self.sut.reset_to_coefficients(self.sut.scenarios[0])
            logger.info(f"{logmsg['dm']} | SUT reset to coefficients")

    def get_master_template(
        self,
        path:str,
    ):
        """
        Generates a master template at the specified path.

        Args:
            path (str): The path where the master template will be generated.

        Returns:
            None
        """
        logger.info(f"{logmsg['w']} | Generating master template in {path}")
        get_fiona_master_template(self,MS_name,MS_cols,RMS_name,RMS_cols,path)
        logger.info(f"{logmsg['w']} | Master template generated")

    def read_master_template(
        self,
        path:str,
        get_inventories:bool = False,
    ):
        """
        Reads the master template from the specified path and performs necessary operations.

        Args:
            path (str): The path to the master template file.
            get_inventories (bool, optional): Flag indicating whether to get inventory templates. Defaults to False.

        Raises:
            ValueError: If the master sheet is empty.

        Returns:
            None
        """
        logger.info(f"{logmsg['r']} | Reading master template from {path}")
        master_sheet, self.regions_maps = read_fiona_master_template(self,path,MS_name,RMS_name)
        logger.info(f"{logmsg['r']} | Master template read successfully")

        self.master_sheet = master_sheet
        self.get_new_sets()
        logger.info(f"{logmsg['r']} | New activities and commodities retrieved")

        if get_inventories:
            self.get_inventory_templates(path=path)
       
    def add_inventories(
        self,
        source:str,
        scenario:str = 'baseline',
        add_to_FIONA:bool = False,
    ):        
        """
        Adds inventories to the database.a

        Args:
            source (str): The source of the inventories. Currently supports 'excel' and 'FIONA'.
            scenario (str, optional): The scenario to add the inventories to. Defaults to 'baseline'.

        Raises:
            ValueError: If the source is not one of the acceptable inventory sources.
            AttributeError: If the inventories have not been parsed yet. Use read_inventories() first.
            NotImplementedError: If the source is 'FIONA' (not implemented yet).

        Returns:
            None
        """
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

            self.Inv_builder = Inventories(self,matrices)
            self.Inv_builder.add_from_master()

            logger.info(f"{logmsg['dm']} | Inventories added to '{scenario}' scenario")
            new_matrices = {'baseline': self.Inv_builder.matrices}
            new_units = self.Inv_builder.units
            indices = self.Inv_builder.mario_indices
        
            # add to FIONA
            if add_to_FIONA:
                logger.info(f"{logmsg['w']} | Adding inventories to FIONA SQL database")

        if source == 'FIONA':
            raise NotImplementedError("FIONA inventories not implemented yet")

        new_matrices['baseline']['EY'] = self.sut.get_data(matrices=['EY'],scenarios=[scenario])[scenario][0]

        # initialize new mario instance
        logger.info(f"{logmsg['dm']} | Initializing new mario.Database instance")
        self.sut = mario.Database(
            name=None,
            table='SUT',
            source=None,
            year=None,
            init_by_parsers={"matrices": new_matrices, "_indeces": indices, "units": new_units},
            calc_all=False,
            )
        logger.info(f"{logmsg['dm']} | New mario.Database instance initialized")

    def get_new_sets(self):
        """
        Retrieves new sets of activities and commodities from the master sheet.

        Returns:
            None
        """
        self.new_activities = self.master_sheet[MI['a']].unique()
        new_commodities = self.master_sheet[MI['c']].unique()

        # excluding already existing commodities
        self.new_commodities = [com for com in new_commodities if com not in self.sut.get_index(MI['c'])]

        # listing activities that have a parent
        parented_activities = []
        for act in self.new_activities:
            parent = self.master_sheet.query(f'{MI["a"]} == "{act}"')[f'Parent {MI["a"]}'].values[0]
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
        """
        Retrieves inventory templates from the master sheet and saves them to the specified path.

        Args:
            path (str): The path where the inventory templates will be saved.
            overwrite (bool, optional): Specifies whether to overwrite existing templates. Defaults to True.
        """
        new_sheets = self.master_sheet['Sheet name'].unique()
        logger.info(f"{logmsg['w']} | Getting inventory templates from the master sheet")
        get_fiona_inventory_templates(new_sheets, self.sut.units, InvS_cols, overwrite, path)
        logger.info(f"{logmsg['w']} | Inventory templates saved to {path}")

    def read_inventories(self, path: str,check_errors:bool=False):
        """
        Reads inventory templates from the specified path and stores them in the 'inventories' attribute.

        Args:
            path (str): The path to the inventory templates.

        Returns:
            None
        """
        self.inventories = read_fiona_inventory_templates(self, path, check_errors)
        if check_errors:
            additional_log = "| No errors found"
        else:
            additional_log = ""
        logger.info(f"{logmsg['r']} | Inventories read from {path} {additional_log}")




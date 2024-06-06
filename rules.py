import logging
from mario.tools.constants import _MASTER_INDEX as MI

#%%
def setup_logger(name):
    # Create a logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Create a console handler
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)

    # Create a formatter and add it to the handler
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(handler)

    return logger

LOG_MESSAGES = {
    'r': 'PARSING',
    'w': 'EXPORTING',
    'a': 'WARNING',
    'dm': 'DATA MANAGEMENT',
}

#%%
_MASTER_SHEET_NAME = 'Master'
_REGIONS_MAPS_SHEET_NAME = 'Regions Map'

_MASTER_SHEET_COLUMNS = [
    MI['r'],
    MI['a'],
    MI['c'],
    'Sheet name',
    'FU quantity',
    'FU unit',
    'Market_share',
    'Total Output',
    'Parent',
    'Empty',
    'Reference'
    ]

_INVENTORY_SHEET_COLUMNS = [  # if change order, change also data validation
    'Quantity',
    'Unit',
    'Input',
    'Item',
    'DB Item', 
    f"DB {MI['r']}",
    'Type',
    'Reference',
]

_REGIONS_MAPS_SHEET_COLUMNS = ['GLOBAL']

#%%
_ACCEPTABLES = {
    'sut_modes': ['flows','coefficients'],
    'sut_formats': ['txt','xlsx'],
    'inventory_sources': ['FIONA','excel']
}
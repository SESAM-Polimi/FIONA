from mario.tools.constants import _MASTER_INDEX as MI

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

_INVENTORY_SHEET_COLUMNS = [
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
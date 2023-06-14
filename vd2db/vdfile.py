import pandas as pd
import pathlib

DIMENSIONS = ['Attribute', 'Commodity', 'Process', 'Period', 'Region', 'Vintage', 'TimeSlice', 'UserConstraint', 'PV']


def read_vdfile(vdfile: pathlib.Path) -> pd.DataFrame:
    params = {'comment': '*', 'header': None, 'dtype': object, 'na_values': ['-', 'NONE']}
    veda = pd.read_csv(vdfile, names=DIMENSIONS, **params).astype({'PV': float})
    veda.insert(0, 'Scenario', vdfile.stem)
    return veda


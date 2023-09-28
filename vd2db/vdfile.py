import pandas as pd
import re
import pathlib


def read_vdfile(vdfile: pathlib.Path) -> pd.DataFrame:
    with open(vdfile) as fp:
        params = {}
        while row := fp.readline().strip():
            if sre := re.search(r'^\*\s*(?P<key>[^-]+)-\s*(?P<val>.+)', row):
                params[sre.group('key')] = sre.group('val')

        options = {'comment': '*', 'header': None, 'dtype': object, 'na_values': ['-', 'NONE'],
                   'sep': params['FieldSeparator'], 'quotechar': params['TextDelim'], 'quoting': 2}

        veda = pd.read_csv(fp, names=params['Dimensions'].split(';'), **options).astype({'PV': float})
        veda.insert(0, 'Scenario', params['ImportID'].split(':')[-1])
        return veda

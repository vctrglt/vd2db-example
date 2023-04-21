import click
import pandas as pd
import numpy as np
import pathlib
from collections import defaultdict
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import create_engine, URL
from sqlalchemy.schema import MetaData, Table, Column, ForeignKey
from sqlalchemy.sql import insert, select, text, update
from sqlalchemy.types import String, Integer, Float, DateTime
from sqlalchemy.schema import MetaData, Table, Column, ForeignKey
from sqlalchemy.types import String, Integer, Float, SmallInteger
from sqlalchemy.ext.automap import automap_base
from datetime import datetime
from .vdfile import read_vdfile
from tqdm import tqdm

METADATA = ['Scenario', 'Attribute', 'Commodity', 'Process', 'Period',
            'Region', 'Vintage', 'TimeSlice', 'UserConstraint']

DIMENSIONS = ['Attribute', 'Commodity', 'Process', 'Period', 'Region', 'Vintage', 'TimeSlice', 'UserConstraint', 'PV']

APP_NAME = 'vd2db'
APP_VERSION = '0.1'

CONFIG_DIR = pathlib.Path(click.get_app_dir(APP_NAME))
DATA_DIR = pathlib.Path.home() / APP_NAME

fmt = '{desc:<25}{percentage:3.0f}%|{bar:40}|{n_fmt:>3}/{total_fmt:>3}'


@click.group()
def cli():
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)


@click.command(name='init')
@click.argument('dbname')
def init_database(dbname):
    """Initialize a new database."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    db = DATA_DIR / f'{dbname}.db'
    engine = create_engine(URL.create('sqlite', database=str(db)), echo=False)
    Base = automap_base()
    Base.prepare(engine)
    metadata = Base.metadata

    # Create dimension tables
    tables = {'Scenario': Table('Scenario', metadata,
                                Column('ID', Integer, primary_key=True),
                                Column('Name', String(255)),
                                Column('created_at', DateTime, default=datetime.now),
                                Column('updated_at', DateTime, default=datetime.now, onupdate=datetime.now))}

    tables |= {dim: Table(dim, metadata,
                          Column('ID', Integer, primary_key=True),
                          Column('Name', String(255)))
               for dim in DIMENSIONS[:-1]}

    metadata.create_all(engine)

    with engine.connect() as con:
        for dim in DIMENSIONS[:-1]:
            con.execute(insert(tables[dim]).values(ID=0, Name='<NA>'))
        con.commit()


@click.command(name='import')
@click.option('-B', '--database', 'dbname', required=True)
@click.argument('vdfile', type=click.Path(path_type=pathlib.Path))
def import_scenario(dbname, vdfile):
    """Import specified scenario."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    db = DATA_DIR / f'{dbname}.db'
    engine = create_engine(URL.create('sqlite', database=str(db)), echo=False)
    Base = automap_base()
    Base.prepare(engine)
    metadata = Base.metadata

    # Read VD File
    veda = read_vdfile(vdfile)

    # Explode data into multiple dataframes
    dataset = {}
    for attr, df in veda.groupby('Attribute'):
        dataset[attr] = df.loc[:, ~df.isna().all()].drop(columns='Attribute')

        # Create missing tables if needed
        if attr not in Base.classes:
            params = [Column('ID', Integer, primary_key=True)]
            for col in dataset[attr].columns[:-1]:
                params.append(Column(col, SmallInteger, ForeignKey(f'{col}.ID')))
            params.append(Column('PV', Float))
            Table(attr, metadata, *params)
    metadata.create_all(engine)
    Base.prepare(engine)

    # Load dictionaries
    with engine.connect() as con:
        uniques = {dim: pd.DataFrame({'Name': veda[dim].dropna().unique()}) for dim in METADATA}
        indexes = {}
        for dim, data in uniques.items():
            stmt = select(Base.classes[dim])

            # Load existing data
            cur = con.execute(stmt)
            df = pd.DataFrame.from_records(cur, columns=cur.keys())
            new_data = data[~data['Name'].isin(df['Name'])]

            # Insert new data and reload it
            if not new_data.empty:
                con.execute(insert(Base.classes[dim]), new_data.to_dict('records'))
                con.commit()
                cur = con.execute(stmt)
                df = pd.DataFrame.from_records(cur, columns=cur.keys())

            indexes[dim] = df.set_index('Name')['ID']

    with engine.connect() as con:
        pbar = tqdm(dataset.items(), bar_format=fmt)
        for attr, df in pbar:
            pbar.set_description(f'Processing {attr}')
            df = df.fillna('<NA>').replace(indexes)
            con.execute(insert(Base.classes[attr]), df.to_dict('records'))
            con.commit()


@click.command(name='update')
def update_scenario():
    """Update an existing scenario."""


@click.command(name='remove')
def remove_scenario():
    """Remove specified  scenario."""


cli.add_command(init_database)
cli.add_command(import_scenario)
cli.add_command(update_scenario)
cli.add_command(remove_scenario)
cli.epilog = f"Run 'vd2db COMMAND --help' for more information on a command."


if __name__ == '__main__':
    cli()

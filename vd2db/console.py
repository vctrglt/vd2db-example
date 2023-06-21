import click
import pandas as pd
import pathlib
from sqlalchemy.engine import create_engine, URL
from sqlalchemy.schema import MetaData, Table, Column, ForeignKey
from sqlalchemy.sql import insert, select
from sqlalchemy.types import String, Integer, Float, DateTime
from sqlalchemy.ext.automap import automap_base
from datetime import datetime
from .vdfile import read_vdfile
from rich.progress import Progress, TextColumn, BarColumn, TaskProgressColumn, MofNCompleteColumn


DIMENSIONS = ['Scenario', 'Sow', 'Commodity', 'Process', 'Period',
              'Region', 'Vintage', 'TimeSlice', 'UserConstraint']

APP_NAME = 'vd2db'
APP_VERSION = '0.1'

CONFIG_DIR = pathlib.Path(click.get_app_dir(APP_NAME))
DATA_DIR = pathlib.Path.home() / APP_NAME

fmt = '{desc:<25.20}{percentage:3.0f}%|{bar:40}|{n_fmt:>3}/{total_fmt:>3}'


@click.group()
def cli():
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)


@click.command(name='init')
@click.argument('dbname')
def init_database(dbname):
    """Initialize a new database."""
    db = DATA_DIR / f'{dbname}.db'
    engine = create_engine(URL.create('sqlite', database=str(db)), echo=False)
    metadata = MetaData()

    # Create dimension tables
    tables = {'Scenario': Table('Scenario', metadata,
                                Column('ID', Integer, primary_key=True),
                                Column('Name', String(255)),
                                Column('created_at', DateTime, default=datetime.now),
                                Column('updated_at', DateTime, default=datetime.now, onupdate=datetime.now))}

    tables |= {dim: Table(dim, metadata,
                          Column('ID', Integer, primary_key=True),
                          Column('Name', String(255)))
               for dim in DIMENSIONS[1:]}

    metadata.create_all(engine)


@click.command(name='import')
@click.argument('vdfiles', type=click.Path(path_type=pathlib.Path), nargs=-1, required=True)
@click.argument('dbname', nargs=1, required=True)
def import_scenario(vdfiles, dbname):
    """Import specified scenario."""
    db = DATA_DIR / f'{dbname}.db'
    engine = create_engine(URL.create('sqlite', database=str(db)), echo=False)
    Base = automap_base()
    Base.prepare(engine)
    metadata = Base.metadata

    for vdfile in vdfiles:
        # Read VD File
        veda = read_vdfile(vdfile)

        # Explode data into multiple dataframes
        dataset = {}
        for attr, df in veda.groupby('Attribute'):
            dataset[attr] = df.loc[:, ~df.isna().all()].drop(columns='Attribute')

        # Create missing tables
        tables = []
        for name, df in dataset.items():
            if name not in Base.classes:
                params = [Column('ID', Integer, primary_key=True)]
                for col in df.columns[:-1]:
                    params.append(Column(col, Integer, ForeignKey(f'{col}.ID', onupdate='CASCADE', ondelete='CASCADE')))
                params.append(Column('PV', Float))
                tables.append(Table(name, metadata, *params))

        # Reload mapped classes
        metadata.create_all(engine, tables=tables)
        Base.prepare(engine)

        # Load dictionaries
        with engine.connect() as con:
            uniques = {dim: pd.DataFrame({'Name': veda[dim].dropna().unique()})
                       for dim in veda.columns.intersection(DIMENSIONS)}
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

        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            MofNCompleteColumn(),
            transient=True
        ) as progress:
            with engine.connect() as con:
                # for attr, df in tqdm(dataset.items(), bar_format=fmt, desc=vdfile.stem, unit='tables'):
                for attr, df in progress.track(dataset.items(), description=f'[yellow]Processing "{vdfile.name}"'):
                    df = df.replace(indexes)
                    con.execute(insert(Base.classes[attr]), df.to_dict('records'))
                    con.commit()
                progress.print(f'[green]Processed "{vdfile.name}": {len(dataset)} tables - {len(veda)} records')


@click.command(name='update')
def update_scenario():
    """Update an existing scenario."""


@click.command(name='remove')
def remove_scenario():
    """Remove specified  scenario."""
    db = DATA_DIR / f'{dbname}.db'
    engine = create_engine(URL.create('sqlite', database=str(db)), echo=False)
    Base = automap_base()
    Base.prepare(engine)


cli.add_command(init_database)
cli.add_command(import_scenario)
cli.add_command(update_scenario)
cli.add_command(remove_scenario)
cli.epilog = f"Run 'vd2db COMMAND --help' for more information on a command."


if __name__ == '__main__':
    cli()


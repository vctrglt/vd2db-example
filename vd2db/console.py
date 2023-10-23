import click
import pandas as pd
import pathlib
from sqlalchemy.engine import create_engine, URL, Engine
from sqlalchemy.schema import MetaData, Table, Column, ForeignKey
from sqlalchemy.sql import insert, select, text, delete
from sqlalchemy.types import String, Integer, Float, DateTime
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.event import listens_for
from sqlalchemy.ext.automap import generate_relationship, interfaces
from datetime import datetime
from vd2db.vdfile import read_vdfile
from rich.progress import Progress, TextColumn, BarColumn, TaskProgressColumn, MofNCompleteColumn
from sqlite3 import Connection as SQLite3Connection

DIMENSIONS = ['Scenario', 'Attribute', 'Sow', 'Commodity', 'Process',
              'Period', 'Region', 'Vintage', 'TimeSlice', 'UserConstraint']

APP_NAME = 'vd2db'

CONFIG_DIR = pathlib.Path(click.get_app_dir(APP_NAME))
DATA_DIR = pathlib.Path.home() / APP_NAME


# enable foreign keys. if you do not do this, ON DELETE CASCADE fails silently!
@listens_for(Engine, 'connect')
def _set_sqlite_pragma(dbapi_connection, connection_record):
    if isinstance(dbapi_connection, SQLite3Connection):
        cursor = dbapi_connection.cursor()
        cursor.execute('PRAGMA foreign_keys=ON;')
        cursor.execute('PRAGMA auto_vacuum=FULL;')
        cursor.close()


def _generate_relationship(base, direction, return_fn, attrname, local_cls, referred_cls, **kw):
    if direction is interfaces.ONETOMANY:
        kw['cascade'] = 'all, delete'
        kw['passive_deletes'] = True
    return generate_relationship(base, direction, return_fn, attrname, local_cls, referred_cls, **kw)


@click.group()
@click.version_option()
def cli():
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)


@click.command(name='init')
@click.argument('dbname')
def init_database(dbname):
    """Initialize a new database."""
    db = DATA_DIR / f'{dbname}.db'
    engine = create_engine(URL.create('sqlite', database=str(db)), echo=False)
    # engine = create_engine(URL.create('mysql', host='127.0.0.1', username='root', database=dbname), echo=False)
    metadata = MetaData()

    # Create dimension tables
    Table('Scenario',
          metadata,
          Column('ID', Integer, primary_key=True),
          Column('Name', String(255), unique=True),
          Column('created_at', DateTime, default=datetime.now),
          Column('updated_at', DateTime, default=datetime.now, onupdate=datetime.now))

    for dim in DIMENSIONS[1:]:
        Table(dim,
              metadata,
              Column('ID', Integer, primary_key=True),
              Column('Name', String(255)))

    metadata.create_all(engine)


@click.command(name='list')
@click.argument('dbname', nargs=1, required=True)
def list_scenarios(dbname):
    """List scenarios in specified database."""
    db = DATA_DIR / f'{dbname}.db'
    engine = create_engine(URL.create('sqlite', database=str(db)), echo=False)
    # engine = create_engine(URL.create('mysql', host='127.0.0.1', username='root', database=dbname), echo=False)
    Base = automap_base()
    Base.prepare(engine)

    with engine.connect() as con:
        scenarios = con.execute(select(Base.classes['Scenario'].Name)).all()
        click.echo(f'{len(scenarios)} scenario(s) found in "{dbname}" database:')
        for scen in scenarios:
            click.echo(f'- {scen.Name}')


@click.command(name='import')
@click.argument('vdfile', type=click.Path(path_type=pathlib.Path), nargs=1, required=True)
@click.argument('dbname', nargs=1, required=True)
def import_scenario(vdfile, dbname):
    """Import specified scenario."""
    db = DATA_DIR / f'{dbname}.db'
    engine = create_engine(URL.create('sqlite', database=str(db)), echo=False)
    # engine = create_engine(URL.create('mysql', host='127.0.0.1', username='root', database=dbname), echo=False)
    Base = automap_base()  # noqa: N806
    Base.prepare(engine)

    # Read VD File
    scenario, veda = read_vdfile(vdfile)

    # Check if the scenario already exists
    with engine.connect() as con:
        Scenario = Base.classes['Scenario']
        row = con.execute(select(Scenario).where(Scenario.Name == scenario)).first()
        if row:
            click.echo(f'Scenario "{row.Name}" already exists.')
            click.echo('Use "remove" command first to delete it then "import" again.')
            return 1

    # Explode data into multiple dataframes
    dataset = {}
    for attr, df in veda.groupby('Attribute'):
        dataset[attr] = df.loc[:, ~df.isna().all()]

    # Create missing attribute tables and their view
    with engine.connect() as con:
        for name, df in dataset.items():
            if f'_{name}' not in Base.classes:
                # Create attribute table
                Table(
                    f'_{name}',
                    Base.metadata,
                    Column('ID', Integer, primary_key=True),
                    *[Column(col, Integer, ForeignKey(f'{col}.ID', ondelete='CASCADE'), nullable=True)
                      for col in df.columns[:-1]],
                    Column('PV', Float)
                ).create(con)

                # Create associated view
                part1 = [f'{col}.Name AS {col}' for col in df.columns[:-1]] + ['PV']
                part2 = [f'LEFT JOIN {col} ON _{name}.{col} = {col}.ID' for col in df.columns[:-1]]
                stmt = f"""CREATE VIEW {name} AS SELECT {', '.join(part1)} FROM _{name} {' '.join(part2)}"""
                con.execute(text(stmt))
        con.commit()

    # Reload attribute tables
    Base.prepare(engine)

    # Load dictionaries
    with engine.connect() as con:
        uniques = {dim: pd.DataFrame({'Name': veda[dim].dropna().unique()})
                   for dim in veda.columns.intersection(DIMENSIONS)}
        indexes = {}
        for dim, data in uniques.items():

            # Load existing data
            cur = con.execute(select(Base.classes[dim]))
            df = pd.DataFrame.from_records(cur, columns=cur.keys())

            # Insert new data
            new_data = data[~data['Name'].isin(df['Name'])]
            if not new_data.empty:
                con.execute(insert(Base.classes[dim]), new_data.to_dict('records'))
                con.commit()

            # Reload data to be sure
            cur = con.execute(select(Base.classes[dim]))
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
            for attr, df in progress.track(dataset.items(), description=f'[yellow]Processing "{vdfile.name}"'):
                cols = df.columns.intersection(indexes)
                df[cols] = df[cols].apply(lambda x: x.map(indexes[x.name].astype('Int32')))
                con.execute(insert(Base.classes[f'_{attr}']), df.to_dict('records'))
                con.commit()
            progress.print(f'[green]Processed "{vdfile.name}": {len(dataset)} tables - {len(veda)} records')


@click.command(name='remove')
@click.argument('scenario', nargs=1, required=True)
@click.argument('dbname', nargs=1, required=True)
def remove_scenario(scenario, dbname):
    """Remove specified  scenario."""
    db = DATA_DIR / f'{dbname}.db'
    engine = create_engine(URL.create('sqlite', database=str(db)), echo=False)
    Base = automap_base()
    Base.prepare(engine)

    with engine.connect() as con:
        Scenario = Base.classes['Scenario']
        con.execute(delete(Scenario).where(Scenario.Name == scenario))
        con.commit()


cli.add_command(init_database)
cli.add_command(list_scenarios)
cli.add_command(import_scenario)
cli.add_command(remove_scenario)
cli.epilog = f"Run 'vd2db COMMAND --help' for more information on a command."


if __name__ == '__main__':
    cli()

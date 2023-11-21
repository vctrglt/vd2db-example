

#  vd2db: feed database with `.vd` files 

`vd2db` is a Python program designed to populate SQLite databases using `.vd` files. This tool streamlines the process of incorporating `.vd` data into SQLite. Then, it is possible to automate the drawing of graphs. 




## Prerequisites 

- Python 3.6
- (optional) veda >= 2.010.1.4 to automate the feeding of the `.vd` with the attribute `CmdF_bot`


## Installation

To install `vd2db`, execute the following command:

```bash
  pip install git+https://github.com/corralien/vd2db.git
```
    
## Usage

`vd2db` provides a command-line interface with several commands to manage database with `.vd` files. 

To view all available options and commands, use:

```bash
  vd2db --help
```

Here's a quick breakdown of the main commands and their purposes:

- **`init`**: Initialize a new SQLite database.

- **`import`**: Import a specific scenario into the database.

- **`update`**: Update an existing scenario in the database.

- **`remove`**: Remove a specific scenario from the database.


For details on any command, use: `vd2db COMMAND --help`.

## Automating Database Feeding with `CmdF_bot` in VEDA

You can automate the process of feeding your SQLite database with `.vd` files using the `CmdF_bot` parameter in VEDA. Here's a step-by-step guide to help you set it up:

1. **Synchronize the Excel File**:
   Ensure the `.xlsx` file named `export_results` is synchronized. This file will automatically add the `vd2db` command to import the results into your database.

2. **Initialize the Database**:
   Before feeding data, initialize the database using:

```bash
vd2db init databasename.db
```

3. **Solve a Case in VEDA**:
In VEDA, solve a case that includes a regular scenario with the attribute `CmdF_bot`. Once completed, VEDA will use the `CmdF_bot` parameter to automatically run the `vd2db` command and import your results.

## Example

There is an example of a modified model DemoS_012 called `DemoS_012_vd2db` that includes a regular scenario called `export_results.xlsx` that feeds a database.
There is also two examples of graphs made with power query and python called `graph_elec.xlxs` and `graph_elec.py`. The files paths of the database and the process dictionnary `dict_vd2db.xlsx` has to be changed depending on your environment.









import pandas as pd
import matplotlib.pyplot as plt
import sqlite3

path_db = 'test_database.db'
path_group='dict_vd2db.xlsx'


SQL_query = '''
SELECT * FROM vd_VAR_FOut x WHERE Commodity IN ('ELC')
'''

conn = sqlite3.connect(path_db)
df_electricity = pd.read_sql_query(SQL_query, conn)
conn.close()


# Read process to technology mapping from CSV file
process_to_tech = pd.read_excel(path_group, sheet_name='process')
df_electricity = df_electricity.merge(process_to_tech, on='Process')

# Read color dictionary from CSV file
process_to_tech = pd.read_excel(path_group, sheet_name='color')
color_dict = pd.read_excel(path_group, sheet_name='color')
color_dict = color_dict.set_index('techno')['color'].to_dict()


# Aggregate the electricity production by technology, summing over regions
df_agg = df_electricity.groupby(['Period', 'techno'])['PV'].sum().unstack()

# Plot the stacked bar chart
ax = df_agg.plot.bar(stacked=True, color=[color_dict[col] for col in df_agg.columns], figsize=(10, 6))
plt.xlabel('Year')
plt.ylabel('Electricity Production')

# Customize the plot
plt.title('Electricity Production by Technology')
lines, labels = ax.get_legend_handles_labels()
ax.legend(lines, labels, loc='upper left')
plt.show()


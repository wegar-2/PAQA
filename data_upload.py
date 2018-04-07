import pandas as pd
import os


this_folder = os.path.dirname(os.path.abspath(__file__))
this_folder = "/home/herhor/github_repos/PAQA"
data_folder = os.path.join(this_folder, "data")

print("Data folder: ")
print(data_folder)

test_file_name = "2016_PM10_24g.xlsx"

datafile_path = os.path.join(data_folder, test_file_name)

try:
    df1 = pd.read_excel(datafile_path, header=5, index_col=0)
except Exception as exc:
    print("Dupa!")
    print(exc)
else:
    print(df1.head())
    print(df1.tail())


df3 = df1.apply(lambda x: x.str.replace(",", "."), axis=0)

colnames = df3.columns
for iter_col in colnames:
    print("Converting type of column: ", iter_col)
    df3[iter_col] = df3[iter_col].astype(float)


df3.reset_index(inplace=True, drop=False)
value_columns = df3.columns[1:]

df4 = pd.melt(df3, id_vars=["Czas pomiaru"], value_vars=value_columns)
df4.sort_values(by=["variable", "Czas pomiaru"], inplace=True)
df4.rename(columns={"variable": "place-pollution-time"})


df5 = df4.copy()

df5.loc[:, "place-pollution-time"].apply()

df5.loc[:, "city"] =

df4.to_csv(os.path.join(data_folder, "my_check.csv"))





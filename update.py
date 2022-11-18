from bs4 import BeautifulSoup
import requests
import pandas as pd

filename = './filename.csv' # path to the CSV file you want to update
df = pd.read_csv(filename, header=None)

url = "" # paste website's URL
html = requests.get(url).text
soup = BeautifulSoup(html, 'html5lib')
                      # set parameters
table = soup.find_all('table', {'class': 'wikitable'})[0] # set number of the html element. In this case it's the first table with 'wikitable' class

output_rows = []
for table_row in table.findAll('tr'):
    headers = table_row.findAll('th')
    columns = table_row.findAll('td')
    output_row = []
    for head in headers:
        output_row.append(head.text.strip())
    for column in columns:
        output_row.append(column.text.strip())
    output_rows.append(output_row)

data_to_be_checked = pd.DataFrame(output_rows)

df_tuple = tuple(df.itertuples(index=False, name=None))
dtbc_tuple = tuple(data_to_be_checked.itertuples(index=False, name=None))

list_of_rows = []

for row in dtbc_tuple:
    if row in df_tuple:
        pass
    else:
        list_of_rows.append(row)

new_data_df = pd.DataFrame(list_of_rows, index=None)

with open(filename, 'a', newline=''):
    new_data_df.to_csv(filename, mode='a', index=False, header=False)

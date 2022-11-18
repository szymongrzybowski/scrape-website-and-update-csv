from bs4 import BeautifulSoup
import requests
import pandas as pd
from apscheduler.schedulers.background import BlockingScheduler
from datetime import datetime
import re
from typing import List, Tuple

sched = BlockingScheduler(timezone="Europe/Berlin") # set timezone

filename = './filename.csv' # path to the CSV file you want to update
df = pd.read_csv(filename, header=None)

url = "" # paste website's URL
html = requests.get(url).text
soup = BeautifulSoup(html, 'html5lib')

def find_data():          # set parameters
    table = soup.find_all('table', {'class': 'wikitable'})[0] # set number of the html element. In this case it's the first table with 'wikitable' class
    
    return table

def extract_data():
    table = find_data()

    output_rows = []
    for table_row in table.findAll('tr')[2:7]: # range of rows you want scan in search of new ones. It's optional
        headers = table_row.findAll('th')
        columns = table_row.findAll('td')
        output_row = []
        for head in headers:
            head_text = head.text.strip()
            tr_head_text = re.sub(r'\[[^][]*[^][\d][^][]*]', '', head_text) # it deletes brackets e.g. "[a]"
            output_row.append(tr_head_text)
        for column in columns:
            column_text = column.text.strip()
            tr_column_text = re.sub(r'\[[^][]*[^][\d][^][]*]', '', column_text)
            output_row.append(tr_column_text)
        output_rows.append(output_row)

    data_to_be_checked = pd.DataFrame(output_rows)
    data_to_be_checked2 = data_to_be_checked.drop(data_to_be_checked.columns[[2, 4, 8, 12]], axis=1) # here you can delete specific columns from the fetched table

    return data_to_be_checked2

def convert_to_tuple() -> Tuple[tuple, tuple]:
    dtbc = extract_data()
    df_tuple = tuple(df.itertuples(index=False, name=None))
    dtbc_tuple = tuple(dtbc.itertuples(index=False, name=None))

    return df_tuple, dtbc_tuple

def create_columns() -> List[tuple]:
    list_of_tuples = []
    
    for i in df:
        a = df[i].values
        list_of_tuples.append(a)
    
    return list_of_tuples

def check_data():
    new_rows_list = []
    data = convert_to_tuple[1]

    col = create_columns()
    for t in data:
        if t[0] in col[0]: # the decision of whether to update the table or not is made based on the contents of the first and second columns
            if t[1] in col[1]:
                pass
            else:
                new_rows_list.append(t)
        else:
            if t[1] in col[1]:
                new_rows_list.append(t)
            else:
                new_rows_list.append(t)

    new_rows_df = pd.DataFrame(new_rows_list, index=None)

    if len(new_rows_df.index) > 0:
        add_rows(new_rows_df)
    else:
        pass

def add_rows(new_data_df):
    with open(filename, 'a', newline=''):
        new_data_df.to_csv(filename, mode='a', index=False, header=False)

sched.add_job(check_data, 'interval', days=2) # runs periodically on selected intervals
#sched.add_job(check_data, 'date', run_date=datetime(2022, 11, 3, 22, 34, 0)) # instead, you can choose specific datetime of task execution

sched.start()

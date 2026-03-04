import pandas as pd
import re
from io import StringIO

with open('csvs/20260228_Pedidos_csv_2026.csv', 'br') as f:
    txt = f.read().decode('utf-16')


cleaned_content = re.sub(r'[\x00\x13\x0b\xa0\x1c\x14]', r' ', txt)
cleaned_content = re.sub(r'\r\n', r'tmpcrlf', cleaned_content)
cleaned_content = re.sub(r'\n', ' ', cleaned_content)
cleaned_content = re.sub(r'tmpcrlf', '\n', cleaned_content)
print(len(cleaned_content.split('\n')))
input = StringIO(cleaned_content)

df = pd.read_csv(input, sep=';', doublequote=True, on_bad_lines='warn')
print(df.info())
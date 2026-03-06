import os
import pandas as pd
import re
from datasets import Dataset
from io import StringIO

CSV_DATA_DIR=os.path.join(os.path.abspath('.'), 'csvs')
XML_DATA_DIR=os.path.join(os.path.abspath('.'), 'csvs')
DATASET_DATA_DIR=os.path.join(os.path.abspath('.'), 'datasets')

type_pedidos = {
    'ProtocoloPedido': 'str'
}


patterns = [("pedidos", re.compile(r".*\_pedidos\_.*\.csv"), type_pedidos), 
            ("recursos", re.compile(r".*\_recursos\_.*\.csv"), None), 
            #("solicitantes", re.compile(r".*\_solicitantes\_.*\.csv"), schema_pedidos, usecols_pedidos), 
            #("pedidos_link_arquivos", re.compile(r".*\_pedidoslinkarquivo\_.*\.csv"), None, None, None), 
            #("recursos_link_arquivos", re.compile(r".*\_recursoslinkarquivo\_.*\.csv"), None, None, None)
            ]


def carrega_arquivos_csv_df(diretorio, pattern, types=None):
        df = pd.DataFrame()
        files = os.listdir(diretorio)
        files.sort(reverse=True)
            
        for file in files:
            if os.path.isfile(os.path.join(diretorio, file)) and pattern.match(file.lower()) != None:
                print(f"Carregando {file}")
                with open(os.path.join(diretorio, file), 'br') as f:
                    txt = f.read().decode('utf-16')


                cleaned_content = re.sub(r'[\x00\x13\x0b\xa0\x1c\x14]', r' ', txt)
                cleaned_content = re.sub(r'\r\n', r'tmpcrlf', cleaned_content)
                cleaned_content = re.sub(r'\n', ' ', cleaned_content)
                cleaned_content = re.sub(r'tmpcrlf', '\n', cleaned_content)
                print(len(cleaned_content.split('\n')))
                input = StringIO(cleaned_content)

                #aux = pd.read_csv(os.path.join(diretorio, file), sep=';', encoding='utf-16', on_bad_lines='warn', usecols=usecols_pedidos, dtype=schema)
                if types != None:
                    aux = pd.read_csv(input, sep=';', on_bad_lines='warn', dtype=types)
                else:
                    aux = pd.read_csv(input, sep=';', on_bad_lines='warn')

                df = pd.concat([df, aux], axis=0)
                print(f'Carregado, memória utilizada após carga: {round(df.memory_usage(deep=True).sum()/(1024*1024), 2)}MB')
        return df


def cria_datasets():
    for pattern in patterns:
        df = carrega_arquivos_csv_df(CSV_DATA_DIR, pattern[1], pattern[2])
        df.to_parquet(os.path.join(DATASET_DATA_DIR, f"{pattern[0]}_lai.parquet"), index=False)
        print(f"DF Shape: {df.shape}")
        ds = Dataset.from_pandas(df)
        print(f"DS Len: {len(ds)}")
        ds.save_to_disk(os.path.join(DATASET_DATA_DIR, f"ds_lai_{pattern[0]}"))

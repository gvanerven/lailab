import os
import pandas as pd
import re
from datasets import Dataset
from io import StringIO

CSV_DATA_DIR=os.path.join(os.path.abspath('.'), 'csvs')
XML_DATA_DIR=os.path.join(os.path.abspath('.'), 'xmls')
DATASET_DATA_DIR=os.path.join(os.path.abspath('.'), 'datasets')

type_pedidos = {
    'ProtocoloPedido': 'str'
}


patterns = [("pedidos", re.compile(r".*\_pedidos\_.*\.xml"), type_pedidos), 
            ("recursos", re.compile(r".*\_recursos\_.*\.xml"), None), 
            #("solicitantes", re.compile(r".*\_solicitantes\_.*\.csv"), schema_pedidos, usecols_pedidos), 
            #("pedidos_link_arquivos", re.compile(r".*\_pedidoslinkarquivo\_.*\.csv"), None, None, None), 
            #("recursos_link_arquivos", re.compile(r".*\_recursoslinkarquivo\_.*\.csv"), None, None, None)
            ]


def carrega_arquivos_df(diretorio, pattern, types=None):
        df = pd.DataFrame()
        files = os.listdir(diretorio)
        files.sort(reverse=True)
            
        for file in files:
            if os.path.isfile(os.path.join(diretorio, file)) and pattern.match(file.lower()) != None:
                print(f"Carregando {file}")
                with open(os.path.join(diretorio, file), 'br') as f:
                    txt = f.read().decode('utf-16')

                cleaned_content = "".join(char for char in txt if char.isprintable())
                cleaned_content = cleaned_content.replace('&#x13;', '').replace('&#x00;', '').replace('&#x0B;', '').replace('&#xA0;', '').replace('&#x1C;', '').replace('&#x14;', '').replace('&#x1F;', '').replace('&#x7F;', '').replace('&#x9F;', '')
                cleaned_content = cleaned_content.replace('&#xE000;', '&#xFFFD;').replace('&#x10000;', '&#x10FFFF;').replace('&#x20;', '&#xD7FF;').replace('&#x18;', '').replace('&#x19;', '').replace('&#x0E;', '').replace('&#x0F;', '')
                cleaned_content = cleaned_content.replace('&#x1D;', '').replace('&#x16;', '').replace('&#x11;', '').replace('&', '&amp;')
                cleaned_content = re.sub(r'\r\n', r'tmpcrlf', cleaned_content)
                cleaned_content = re.sub(r'\n', ' ', cleaned_content)
                cleaned_content = re.sub(r'tmpcrlf', '\n', cleaned_content)
                #print(len(cleaned_content.split('\n')))
                input = StringIO(cleaned_content)

                #aux = pd.read_csv(os.path.join(diretorio, file), sep=';', encoding='utf-16', on_bad_lines='warn', usecols=usecols_pedidos, dtype=schema)
                if types != None:
                    aux = pd.read_xml(input, dtype=types, encoding='utf-16')
                else:
                    aux = pd.read_xml(input, encoding='utf-16')

                df = pd.concat([df, aux], axis=0)
                print(f'Carregado, memória utilizada após carga: {round(df.memory_usage(deep=True).sum()/(1024*1024), 2)}MB')
        return df


def cria_datasets():
    for pattern in patterns:
        df = carrega_arquivos_df(XML_DATA_DIR, pattern[1], pattern[2])
        parquet_file = os.path.join(DATASET_DATA_DIR, f"{pattern[0]}_lai.parquet")
        df.to_parquet(parquet_file, index=False)
        print(f"DF Shape: {df.shape}")
        ds = Dataset.from_parquet(parquet_file)
        print(f"DS Len: {len(ds)}")
        ds.save_to_disk(os.path.join(DATASET_DATA_DIR, f"ds_lai_{pattern[0]}"))

import os
import pandas as pd
import re
from datasets import Dataset
import numpy as np


def fn_convert_int(x):
     try:
        return int(x)
     except:
        return -1

schema_pedidos = {
    "IdPedido": str,
    "ProtocoloPedido": str,
    "Esfera": str,
    "OrgaoDestinatario ": str,
    "Situacao": str,
    "DataRegistro": str,
    "ResumoSolicitacao": str,
    "DetalhamentoSolicitacao": str,
    "PrazoAtendimento": str,
    "FoiProrrogado": str,
    "FoiReencaminhado": str,
    "FormaResposta": str,
    "OrigemSolicitacao": str,
    "IdSolicitante": np.int64,
    "AssuntoPedido": str,
    "SubAssuntoPedido": str,
    "Tag": str,
    "DataResposta": str,
    "Resposta": str,
    "Decisao": str,
    "EspecificacaoDecisao": str,
    "DetalhamentoDecisao": str,
    "MotivoNegativaAcesso": str,
    "PrazoRestricaoAcesso": str
}

usecols_pedidos = [ "IdPedido",
                    "ProtocoloPedido",
                    "DataRegistro",
                    "ResumoSolicitacao",
                    "DetalhamentoSolicitacao",
                    "AssuntoPedido",
                    "SubAssuntoPedido",
                    "Tag",
                    "Resposta"]


schema_recursos = {
    "IdRecurso": str,
    "IdRecursoPrecedente": str,
    "DescRecurso": str,
    "IdPedido": str,
    "IdSolicitante": str,
    "ProtocoloPedido": str,
    "OrgaoPedido": str,
    "OrgaoDestinatario": str,
    "Instancia": str,
    "Situacao": str,
    "DataRegistro": str,
    "PrazoAtendimento": str,
    "OrigemSolicitacao": str,
    "TipoRecurso": str,
    "DataResposta": str,
    "RespostaRecurso": str,
    "TipoResposta": str
}

usecols_recursos = [
    "IdRecurso",
    "IdRecursoPrecedente",
    "DescRecurso",
    "IdPedido",
    "IdSolicitante",
    "ProtocoloPedido",
    "OrgaoPedido",
    "OrgaoDestinatario",
    "Instancia",
    "Situacao",
    "DataRegistro",
    "PrazoAtendimento",
    "OrigemSolicitacao",
    "TipoRecurso",
    "DataResposta",
    "RespostaRecurso",
    "TipoResposta"
]

ls_convert_int = ['IdPedido',
               ]

ls_convert_int_recursos = ['IdPedido',
                           "IdRecurso",
               ]

CSV_DATA_DIR=os.path.join(os.path.abspath('.'), 'csvs')
XML_DATA_DIR=os.path.join(os.path.abspath('.'), 'csvs')
DATASET_DATA_DIR=os.path.join(os.path.abspath('.'), 'datasets')


patterns = [#("pedidos", re.compile(r".*\_pedidos\_.*\.csv"), schema_pedidos, usecols_pedidos, ls_convert_int), 
            ("recursos", re.compile(r".*\_recursos\_.*\.csv"), schema_recursos, usecols_recursos, ls_convert_int_recursos), 
            #("solicitantes", re.compile(r".*\_solicitantes\_.*\.csv"), schema_pedidos, usecols_pedidos), 
            #("pedidos_link_arquivos", re.compile(r".*\_pedidoslinkarquivo\_.*\.csv"), None, None, None), 
            #("recursos_link_arquivos", re.compile(r".*\_recursoslinkarquivo\_.*\.csv"), None, None, None)
            ]


def carrega_arquivos_csv_df(diretorio, pattern, schema, usecols_pedidos, convert_int=None):
        df = pd.DataFrame()
        files = os.listdir(diretorio)
        files.sort(reverse=True)
            
        for file in files:
            if os.path.isfile(os.path.join(diretorio, file)) and pattern.match(file.lower()) != None:
                print(f"Carregando {file}")
                #aux = pd.read_csv(os.path.join(diretorio, file), sep=';', encoding='utf-16', on_bad_lines='warn', usecols=usecols_pedidos, dtype=schema)
                aux = pd.read_csv(os.path.join(diretorio, file), sep=';', encoding='utf-16', on_bad_lines='warn',  usecols=usecols_pedidos, dtype=schema)
                if convert_int != None:
                    for col in convert_int:
                        aux[col] = aux[col].apply(lambda x: fn_convert_int(x))
                        aux[col] = aux[col].astype(np.int64)
                df = pd.concat([df, aux], axis=0)
                print(f'Carregado, memória utilizada após carga: {round(df.memory_usage(deep=True).sum()/(1024*1024), 2)}MB')
        return df


def cria_datasets():
    for pattern in patterns:
        df = carrega_arquivos_csv_df(CSV_DATA_DIR, pattern[1], pattern[2], pattern[3], pattern[4])
        df.to_parquet(os.path.join(DATASET_DATA_DIR, f"{pattern[0]}_lai.parquet"), index=False)
        print(f"DF Shape: {df.shape}")
        ds = Dataset.from_pandas(df)
        print(f"DS Len: {len(ds)}")
        ds.save_to_disk(os.path.join(DATASET_DATA_DIR, f"ds_lai_{pattern[0]}"))

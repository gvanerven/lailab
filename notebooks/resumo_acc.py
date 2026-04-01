# %%
import pandas as pd
import sys

sys.path.append('/home1/gvanerven/code/lailab')
from models.classes_pydantic import RegistroPedido, ResumoPedidoSimples

from transformers import AutoModelForCausalLM, AutoTokenizer
from datasets import Dataset
from torch.utils.data import DataLoader

import torch

from tqdm import tqdm
import json
import logging

from accelerate import Accelerator

# %%
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

system_prompt = f"""
Você é um Analista de Pedidos de Acesso à Informação e deve realizar tarefas de consolidação de informações de um pedido de acesso à informação.

O pedido de acesso à informação, ou simplesmente pedido, é realizada a partir dos seguintes campos com as respectivas descrições sobre o que tratam:
    "IdPedido": Número inteiro identificando unicamente o pedido.
    "ProtocoloPedido": Número do protocolo do pedido, com 17 caracteres numéricos.
    "Orgaodestinatario": Nome do órgão do governo de destino do pedidos.
    "ResumoSolicitacao": Resumo do pedido, que pode ter um valor ou não.
    "DetalhamentoSolicitacao": O texto principal do pedido de acesso à informação.
    "AssuntoPedido": O assunto em geral do pedido selecionado de uma lista finita de opções.
    "SubAssuntoPedido": O subassunto em geral do pedido selecionado de uma lista finita de opções.
    "Tag": Palavras-chave gerais para o pedido.
    "Resposta": A resposta do órgão para o pedido.
    "Decisao": A decisão em geral do órgão ao pedido selecionado de uma lista finita de opções.
    "DetalhamentoDecisao": Informações adicionais sobre a decisão do órgão para o pedido, que pode ter um valor ou não.
    "MotivoNegativaAcesso": Motivação sobre a decisão do órgão em caso de negativa de acesso ao pedido, que pode ter um valor ou não.

O formato json do pedido possui o seguinte esquema:

{RegistroPedido.model_json_schema()}

Para o pedido, deve-se extrair as seguintes informações:
    "IdPedido": Id do Pedido analisado.
    "resumo": Corrija eventuais erros de escrita e escreva um resumo em linguagem formal de um parágrafo no máximo sobre o pedido contendo as informações mais relevantes como, por exemplo: O quê? (What): O fato, o acontecimento central; Quem? (Who): Os agentes, sujeitos envolvidos; Quando? (When): O tempo, a data ou momento do ocorrido; Onde? (Where): O local, o espaço físico onde o fato ocorreu; Como? (How): O modo, as circunstâncias em que o fato se desenrolou; Por quê? (Why): O motivo, a razão ou a causa do fato. Inlcua o número do protocolo do pedido no texto.

Extraia as informações do pedido de acesso à informação do usuário delimitado pelas tags <pedido></pedido>:

"""

accelerator = Accelerator()

model_id = "Qwen/Qwen3-8B"

tokenizer = AutoTokenizer.from_pretrained(model_id)

model = AutoModelForCausalLM.from_pretrained(
    model_id,
    tp_plan="auto",
    trust_remote_code=True,
    attn_implementation="flash_attention_2",
    dtype=torch.bfloat16,
)

sel_cols = ['IdPedido', 
            'Ano',
            'ProtocoloPedido', 
            'Orgaodestinatario', 
            'ResumoSolicitacao', 
            'DetalhamentoSolicitacao', 
            'AssuntoPedido', 
            'SubAssuntoPedido', 
            'Tag', 
            'Resposta', 
            'Decisao',
            'DetalhamentoDecisao',
            'MotivoNegativaAcesso']

# %%
ANO = sys.argv[1]

# %%
df = pd.read_parquet('/home1/gvanerven/code/lailab/etl/datasets/pedidos_lai.parquet', columns=sel_cols, filters=[('Ano', '==', ANO)])
df = df.fillna('')
print(f"DF Shape: {df.shape}")

pedidos = []
for _, row in df.iterrows():
    pedidos.append(json.dumps(RegistroPedido(**row.to_dict()).model_dump_json(), indent=2))
    
assert df.shape[0] == len(pedidos)

del df
dataset = Dataset.from_pandas(pd.DataFrame({'registro': pedidos}))
print(dataset)

def preprocess_function(pedidos):
    batch = []
    for pedido in pedidos:
        user_prompt = f"""
                <pedido>
                    {pedido}
                </pedido>
                
                Retorne o resultado contento apenas os campos do formato json abaixo:
                    {ResumoPedidoSimples.model_json_schema()}
            """
        messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ]
        text = tokenizer.apply_chat_template(
                messages,
                tokenize=False,
                add_generation_prompt=True,
                temperature = 0.01,
                enable_thinking=False
            )
        batch.append(text)
    
    return tokenizer(batch, return_tensors="pt", padding=True, truncation=True, padding_side='left')

processed_dataset = dataset.map(preprocess_function, batched=True, remove_columns=['registro'])

dataloader = DataLoader(processed_dataset, batch_size=16)

device_placement = [True, True,]
model, dataloader = accelerator.prepare(model, dataloader, device_placement=device_placement)
model.eval()
resumos_final = []
for i, batch in enumerate(dataloader):
    with torch.no_grad():
        outputs = model(**batch,
                        use_cache=True,
                        cache_implementation="static",
                        max_new_tokens=8192
            )
    # Gather outputs from all GPUs
    #gathered_outputs = accelerator.gather(outputs.logits)
    gathered_outputs = accelerator.gather(outputs)
    for gen_id in gathered_outputs:
        output_ids = gen_id[len(batch.input_ids[0]):].tolist()
        content = tokenizer.decode(output_ids, skip_special_tokens=True)
        try:
            aux = ResumoPedidoSimples(**json.loads(content)).model_dump()
            resumos_final.append(aux)

        except Exception as e:
            logger.error(f'error procesing content: {content}. ERROR: {e}')


    if (i+1) % 1000 == 0:
        tmp_df = pd.DataFrame(resumos_final)
        tmp_df.to_parquet(f"/home1/gvanerven/code/lailab/resumos/pedidos_resumos_tmp_df_batch{i}_{ANO}.parquet", index=False)



# %%
final_df = pd.DataFrame(resumos_final)
final_df.to_parquet(f"/home1/gvanerven/code/lailab/resumos/pedidos_resumos_final_{ANO}.parquet", index=False)



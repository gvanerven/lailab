from pydantic import BaseModel
from typing import List

class RegistroPedido(BaseModel):
    protocoloPedido: str
    orgaoDestinatario: str
    resumoSolicitacao: str 
    detalhamentoSolicitacao: str
    assuntoPedido: str 
    subAssuntoPedido: str
    tag: str
    resposta: str
    decisao: str
    detalhamentoDecisao: str
    motivoNegativaAcesso: str

class ResumoPedido(BaseModel):
    lide: str
    resumo: str
    entidades: str
    proposicoes: List[str]
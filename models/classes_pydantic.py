from pydantic import BaseModel
from typing import List, Optional

class RegistroPedido(BaseModel):
    idPedido: int
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
    motivoNegativaAcesso: Optional[str] = None


class ResumoPedido(BaseModel):
    tema: str
    resumo: str
    entidades: List[str]
    proposicoes: List[str]
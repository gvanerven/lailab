from pydantic import BaseModel
from typing import List, Optional

class RegistroPedido(BaseModel):
    IdPedido: int
    Ano: str
    ProtocoloPedido: str
    Orgaodestinatario: str
    ResumoSolicitacao: Optional[str] = ''
    DetalhamentoSolicitacao: str
    AssuntoPedido: Optional[str] = ''
    SubAssuntoPedido: Optional[str] = ''
    Tag: Optional[str] = ''
    Resposta: Optional[str] = ''
    Decisao: Optional[str] = ''
    DetalhamentoDecisao: Optional[str] = ''
    MotivoNegativaAcesso: Optional[str] = ''


class ResumoPedido(BaseModel):
    IdPedido: int
    tema: str
    resumo: str
    entidades: List[str]
    proposicoes: List[str]

class ResumoPedidoSimples(BaseModel):
    IdPedido: int
    resumo: str

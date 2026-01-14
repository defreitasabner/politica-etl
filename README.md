# ETL

## Extração

### Siglas
A princípio, qualquer sigla presente no [exemplo do endpoint ListarSiglasTipoProposicao](https://www.camara.leg.br/SitCamaraWS/Proposicoes.asmx/ListarSiglasTipoProposicao) poderia ser utilizado. Entretanto, selecionei algumas para compor o primeiro dataset:

| Sigla | Descrição |
| ----- | --------- |
| `PL`  | Projeto de Lei |
| `PEC` | Proposta de Emenda à Constituição |
| `PLP` | Projeto de Lei Complementar |

### Fontes
- [ListarProposições](https://www2.camara.leg.br/transparencia/dados-abertos/dados-abertos-legislativo/webservices/proposicoes-1/listarproposicoes)
- [ListarSiglasTipoProposicao](https://www2.camara.leg.br/transparencia/dados-abertos/dados-abertos-legislativo/webservices/proposicoes-1/listarsiglastipoproposicao)
- [ObterPartidosCD](https://www2.camara.leg.br/transparencia/dados-abertos/dados-abertos-legislativo/webservices/deputados/obterpartidosCD)
- [ListarProposicoesVotadasEmPlenario](https://www2.camara.leg.br/transparencia/dados-abertos/dados-abertos-legislativo/webservices/proposicoes-1/ProposicoesVotadasEmPlenario)
- [ObterVotacaoProposicao](https://www2.camara.leg.br/transparencia/dados-abertos/dados-abertos-legislativo/webservices/proposicoes-1/obtervotacaoproposicao)
- [ListarSituacoesProposicao](https://www2.camara.leg.br/transparencia/dados-abertos/dados-abertos-legislativo/webservices/proposicoes-1/listarsituacoesproposicao)

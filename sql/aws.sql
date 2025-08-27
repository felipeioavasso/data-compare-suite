select 
  nota_fiscal_id,
  sum(volume_fisico_realizado) as vol,
  sum(faturamento_bruto_realizado) as fat, 
  sum(faturamento_liquido_realizado) as fatliq,
  sum(faturamento_dolar) as fatdol,
  sum(faturamento_bruto_bonificado) as fatbon,
  sum(custo_comercializacao) as cc,
  sum(custo_producao_realizado) as cp,
  sum(custo_materiais_realizado) as ci,
  sum(custo_financeiro) as cf,
  sum(valor_frete) as frete
from schema.tabela
where tempo_id >= %(dt_inicio)s
  and status_pedido_id = ANY(%(status_lista)s)
group by nota_fiscal_id
having sum(volume_fisico_realizado) <> 0
order by nota_fiscal_id;

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
from cory_lakehouse.dbo.f_faturado
where tempo_id >= ?
-- {{STATUS_FILTER}}
group by nota_fiscal_id
having sum(volume_fisico_realizado) <> 0
order by nota_fiscal_id;

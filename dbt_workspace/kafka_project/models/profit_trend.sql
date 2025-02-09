


/* Find the profit trend over time 
   Note: we are creating a view in postgres, which is always up to date.
   Questo significa che quando nuovi dati vengono inseriti nel database, 
   la view riflette automaticamente le modifiche, perché la query viene eseguita direttamente sulle tabelle aggiornate.
*/

{{ config(materialized='view') }}

with profit_by_date as ( 
    select order_date, sum(profit) as total_profit
    from orders
    group by order_date
)

select order_date as date, total_profit as profit
from profit_by_date
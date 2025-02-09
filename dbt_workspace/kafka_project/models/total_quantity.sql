


/* Find the total quantity sold */

{{ config(materialized='view') }}

select sum(quantity) as Total_Quantity
from orders


select 
od.order_id, 
od.unit_price, 
od.quantity,
pr.product_name,
pr.product_id,
pr.supplier_id,
pr.category_id,
od.unit_price * od.quantity total,
(pr.unit_price * od.quantity) - total discount
from {{source('sources','order_details')}} od
left join {{source('sources','products')}} pr on pr.product_id = od.product_id
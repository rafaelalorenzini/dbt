with prod as (
    select 
    ct.category_name, 
    sp.company_name suppliers,
    pr.product_name,
    pr.unit_price,
    pr.product_id
    from {{source('sources', 'products')}} pr
    left join {{source('sources', 'suppliers')}} sp on (sp.supplier_id = pr.supplier_id)
    left join {{source('sources', 'categories')}} ct on (pr.category_id= ct.category_id)
)
,orderdetail as (
    select pd.*, od.order_id, od.quantity, od.discount
    from {{ref('orderdetails')}} od
    left join prod pd on (pd.product_id = od.product_id)
)
,orders as (
    select ord.order_date,
    ord.order_id,
    cs.company_name customer,
    em.name employee,
    em.age,
    em.lengthofservice
    from {{source('sources', 'orders')}} ord
    left join {{ref('customers')}} cs on (cs.customer_id=ord.customer_id)
    left join {{ref('employees')}} em on (em.employee_id=ord.employee_id)
    left join {{source('sources', 'shippers')}} sh on (ord.ship_via = sh.shipper_id)
)
,finaljoin as (select 
    od.*,
    ord.order_date,
    ord.customer,
    ord.employee,
    ord.age,
    ord.lengthofservice
    from orderdetail od
    inner join orders ord on od.order_id=ord.order_id
)
select * from finaljoin
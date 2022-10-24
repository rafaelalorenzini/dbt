select count(*) count, company_name, contact_name 
from {{ref('customers')}} 
group by 2,3
having count > 1
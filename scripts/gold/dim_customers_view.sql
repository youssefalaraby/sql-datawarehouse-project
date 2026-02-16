CREATE VIEW gold.dim_customers 
AS 
select 
ROW_NUMBER() OVER(ORDER BY ci.cst_id) as customer_key,
ci.cst_id as customer_id,
ci.cst_key as customer_name,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
loc.cntry as country,
CASE WHEN ci.cst_gndr !='unknown' then ci.cst_gndr
ELSE COALESCE(ca.gen, 'N/A')
END AS gender,
ci.cst_marital_status as marital_status,
ci.cst_create_date as create_date,
ca.bdate as birth_date
from silver.crm_cust_info as ci
LEFT JOIN silver.erp_cust_az12 as ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 as loc
on ci.cst_key = loc.cid;

CREATE VIEW gold.dim_product
AS 
SELECT 
ROW_NUMBER() OVER(ORDER BY pd.prd_start_dt) as product_key,
pd.prd_id as product_id,
pd.prd_key as product_number,
pd.prd_nm as product_name ,
pd.cat_id as category_id,
ct.cat as category,
ct.subcat as sub_category,
pd.prd_cost as product_cost,
pd.prd_line as product_line,
pd.prd_start_dt as product_start_date,
ct.maintenance as maintenance
from silver.crm_prd_info as pd
LEFT JOIN silver.erp_px_cat_g1v2 as ct
on pd.cat_id = ct.id
where pd.prd_end_dt is null;

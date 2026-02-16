# Data Catalog: Gold Layer Tables  
**SQL Data Warehouse Project**  

**Version:** 1.0  
**Date:** 2026-02-17  
**Layer:** Gold (Presentation / Consumption Layer)  
**Schema Design:** Star Schema (Fact + Dimensions)  

---

## Overview

The **Gold layer** contains the final, business-ready, clean, and enriched tables optimized for BI tools (Power BI, Tableau, Looker, etc.), self-service analytics, and reporting.  

All tables use **surrogate keys** (`*_key`) for performant joins and SCD (Slowly Changing Dimension) handling.  
Dates are stored as native `DATE` type for easy time-intelligence functions.  
Financial columns use `DECIMAL(10,2)` to support currency precision.

The three tables form a classic star schema:

- **fact_sales** (Fact table – grain = one row per order line)  
- **dim_product** (Product dimension)  
- **dim_customer** (Customer dimension)

---

## 1. Table: `fact_sales`

**Description**  
Central fact table storing every sales transaction. Contains measurable business metrics (sales amount, quantity) and foreign keys to dimensions. Perfect for revenue, order volume, delivery performance, and profitability analysis.

**Primary Key:** Composite `(order_number, product_key)` (or add a surrogate `sales_key` if needed)

| Column Name    | Data Type         | Nullable | Description                                                                 | Sample Value    |
|----------------|-------------------|----------|-----------------------------------------------------------------------------|-----------------|
| order_number   | VARCHAR(20)       | No       | Business order number (e.g., SO43697)                                       | SO43697         |
| product_key    | INT               | No       | Surrogate FK → `dim_product.product_key`                                    | 31              |
| customer_key   | INT               | No       | Surrogate FK → `dim_customer.customer_key`                                  | 10769           |
| order_date     | DATE              | No       | Date the order was placed                                                   | 2010-12-29      |
| ship_date      | DATE              | Yes      | Actual shipment date                                                        | 2011-01-05      |
| due_date       | DATE              | Yes      | Promised delivery date                                                      | 2011-01-10      |
| sales          | DECIMAL(10,2)     | No       | Total line sales amount (price × quantity)                                  | 3578.00         |
| quantity       | INT               | No       | Number of units ordered                                                     | 1               |
| price          | DECIMAL(10,2)     | No       | Unit selling price                                                          | 3578.00         |

**Notes**  
- Additive measures: `sales` and `quantity` can be summed.  
- `sales` = `price` × `quantity` (denormalized for speed).  
- Date columns enable easy DAX / SQL time intelligence (YTD, QoQ, etc.).

---

## 2. Table: `dim_product`

**Description**  
Product dimension with rich attributes for slicing sales by category, line, cost, and lifecycle. Supports product performance, inventory, and margin analysis.

**Primary Key:** `product_key` (surrogate key)

| Column Name         | Data Type       | Nullable | Description                                                      | Sample Value                  |
|---------------------|-----------------|----------|------------------------------------------------------------------|-------------------------------|
| product_key         | INT             | No       | Surrogate primary key                                            | 1                             |
| product_id          | INT             | No       | Natural / source system product ID                               | 211                           |
| product_number      | VARCHAR(25)     | No       | Product SKU / number                                             | FR-R92R-58                    |
| product_name        | VARCHAR(100)    | No       | Full product name                                                | HL Road Frame - Red - 58      |
| category_id         | VARCHAR(10)     | No       | Category code                                                    | CO_RF                         |
| category            | VARCHAR(50)     | No       | Top-level category                                               | Components                    |
| sub_category        | VARCHAR(50)     | No       | Sub-category                                                     | Road Frames                   |
| product_cost        | DECIMAL(10,2)   | No       | Standard cost (for margin calculation)                           | 0.00                          |
| product_line        | VARCHAR(50)     | No       | Product line (Road, Mountain, etc.)                              | Road                          |
| product_start_date  | DATE            | No       | Date product became active                                       | 2003-07-01                    |
| maintenance         | VARCHAR(3)      | No       | Maintenance required? ('Yes' / 'No')                             | Yes                           |

**Notes**  
- Hierarchical structure: `category` → `sub_category` → `product_line`  
- `product_cost` is used to calculate gross margin (`sales - (product_cost × quantity)`)

---

## 3. Table: `dim_customer`

**Description**  
Customer dimension containing demographic and contact attributes. Enables customer segmentation, geographic analysis, and lifetime value reporting.

**Primary Key:** `customer_key` (surrogate key)

| Column Name     | Data Type       | Nullable | Description                                                | Sample Value      |
|-----------------|-----------------|----------|------------------------------------------------------------|-------------------|
| customer_key    | INT             | No       | Surrogate primary key                                      | 1                 |
| customer_id     | INT             | No       | Natural / source system customer ID                        | 11000             |
| customer_name   | VARCHAR(50)     | No       | Full customer account identifier                           | AW00011000        |
| first_name      | VARCHAR(50)     | No       | First name                                                 | Jon               |
| last_name       | VARCHAR(50)     | No       | Last name                                                  | Yang              |
| country         | VARCHAR(50)     | No       | Country of residence                                       | Australia         |
| gender          | VARCHAR(10)     | No       | Gender                                                     | Male              |
| marital_status  | VARCHAR(20)     | No       | Marital status                                             | Married           |
| create_date     | DATE            | No       | Date the customer record was created                       | 2025-10-06        |
| birth_date      | DATE            | No       | Date of birth (used for age banding)                       | 1971-10-06        |

**Notes**  
- `birth_date` can be used to derive `age` or `age_group` in views / BI models.  
- `country` supports geographic hierarchies (can be extended with region, city later).

---

## Relationships (Star Schema)

```mermaid
graph TD
    fact_sales[ fact_sales ] -->|product_key| dim_product[dim_product]
    fact_sales -->|customer_key| dim_customer[dim_customer]

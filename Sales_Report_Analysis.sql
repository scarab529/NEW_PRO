select * 
from amazon_sale_report;

/*To keep the original Data safe we created a new table*/

create table sales_report
like amazon_sale_report;

select *
from sales_report;

insert sales_report
select * 
from amazon_sale_report;

/*In order to check if there is any duplicate data we checked simply using group by clause*/

select `order id`,`date`, sku ,count(*) as duplicate_count
from sales_report 
group by `order id`,`date`,sku 
having duplicate_count >1;

/*In order to check if there is any duplicate data we can check through this method also*/

/* select count(distinct `order id`) 
from sales_report;

select count(distinct `index` )
from sales_report;

select *, ROW_NUMBER()
over (PARTITION BY `order id`,`date`,`status`,`ship-city`,
`qty`,`amount`,`Fulfilment`,`Courier Status`,`ship-service-level`,
`Size`,`sku`,`Style`,`ship-state`) as row_num
from sales_report;

select *, ROW_NUMBER()
over (PARTITION BY `order id`) as row_num
from sales_report;

with duplicate_cte as 
( select *, ROW_NUMBER()
over (PARTITION BY `order id`) as row_num
from sales_report
)
select * from duplicate_cte where row_num >1; */

select * from sales_report
where `order id` = '171-0106620-2575543';

/*in order to remove the duplicate values wwe created duplicate_cte using partition by to 
find if there's any duplicate row present */

with duplicate_cte as 
( select * ,
row_number() over(partition by `order id`,`date`,`sku`) as row_num 
from sales_report
)
select *
from duplicate_cte
where row_num > 1;


/*To active the delete update function sometimes we need to write such codes */

SET SQL_SAFE_UPDATES = 0;

/* from the duplicate cte we used a basic where in query to remove the duplicated data*/

with duplicate_cte as 
( select * ,
row_number() over(partition by `order id`,`date`,`sku`) as row_num 
from sales_report
)
delete from sales_report
where `index` in (
select `index`
from duplicate_cte
where row_num > 1);

/*Let's now count the number of null values in each column*/
/* We will categorize columns into groups for our better understanding */
/* Transaction Details: Order-ID, Date,Qty,Amount
    Fulfillment Details: Fulfilment, Courier Status,Ship-Service-level
    Categorization:Category,Style,Size,Sku
    Geographic Information:ship-city,ship-state,ship-postal code,ship-country
    Sales Details:Sales channel,currency  */
   /*But first we will set all blanks as null*/
   
 Update sales_report
 set 
 `Order ID` = nullif(trim(`order id`),''),
 `Date` = nullif(trim(`date`),''),
 `qty` = nullif(trim(`qty`),''),
 `amount` = nullif( trim(`amount`),''),
 `Status` = NULLIF(TRIM(`Status`), ''),
 `Fulfilment` = NULLIF(TRIM(`Fulfilment`), ''),
 `Sales Channel` = NULLIF(TRIM(`Sales Channel`), ''),
 `ship-service-level` = NULLIF(TRIM(`ship-service-level`), ''),
 `Style` = NULLIF(TRIM(`Style`), ''),
 `SKU` = NULLIF(TRIM(`SKU`), ''),
 `Category` = NULLIF(TRIM(`Category`), ''),
 `Size` = NULLIF(TRIM(`Size`), ''),
 `ASIN` = NULLIF(TRIM(`ASIN`), ''),
 `Courier Status` = NULLIF(TRIM(`Courier Status`), ''),
 `currency` = NULLIF(TRIM(`currency`), ''),
  `ship-city` = NULLIF(TRIM(`ship-city`), ''),
    `ship-state` = NULLIF(TRIM(`ship-state`), ''),
    `ship-postal-code` = NULLIF(`ship-postal-code`, ''),
    `ship-country` = NULLIF(TRIM(`ship-country`), ''),
    `promotion-ids` = NULLIF(TRIM(`promotion-ids`), ''),
    `B2B` = NULLIF(TRIM(`B2B`), ''),
    `fulfilled-by` = NULLIF(TRIM(`fulfilled-by`), ''),
    `Unnamed: 22` = NULLIF(TRIM(`Unnamed: 22`), '');
 
 
-- 1st for Transaction Details
Select count(*) as total_rows,
sum(case when `order id` is null then 1 else 0 end) as null_order,
sum(case when `date` is null then 1 else 0 end) as null_date,
sum(case when `qty` is null then 1 else 0 end) as null_qty,
sum(case when `amount` is null then 1 else 0 end) as null_amt
from sales_report;

-- 2nd for Fulfillment Details
Select count(*) as total_rows,
sum(case when `Fulfilment` is null then 1 else 0 end) as null_fulfil,
sum(case when `Courier Status` is null then 1 else 0 end) as null_courier,
sum(case when `ship-service-level` is null then 1 else 0 end) as null_ssl
from sales_report;

-- 3rd for Categorization Details
Select count(*) as total_rows,
sum(case when `Category` is null then 1 else 0 end) as null_category,
sum(case when `Size` is null then 1 else 0 end) as null_size,
sum(case when `sku` is null then 1 else 0 end) as null_sku,
sum(case when `Style` is null then 1 else 0 end) as null_style
from sales_report;

-- 4th for geographic information
Select count(*) as total_rows,
sum(case when `ship-city` is null then 1 else 0 end) as null_shipcity,
sum(case when `ship-state` is null then 1 else 0 end) as null_shipstate,
sum(case when `ship-country` is null then 1 else 0 end) as null_shipcountry,
sum(case when `ship-postal-code` is null then 1 else 0 end) as null_shippost
from sales_report;

-- 5th for Fulfillment Details
Select count(*) as total_rows,
sum(case when `sales channel` is null then 1 else 0 end) as null_sales,
sum(case when `currency` is null then 1 else 0 end) as null_currency
from sales_report;

Select count(*) as total_rows,
sum(case when `ASIN` is null then 1 else 0 end) as null_asin,
sum(case when `promotion-ids` is null then 1 else 0 end) as null_promotion,
sum(case when `B2B` is null then 1 else 0 end) as null_b2b,
sum(case when `fulfilled-by` is null then 1 else 0 end) as null_fulfilled
from sales_report;

 /* From the above data we saw there was no null but there were blanks so we will first update and then write */
select * 
from sales_report
where currency is null;

select * 
from sales_report;

select distinct `courier status`
from sales_report;

select count(`courier status`)
from sales_report
where `courier status` = 'unshipped';

update sales_report
set `courier status` ='Cancelled'
where `courier status` is null;

alter table sales_report
drop column `Unnamed: 22`;

alter table sales_report
drop column `promotion-ids`;

alter table sales_report
drop column `fulfilled-by`;

select DISTINCT `ship-state`
from sales_report;

 UPDATE sales_report
SET `ship-state` = 'Punjab'
WHERE `ship-state` = 'PB';

UPDATE sales_report
SET `ship-state` = 'Punjab'
WHERE `ship-state` = 'Punjab/Mohali/Zirakpur';
 
update sales_report
set `ship-state` = case
					  when `ship-state` = 'Rajshthan' then 'RAJASTHAN'
                      when `ship-state` = 'rj' then 'RAJASTHAN'
                      when `ship-state` = 'rajsthan' then 'RAJASTHAN'
					  when `ship-state` = 'Goa' then 'GOA'
                      when `ship-state` = 'nl' then 'NAGALAND'
                      when `ship-state` = 'ar' then 'ARUNACHAL PRADESH'
                      when `ship-state` = 'orissa' then 'ODISHA'
                      when `ship-state` = 'Gujarat' then 'GUJARAT'
                      when `ship-state` = 'Pondicherry' then 'PUDUCHERRY'
                      WHEN `ship-state` = 'LAKSHADWEEP' then 'LAKSHAYDWEEP'
                      when `ship-state` is Null then 'DELHI'
                      end
 where `ship-state` in ( 'Rajshthan', 'rj','rajsthan', 'Goa','nl','ar','orissa','New Delhi','Pondicherry','Gujarat');                  


update sales_report
set `ship-state` ='DELHI'
where `SHIP-STATE` is null;

select distinct `ship-state`
from sales_report;

select distinct `ship-city`
from sales_report;

select distinct currency
from sales_report;

select *
from sales_report;

select distinct `Status`
from sales_report;

/*Standardizing data*/

UPDATE sales_report
SET `ship-city` = CONCAT(UPPER(SUBSTRING(`ship-city`, 1, 1)), LOWER(SUBSTRING(`ship-city`, 2)));

/*Udated date format using the correct format*/

update sales_report
set `date` = str_to_date(`date`, '%d-%m-%Y');

/*To check the text format*/
DESCRIBE sales_report;

SELECT `date`, DATE_FORMAT(`date`, '%d-%m-%Y') AS formatted_date
FROM sales_report;

SELECT `date`, DATE_FORMAT(`date`, '%Y-%m-%d') AS formatted_date
FROM sales_report
LIMIT 10;

UPDATE sales_report
SET `Category` = 'Unknown'
WHERE `Category` IS NULL;


select *
from sales_report;

show DATABASES;

SELECT * INTO OUTFILE ''
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
FROM your_table;


SELECT * INTO OUTFILE 'D:\Downloads PC\salesreportfile.csv'
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
FROM sales_report;

SHOW VARIABLES LIKE 'secure_file_priv';

SELECT * INTO OUTFILE 'C:\ProgramData\MySQL\MySQL Server 8.0\Uploads\salesreportfile.csv'
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
FROM sales_report;
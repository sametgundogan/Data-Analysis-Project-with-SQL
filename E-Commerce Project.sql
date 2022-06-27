
UPDATE orders_dimen
set order_date = concat(SUBSTRING(order_date,7,4),'-' , SUBSTRING(order_date,4,2),'-', SUBSTRING(order_date, 1,2))
where order_date like '[0-9][0-9]-[0-9][0-9]-[0-9][0-9][0-9][0-9]'

alter table orders_dimen
ALTER COLUMN order_date date; 

update shipping_dimen
set ship_date = concat(SUBSTRING(ship_date,7,4),'-' , SUBSTRING(ship_date,4,2),'-', SUBSTRING(ship_date, 1,2))
where ship_date like '[0-9][0-9]-[0-9][0-9]-[0-9][0-9][0-9][0-9]'

ALTER TABLE shipping_dimen
ALTER COLUMN ship_date date;

update cust_dimen
set Cust_id = SUBSTRING(Cust_id,6,6)
WHERE Cust_id like '[Cust_]%'

ALTER TABLE cust_dimen
ALTER column Cust_id int

update orders_dimen
set ord_id = SUBSTRING(ord_id,5,6)
where ord_id like '[Ord_]%'

alter TABLE orders_dimen
ALTER COLUMN ord_id int

alter TABLE prod_dimen
ALTER COLUMN prod_id int

update shipping_dimen
set Ship_id = SUBSTRING(Ship_id,5,6)
WHERE Ship_id like '[SHP_]%'

alter TABLE shipping_dimen
ALTER COLUMN ship_id int

update market_fact
set prod_id = SUBSTRING(prod_id,6,6)
WHERE prod_id like '[Prod_]%'

update market_fact
set ord_id = SUBSTRING(ord_id,5,6)
where ord_id like '[Ord_]%'

update market_fact
set Cust_id = SUBSTRING(Cust_id,6,6)
WHERE Cust_id like '[Cust_]%'

update market_fact
set Ship_id = SUBSTRING(Ship_id,5,6)
WHERE Ship_id like '[SHP_]%'

alter TABLE market_fact
ALTER COLUMN ship_id int

alter TABLE market_fact
ALTER COLUMN cust_id int

alter TABLE market_fact
ALTER COLUMN prod_id int

alter TABLE market_fact
ALTER COLUMN ord_id int

--1. Using the columns of “market_fact”, “cust_dimen”, “orders_dimen”, “prod_dimen”, “shipping_dimen”,
-- I created a new table, named as “combined_table”.
SELECT * INTO combined_table
FROM (
SELECT mf.Ord_id, mf.Prod_id, mf.Ship_id, mf.Cust_id, mf.Sales, mf.Discount, mf.Order_Quantity, mf.Product_Base_Margin,
        cd.Customer_Name, cd.Province, cd.Customer_Segment, cd.Region,
        sd.Order_ID, sd.Ship_Mode, sd.Ship_Date,
        od.Order_Date, od.Order_Priority,
        pd.Product_Category, pd.Product_Sub_Category
FROM market_fact mf
   join cust_dimen cd
   on  cd.Cust_id = mf.Cust_id
jOIN shipping_dimen sd
on sd.Ship_id = mf.Ship_id
JOIN orders_dimen od
on od.Ord_id = mf.Ord_id
JOIN prod_dimen pd
on pd.Prod_id = mf.Prod_id
) as a

--2. To find the top 3 customers who have the maximum count of orders:

select top 3  Cust_id, customer_name, count(distinct Ord_id) count_of_orders
from combined_table
GROUP by Cust_id, Customer_Name
ORDER by 3 desc

-- 3. To create a new column at combined_table as DaysTakenForDelivery that contains the date difference of Order_Date and Ship_Date:

select *, DATEDIFF(DAY, Order_Date,ship_date) DaysTakenForDelivery
from combined_table


-- 4. To find the customer whose order took the maximum time to get delivered:

SELECT *
FROM  combined_table ct, (
        select max(DATEDIFF(DAY, Order_Date,ship_date)) DaysTakenForDelivery
        from combined_table
) as df
where DATEDIFF(DAY, Order_Date,ship_date) = DaysTakenForDelivery
 
-- 5. To count the total number of unique customers in January and how many of them came back every month over the entire year in 2011:

select  COUNT( distinct Cust_id) 
from combined_table
where YEAR(Order_Date) = 2011 and MONTH(Order_Date) = 1

SELECT * 
FROM (
select Cust_id, MONTH(Order_Date) order_month, Ord_id
from combined_table
where cust_id in ( -- Customers who purchased in January
        select  distinct Cust_id
        from combined_table
        where YEAR(Order_Date) = 2011 and MONTH(Order_Date) = 1
) and YEAR(Order_Date)  = 2011
) tab
PIVOT (
        count(cust_id)
        for ORDER_month 
        in ([1],[2],[3],[4],[5],[6],[7],[8],[9],[10],[11],[12])
) as pvt_t -- up to here, the code returns monthly information about purchasing for customers who bought in January
where [1] >0 and [2] > 0 and [3] > 0 and [4] > 0 -- but to return the customer who purchased in all months
-- and since there's no customer who already purchased first 4 months, I didn't questioned for the rest.

-- 6. To return for each user the time elapsed between the first purchasing and the third purchasing, 
-- in ascending order by Customer ID:

with cte as (
        SELECT cust_id, Order_Date, row_number() OVER(PARTITION by cust_id order by order_date) order_time,
        DATEDIFF(day,Order_Date,LEAD(Order_Date,2,Order_Date) OVER (PARTITION by cust_id order by order_date)) day_diff
        from combined_table
        where Cust_id in (
                select Cust_id
                from combined_table
                GROUP by Cust_id
                HAVING COUNT(distinct Order_id) >= 3
)
)
select cust_id, day_diff
from cte
where order_time = 1
order by Cust_id

-- 7. To return customers who purchased both product 11 and product 14, as well as the ratio of these 
-- products to the total number of products purchased by the customer:

with cte as(
        select cust_id, prod_id, Order_Quantity, sum(Order_Quantity) OVER (partition by cust_id) summ
        from combined_table
        where cust_id in (
                select Cust_id
                from combined_table
                where Prod_id = 11
                INTERSECT
                select Cust_id
                from combined_table
                where Prod_id = 14
        )
)
select distinct Cust_id, cast((1.0 *  sum(Order_Quantity) over (partition by cust_id) / summ)  as decimal(2,2)) ratio
from  cte c
where prod_id in (11,14)


-----------------O-O-O--------------------
-- 1. To create a “view” that keeps visit logs of customers on a monthly basis:

CREATE VIEW  visit_logs as
select distinct Cust_id,year(order_date) year_, month(Order_Date) month_
from combined_table

-- 2. To create a “view” that keeps the number of monthly visits by users:

create or alter view quant_logs as
select *
from  (
        select year_,month_, cust_id
        from visit_logs
) tabl
PIVOT(
        count(cust_id)
        for month_
        in([1],[2],[3],[4],[5],[6],[7],[8],[9],[10],[11],[12])
) pivt


-- 3. To create the next month of the visit as a separate column, for each visit of customers: 

select *, lead(month_,1) over(partition by  cust_id, year_ order by cust_id, year_,month_) next_month_visit
from visit_logs
order by 1,2

-- 4. To calculate the monthly time gap between two consecutive visits by each customer:

select *, lead(month_,1) over(partition by  cust_id, year_ order by cust_id, year_,month_) - month_ monthly_gap
from visit_logs
order by 1,2

-- 5. To categorise customers using average time gaps. Choose the most fitted labeling model for you:


-- To solve this question, as there's no customer who purchased in every month, I thought a person counts as 'regular' 
-- if his or her monthly average purchasing ratio in a year is less than 1, otherwise 'churn'

with cte as(-- This block is taken from previous question
        select cust_id, year_, month_, 
        1.0 * sum(monthly_gap) OVER(PARTITION by cte.cust_id, cte.year_) / count(month_) over(PARTITION by cust_id,year_) aver
        from (select *, lead(month_,1) over(partition by  cust_id, year_ order by cust_id, year_,month_) - month_ monthly_gap
                from visit_logs) cte
)
select cust_id, year_,month_,case 
        when aver > 1 then 'Churn'
        when aver <= 1 then 'Regular'
        else '1 Purchase'
        END aver_type
from  cte

-- 1. To find the number of customers retained month-wise:

create or alter view time_gap as(
        select *, lead(month_,1) over(partition by  cust_id, year_ order by cust_id, year_,month_) - month_ monthly_gap
        from visit_logs
)
select * -- Retained Customers
from time_gap
where monthly_gap = 1
order by 2, 3 -- 381 rows return, that means 381 customers have retained...

-- 2. To calculate the month-wise retention rate:

create or alter view TotalCMonthly as(
        select distinct cust_id, year_,month_ ,count(cust_id) OVER(PARTITION by year_,month_) as TotalCustomersintheMonth -- Shows the month has how many customers.
        from time_gap
)
select distinct tm.*, 
        cast(1.0 * count(tg.cust_id) over(PARTITION by tg.cust_id , tg.year_,tg.month_) / TotalCustomersintheMonth as decimal (3,3)) moth_wise_retention
from time_gap tg, TotalCMonthly tm
where tg.Cust_id = tm.Cust_id and tg.monthly_gap = 1
-- In this table, month_wise_retention represents every customer who purchased next month ratio to all customers in that month.

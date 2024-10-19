--Creation of Staging table for data dump
CREATE TABLE Customers_Staging (
    Customer_Name VARCHAR(255) NOT NULL,
    Customer_Id VARCHAR(18) NOT NULL,
    Customer_Open_Date DATE NOT NULL,
    Last_Consulted_Date DATE NOT NULL,
    VAC_ID CHAR(5),
    Dr_Name VARCHAR(255),
    State CHAR(5) NOT NULL,
    Country CHAR(5) NOT NULL,
	Postal_Code INT,
    DOB DATE NOT NULL,
    Is_Active CHAR(1),
	--Age INT,
	--Days_Since_LastConsulted INT, 
	--Last_Consulted_Flag CHAR(1)
)

--inserting Data
INSERT INTO Customers_Staging (Customer_Name, Customer_Id, Customer_Open_Date, Last_Consulted_Date, VAC_ID, Dr_Name, State, Country, DOB, Is_Active) VALUES
('Alex', '123457', '2010-10-12', '2012-11-13', 'MVD', 'Paul', 'TN', 'IND', '1987-03-06', 'A'),
('Alex', '123457', '2010-10-12', '2012-10-13', 'MVD', 'Paul', 'SA', 'USA', '1987-03-06', 'A'),
('John', '123458', '2010-10-12', '2012-10-13', 'MVD', 'Paul', 'TN', 'IND', '1987-03-06', 'A'),
('Mathew', '123459', '2010-10-12', '2012-10-13', 'MVD', 'Paul', 'WAS', 'PHIL', '1987-03-06', 'A'),
('Matt', '12345', '2010-10-12', '2012-10-13', 'MVD', 'Paul', 'BOS', 'NYC', '1987-03-06', 'A'),
('Jacob', '1256', '2010-10-12', '2012-10-13', 'MVD', 'Paul', 'VIC', 'AU', '1987-03-06', 'A');

--Checking Data insertion
select * from Customers_Staging;

--Validations----------------------------------------------

--1-- Not Null attributes 
select * FROM Customers_Staging
WHERE Customer_Name IS NULL OR Customer_Id IS NULL OR Customer_Open_Date IS NULL;

--2--Date format
select 
	Customer_Open_Date,
	case when try_cast(Customer_Open_Date as date) is not null then 'Valid' else 'Invalid' end as Check1, 
	Last_Consulted_Date,
	case when try_cast(Last_Consulted_Date as date) is not null then 'Valid' else 'Invalid' end as Check2
	from Customers_Staging

--Taking distinct country into #country table
with cte as(select distinct country,ROW_NUMBER() over(partition by country order by country) rn  
from Customers_Staging) select * into #country from cte where rn = 1;

select * from #country;

--Refining Row numbers for looping purpose in next dynamic query of table creation
with cte as(select distinct country,ROW_NUMBER() over(order by country) rn  
from #country) select * into #country_refined from cte;

select * from #country_refined;


--Dynamic query to check existence and then create countrywise table automatically.
declare @i int = 1;
declare @z int = (select count(1) from #country_refined);
declare @country varchar(100);
declare @query nvarchar(max);
declare @insertquery nvarchar(max);

while @i <= @z
begin
    -- Get the country name for the current iteration
    set @country = (select country from #country_refined where rn = @i);
    
    -- Define the table name dynamically
    declare @country_table varchar(100) = 'Customers_' + @country;

    -- Check if the table already exists
    if not exists (select * from INFORMATION_SCHEMA.TABLES where table_name = @country_table)
    begin
        -- Create the table
        set @query = N'CREATE TABLE ' + QUOTENAME(@country_table) + ' (
            Customer_Name VARCHAR(255) NOT NULL,
            Customer_Id VARCHAR(18) PRIMARY KEY,
            Customer_Open_Date DATE NOT NULL,
            Last_Consulted_Date DATE NOT NULL,
            VAC_ID CHAR(5),
            Dr_Name VARCHAR(255),
            State CHAR(5) NOT NULL,
            Country CHAR(5) NOT NULL,
            Postal_Code INT,
            DOB DATE NOT NULL,
            Is_Active CHAR(1),
            Age INT,
            Days_Since_LastConsulted INT, 
            Last_Consulted_Flag CHAR(1)
        );';
        exec sp_executesql @query;
    end

    -- Prepare the insert query
    set @insertquery = N'insert into ' + QUOTENAME(@country_table) + ' 
    select 
        *,
        datediff(YEAR, DOB, GETDATE()) as Age,
        datediff(DAY, Last_Consulted_Date, GETDATE()) as Days_Since_LastConsulted,
        case when datediff(DAY, Last_Consulted_Date, GETDATE()) > 30 then ''Y'' ELSE ''N'' END as Last_Consulted_Flag
    from Customers_Staging 
    where Country = @country_name;';

    -- Execute the insert query with the parameter
    exec sp_executesql @insertquery, N'@country_name VARCHAR(100)', @country_name = @country;

    -- Increment the counter
    set @i = @i + 1;
end

--Handling Immigrations (For this part a Stored proc can be created separately)
update ci
set ci.last_consulted_date = sq.Latest_Consul_Dt
from dbo.Customers_IND ci
inner join (
    select Customer_Id, MAX(last_consulted_date) as Latest_Consul_Dt
    from Customers_Staging
    where Country <> 'ind'
    group by Customer_Id
) sq on ci.Customer_Id = sq.Customer_Id
where sq.Latest_Consul_Dt > ci.last_consulted_date;







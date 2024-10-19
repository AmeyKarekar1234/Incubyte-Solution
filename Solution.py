import pandas as pd
import pyodbc
from datetime import datetime

# Sample data for demonstration
#Approach 1
'''data = {
    'Customer_Name': ['Alex', 'John', 'Mathew', 'Matt', 'Jacob'],
    'Customer_Id': ['123457', '123458', '123459', '12345', '1256'],
    'Open_Date': ['20101012', '20101012', '20101012', '20101012', '20101012'],
    'Last_Consulted_Date': ['20121013', '20121013', '20121013', '20121013', '20121013'],
    'Vaccination_Id': ['MVD', 'MVD', 'MVD', 'MVD', 'MVD'],
    'Dr_Name': ['Paul', 'Paul', 'Paul', 'Paul', 'Paul'],
    'State': ['SA', 'TN', 'WAS', 'BOS', 'VIC'],
    'Country': ['USA', 'IND', 'PHIL', 'NYC', 'AU'],
    'DOB': ['06031987', '06031987', '06031987', '06031987', '06031987'],
    'Is_Active': ['A', 'A', 'A', 'A', 'A']
}

# Create a DataFrame  
df = pd.DataFrame(data)'''

#Approach 2
df = pd.read_csv('D:\Python\Incubyte Solution\data.txt', delimiter= '|', engine= 'python')

# Convert date columns to datetime format
df['Open_Date'] = pd.to_datetime(df['Open_Date'], format='%Y%m%d')
df['Last_Consulted_Date'] = pd.to_datetime(df['Last_Consulted_Date'], format='%Y%m%d')
df['DOB'] = pd.to_datetime(df['DOB'], format='%d%m%Y')

# Calculate derived columns
today = pd.Timestamp(datetime.now())
df['Age'] = (today - df['DOB']).dt.days // 365
df['Days_Since_LastConsulted'] = (today - df['Last_Consulted_Date']).dt.days
df['Last_Consulted_Flag'] = df['Days_Since_LastConsulted'].apply(lambda x: 'Y' if x > 30 else 'N')

# Validation: Check for mandatory fields
invalid_records = df[(df['Customer_Name'].isnull()) | 
                     (df['Customer_Id'].isnull()) | 
                     (df['Open_Date'].isnull()) | 
                     (df['Dr_Name'].isnull()) | 
                     (df['DOB'].isnull())]

if not invalid_records.empty:
    print("Invalid records found:")
    print(invalid_records)

#countrywise delta data
country_tables = {}
for country in df['Country'].unique():
    country_data = df[df['Country'] == country].copy()
    country_tables[country] = country_data

# Display the results
for country, table in country_tables.items():
    print(f"\nTable_{country}:\n", table[['Customer_Name', 'Customer_Id', 'Open_Date', 'Last_Consulted_Date', 
                                            'Vaccination_Id', 'Dr_Name', 'State', 'Country', 'DOB', 
                                            'Is_Active', 'Age', 'Days_Since_LastConsulted', 'Last_Consulted_Flag']])

# Database connection
conn = pyodbc.connect(
    "Driver={SQL Server Native Client 11.0};"
    "server=ZEUS;"
    "Database=AdventureWorksDW2016;"
    "Trusted_Connection=yes;"
)

cursor = conn.cursor()

# Create tables
conn.execute('''
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
    declare @country_table varchar(100) = 'Table_' + @country;

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
''')

# Commit changes and close the connection
conn.commit()
conn.close()

print("Data Load Completed Successfully!")

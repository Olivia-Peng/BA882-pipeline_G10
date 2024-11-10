# Census API Function Overview

As part of Phase Two's effort, we wanted to provide a little more demographic data to the CDC disease records, we did this using the Census API. Census API provides many different surveys that include demographic and socio-economic data. In this function, I used the American Community Survey from 2022. The function follows the steps below:

1. Map state names to state fips
2. Retrieve racial and income data using state fips
* The race data includes White, Black, Native American, Asian
* Socioeconomic data includes income
3. Create a schema and table if not exist
4. Finally, insert the table into BigQuery


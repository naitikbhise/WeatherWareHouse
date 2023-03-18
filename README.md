# WeatherWareHouse
Weather Warehouse is a github dedicated to the creation of the data warehouse for the storage of the climate data cleaned afrom meteomatics.

# API Documentation
## Overview
API Documentation
Overview
This API provides weather data for various locations.

The API supports the following endpoints:

1. /locations
2. /temperatures
3. /precipitations
4. /winds
5. /averages

### Endpoints

1. /locations
Returns a list of available locations.

Request
Method: GET
Endpoint: /locations
Response
Status Code: 200 OK
Content Type: application/json
Example Response
css
Copy code
[    {        "location": "New York"    },    {        "location": "Los Angeles"    },    {        "location": "San Francisco"    }]

2. /temperatures
Returns temperature data.

Request
Method: GET
Endpoint: /temperatures
Query Parameters
date_start : Start date for the data, in the format YYYY-MM-DD.
day_count : Number of days to include in the data. Must be a positive integer between 1 and 10.
location : Location to filter the data for.
statistics (optional): If set to 1, will return statistics for each location and date (average temperature and maximum temperature).
Response
Status Code: 200 OK
Content Type: application/json
Example Response
css
Copy code
[    {        "date": "2023-03-14",        "location": "New York",        "temperature": 12.5    },    {        "date": "2023-03-14",        "location": "Los Angeles",        "temperature": 21.3    },    {        "date": "2023-03-14",        "location": "San Francisco",        "temperature": 15.6    }]

3. /precipitations
Returns precipitation data.

Request
Method: GET
Endpoint: /precipitations
Query Parameters
date_start : Start date for the data, in the format YYYY-MM-DD.
day_count : Number of days to include in the data. Must be a positive integer between 1 and 10.
location : Location to filter the data for.
statistics : If set to 1, will return statistics for each location and date (average precipitation and maximum precipitation).
Response
Status Code: 200 OK
Content Type: application/json
Example Response
css
Copy code
[    {        "date": "2023-03-14",        "location": "New York",        "precipitation": 3.5    },    {        "date": "2023-03-14",        "location": "Los Angeles",        "precipitation": 0.0    },    {        "date": "2023-03-14",        "location": "San Francisco",        "precipitation": 1.2    }]

4. /winds
Returns wind data.

Request
Method: GET
Endpoint: /winds
Query Parameters
date_start : Start date for the data, in the format YYYY-MM-DD.
day_count (optional): Number of days to include in the data. Must be a positive integer between 1 and 10.
location (optional): Location to filter the data for.
statistics (optional): If set to 1, will return statistics for each location and date (average wind
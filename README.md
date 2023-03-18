# API Documentation
## Overview

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

### Request
- Method: GET
- Endpoint: /locations

### Response
- Status Code: 200 OK
- Content Type: application/json
### Example Response

"""
[    {        "location": "New York"    },    {        "location": "Los Angeles"    },    {        "location": "San Francisco"    }]
"""
2. /temperatures
Returns temperature data.

### Request
- Method: GET
- Endpoint: /temperatures
### Query Parameters
- date_start : Start date for the data, in the format YYYY-MM-DD.
- day_count : Number of days to include in the data. Must be a positive integer between 1 and 10.
- location : Location to filter the data for.
- statistics (optional): If set to 1, will return statistics for each location and date (average temperature and maximum temperature).
### Response
- Status Code: 200 OK
- Content Type: application/json
### Example Response
"""
[    {        "date": "2023-03-14T00:00:00",        "location": "New York",        "temperature": 12.5    },    {        "date": "2023-03-14T00:00:00",        "location": "San Francisco",        "temperature": 21.3    },    {        "date": "2023-03-14T00:00:00",        "location": "Mumbai",        "temperature": 35.6    }]"""

3. /precipitations
Returns precipitation data.

### Request
- Method: GET
- Endpoint: /precipitations
### Query Parameters
- date_start : Start date for the data, in the format YYYY-MM-DD.
- day_count : Number of days to include in the data. Must be a positive integer between 1 and 10.
- location : Location to filter the data for.
- statistics (optional): If set to 1, will return statistics for each location and date (average precipitation and maximum precipitation).
### Response
- Status Code: 200 OK
- Content Type: application/json
### Example Response

"""[    {        "date": "2023-03-14T00:00:00",        "location": "New York",        "precipitation": 3.5    },    {        "date": "2023-03-14T00:00:00",        "location": "Los Angeles",        "precipitation": 0.0    },    {        "date": "2023-03-14T00:00:00",        "location": "San Francisco",        "precipitation": 1.2    }]"""

4. /winds
Returns precipitation data.

### Request
- Method: GET
- Endpoint: /winds
### Query Parameters
- date_start : Start date for the data, in the format YYYY-MM-DD.
- day_count : Number of days to include in the data. Must be a positive integer between 1 and 10.
- location : Location to filter the data for.
- statistics (optional): If set to 1, will return statistics for each location and date (average precipitation and maximum precipitation).
### Response
- Status Code: 200 OK
- Content Type: application/json
### Example Response

"""[    {        "date": "2023-03-14T00:00:00",        "location": "New York",        "wind": 3.5    },    {        "date": "2023-03-14T00:00:00",        "location": "Los Angeles",        "wind": 0.0    },    {        "date": "2023-03-14T00:00:00",        "location": "San Francisco",        "wind": 1.2    }]"""

5. /averages
Returns averages for all 3 quantities.

### Request
- Method: GET
- Endpoint: /winds
### Query Parameters
- date_start : Start date for the data, in the format YYYY-MM-DD.
- day_count : Number of days to include in the data. Must be a positive integer between 1 and 10.
- location : Location to filter the data for.
- statistics (optional): If set to 1, will return statistics for each location and date (average precipitation and maximum precipitation).
### Response
- Status Code: 200 OK
- Content Type: application/json
### Example Response

"""{
        "location": "Mumbai",
        "date": "2023-03-17T00:00:00",
        "wind_avg": 2.5749999999999997,
        "temp_avg": 27.083333333333332,
        "prec_avg": 0.13041666666666665
    },
    {
        "location": "Mumbai",
        "date": "2023-03-18T00:00:00",
        "wind_avg": 2.6833333333333336,
        "temp_avg": 27.391666666666666,
        "prec_avg": 0.002916666666666667
    },
    {
        "location": "Mumbai",
        "date": "2023-03-19T00:00:00",
        "wind_avg": 2.6375,
        "temp_avg": 26.570833333333336,
        "prec_avg": 0.06333333333333334
    }"""

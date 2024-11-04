import requests

"""
    The service is defined at handle_query:server.py.
"""

# Define the URL and query parameters
url = "http://localhost:3000"
params = {
    "query": """
        {\"sql\":"SELECT 1\",
        \"type\":\"arrow\"}
        """
}

# params = {
#     "query": """
#         {\"sql\":"CREATE OR REPLACE TABLE test AS select 1;\",
#         \"type\":\"exec\"}
#         """
# }


# params = {
#     "query": """
#         {\"sql\":"SELECT * FROM test;\",
#         \"type\":\"json\"}
#         """
# }

# Make the GET request with query parameters
response = requests.get(url, params=params)

# Print the response
print(response.status_code)
print(response.text)
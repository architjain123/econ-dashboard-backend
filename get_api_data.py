import requests

headers = {"API": "xFBZnrnaCYvLWjjyXsAsxFUm5TJhzEvH"}
url = "https://api.safegraph.com/v2/graphql"
query = """query {
  search(
    filter: {
      address: { 
        city: "San Jose", 
        region: "CA" 
      }
    }
  ) {
    places {
      results (first:10000) {
        edges {
          node {
            safegraph_core {
              location_name
              placekey
              naics_code
            }
            monthly_patterns(start_date: "2022-01-01" end_date: "2022-01-31") {
              visits_by_day
            }
          }
        }
        }
    }
  }
}
"""


r = requests.post(url, json={'query': query}, headers=headers)
print(r.status_code)
print(r.text)




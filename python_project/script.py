import requests
import json
import snowflake.connector as connector

url = 'https://api.ycombinator.com/v0.1/companies'

response = requests.get(url)
total_number_of_pages = response.json()['totalPages']

try:
    connection = connector.connect(
            user = 'sandbox_user',
            password = r'I3i3pU%h%O^qXkxh$aq',
            account = 'oca47409.us-east-1',
            warehouse = 'RESEARCH',
            database = 'B2B_SOFTWARE_STUDY_ROLE',
            schema = 'WRITE'
        )
    cursor = connection.cursor()

    sql = 'CREATE TABLE IF NOT EXISTS B2B_SOFTWARE_STUDY.WRITE.Y_COMBINATOR_RAW_DATA (JSON variant);'
    records = cursor.execute(sql)
    print('TABLE CREATED')
    connection.commit()

    url = 'https://api.ycombinator.com/v0.1/companies'
    
    response = requests.get(url)
    total_number_of_pages = response.json()['totalPages']
    total_number_of_companies_added = 0
    for i in range(1,total_number_of_pages+1):
        print(f'WORKING ON PAGE NUMBER: {i}')
        data = requests.get(f'https://api.ycombinator.com/v0.1/companies?page={i}')
        companies = []
        for j in data.json()['companies']:
            company = {
                "id":j["id"],
                "name":j["name"].replace("\'",""),
                "slug":"" if j["slug"]==None else j["slug"],
                "website":j["website"],
                "teamSize":j["teamSize"],
                "url":j["url"],
                "batch":"" if j["batch"]==None else j["batch"],
                "status":j["status"],
                "industries":j["industries"],
                "regions":[region.replace("\'s","s") for region in j["locations"]],
                "locations":[location.replace("\'s","s") for location in j["locations"]]
            }
            companies.append(company)
        total_number_of_companies_added+=len(companies)
        company_data = json.dumps(companies)
        sql = f"""INSERT INTO B2B_SOFTWARE_STUDY.WRITE.Y_COMBINATOR_RAW_DATA (JSON) SELECT PARSE_JSON('{company_data}') ;"""
        records = cursor.execute(sql)
        print(f"PAGE {i} DATA SUCCESS. INSERTED {len(companies)} COMPANIES! {total_number_of_companies_added} COMPANIES IN TOTAL")
        connection.commit()

except Exception as e:
    print(e)
finally:
    connection.close()
    
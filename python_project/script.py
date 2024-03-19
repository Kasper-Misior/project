from typing import Dict
import requests
import json
import snowflake.connector

class YCombinatorAPI:
    """Handles interactions with the Y Combinator API."""
    BASE_URL = 'https://api.ycombinator.com/v0.1/companies'

    @staticmethod
    def get_total_pages() -> int:
        """
        Get the total number of pages available in the API.

        Returns:
        - int: The total number of pages.
        """
        total_pages = requests.get(YCombinatorAPI.BASE_URL).json()['totalPages']
        return total_pages

    @staticmethod
    def fetch_companies(page: int) -> str:
        """
        Fetch companies from a specific page of the Y Combinator API and select certain fields.

        Parameters:
        - page (int): The page number to fetch.
        
        Returns:
        - str: String containing a list of companies, each represented as a dictionary with selected fields.
        """
        response = requests.get(f'{YCombinatorAPI.BASE_URL}?page={page}')
        data = response.json()['companies']
        list_of_filtered_companies = []
        for company in data:
            company_filtered = {
                    "id":company["id"],
                    "name":company["name"].replace("\'",""),
                    "slug":"" if company["slug"]==None else company["slug"],
                    "website":company["website"],
                    "teamSize":company["teamSize"],
                    "url":company["url"],
                    "batch":"" if company["batch"]==None else company["batch"],
                    "status":company["status"],
                    "industries":company["industries"],
                    "regions":[region.replace("\'s","s") for region in company["locations"]],
                    "locations":[location.replace("\'s","s") for location in company["locations"]]
                }
            list_of_filtered_companies.append(company_filtered)
        return json.dumps(list_of_filtered_companies)
    

class SnowflakeDatabase:
    """Manages database operations in Snowflake."""
    def __init__(self, user: str, password: str, account: str, warehouse: str, database: str, schema: str):
        self.user = user
        self.password = password
        self.account = account
        self.warehouse = warehouse
        self.database = database
        self.schema = schema

    def __enter__(self) -> 'SnowflakeDatabase':
        self.connection = snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema
        )
        return self

    def __exit__(self, *args) -> None:
        self.connection.close()

    def create_table(self) -> None:
        """
        Creates a table if it doesn't exist.
        """
        with self.connection.cursor() as cursor:
            cursor.execute('CREATE TABLE IF NOT EXISTS aaa.bbb.ccc (JSON VARIANT);')
            print('TABLE CREATED')

    def insert_companies(self, companies: str) -> None:
        """
        Inserts a list of companies into the database.

        Parameters:
        - companies (str): A list of companies to insert.
        """
        with self.connection.cursor() as cursor:
            cursor.execute(f"""INSERT INTO aaa.bbb.ccc (JSON) SELECT PARSE_JSON('{companies}') ;""")

class MainProcess:
    """Orchestrates fetching data from Y Combinator API and storing it in Snowflake."""
    def __init__(self, db_params: Dict[str, str]):
        self.db = SnowflakeDatabase(**db_params)

    def run(self) -> None:
        """
        Executes the main process of fetching and storing company data.
        """
        total_pages = YCombinatorAPI.get_total_pages()
        
        with self.db as database:
            database.create_table()
            
            for page in range(1, total_pages + 1):
                print(f'WORKING ON PAGE NUMBER: {page}')
                companies = YCombinatorAPI.fetch_companies(page)
                database.insert_companies(companies)
                
                print(f"PAGE {page} DATA SUCCESS!")

db_params = {
    'user': '...',
    'password': r'...',
    'account': '...',
    'warehouse': '...',
    'database': '...',
    'schema': '...'
}

main_process = MainProcess(db_params)
main_process.run()
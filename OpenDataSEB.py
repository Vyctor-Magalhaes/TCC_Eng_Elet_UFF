import asyncio
import httpx
import pandas as pd
import requests


class ElectricSectorOpenData:
    """
    A class to fetch open data from the Brazilian Electric Sector.
    """

    def __init__(self, institution: str):
        """
        Initializes the class with the desired institution: CCEE, ONS, or ANEEL.
        Sets the base URL (host) from where the data will be fetched.
        """
        self.api_path = '/api/3/action/'  # Common CKAN API path used by all institutions

        # Sets the base host URL depending on the provided institution
        if institution.lower() == "ccee":
            self.host = 'https://dadosabertos.ccee.org.br'
        elif institution.lower() == "ons":
            self.host = 'https://dados.ons.org.br'
        elif institution.lower() == "aneel":
            self.host = 'https://dadosabertos.aneel.gov.br/'
        else:
            raise ValueError("Institution not found!")  # Raises an error for an invalid institution

    def list_available_products(self):
        """
        Returns a list of all available products (datasets) from the API.
        Each product represents a public dataset that can be queried.
        """
        response = requests.get(self.host + self.api_path + "package_list")
        return response.json()

    def __get_resource_ids_by_product(self, product: str):
        """
        Returns the IDs of the files (resources) related to a product.
        Each resource_id represents a table accessible via the API.
        """
        response = requests.get(self.host + self.api_path + f"package_show?id={product}")
        return [item['id'] for item in response.json()['result']['resources'] if 'id' in item]

    async def __fetch_offset(self, client, resource_id, offset, limit):
        """
        Asynchronous function that fetches a chunk (page) of data from a specific resource_id.
        It works with pagination (offset) and a maximum number of records (limit).
        """
        url = f"{self.host}{self.api_path}datastore_search?resource_id={resource_id}&limit={limit}&offset={offset}"
        try:
            response = await client.get(url, timeout=30)  # Performs the request asynchronously
            data = response.json()
            return data.get("result", {}).get("records", [])  # Returns only the data (records)
        except Exception as e:
            print(f"[{resource_id}] Offset {offset} failed: {e}")
            return []  # Returns an empty list in case of an error

    async def __download_full_resource(self, client, resource_id, limit=10000):
        """
        Asynchronous function that downloads all data from a single resource_id, handling pagination.
        """
        offset = 0
        all_records = []

        # Loop that fetches page by page (10,000 records at a time)
        while True:
            records = await self.__fetch_offset(client, resource_id, offset, limit)
            if not records:
                break  # Stops when there is no more data
            all_records.extend(records)  # Appends the new data
            offset += limit  # Moves to the next page

        return all_records

    async def download_full_product_data_async(self, product: str):
        """
        Main asynchronous function to download all data for a specific product.
        It accesses multiple resource_ids in parallel and combines the data into a single DataFrame.
        """
        print("Starting asynchronous download...")
        resource_ids = self.__get_resource_ids_by_product(product)  # Fetches the resource IDs

        # Creates an asynchronous HTTP client
        async with httpx.AsyncClient() as client:
            # Creates a list of asynchronous tasks, one for each resource_id
            tasks = [self.__download_full_resource(client, res_id) for res_id in resource_ids]
            # Executes all tasks concurrently
            results = await asyncio.gather(*tasks)

        # Flattens the list of lists into a single list of records
        all_records = [item for sublist in results for item in sublist]

        # Returns a DataFrame with the data (or None if no data was found)
        return pd.DataFrame(all_records) if all_records else None

    def download_full_product_data(self, product: str):
        """
        A wrapper method compatible with standard synchronous environments (like Python scripts).
        It detects if an asyncio event loop is already running (e.g., in a Jupyter Notebook) and adapts accordingly.
        """
        try:
            # If an event loop is already running (e.g., in Jupyter), create a task
            loop = asyncio.get_running_loop()
            return loop.create_task(self.download_full_product_data_async(product))
        except RuntimeError:
            # Otherwise, run the asynchronous method from scratch
            return asyncio.run(self.download_full_product_data_async(product))
        


# Example usage:
if __name__ == "__main__":
    
    client = ElectricSectorOpenData("ccee")

    # Lista os produtos disponíveis na API da CCEE
    products = client.list_available_products()
    print(products)

    # Baixa todos os dados do produto desejado como DataFrame
    df = client.download_full_product_data("geracao_horaria_submercado")
    print(df)

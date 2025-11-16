import asyncio
import json
import random
import sync_fga_database
from fga_client import FGAClient

async def main():
    client = FGAClient()
    await sync_fga_database.main()
    await client.load_store_and_model()

    n_parallel_requests = 100

    with open("tuples.json", "r") as f:
        tuples = json.load(f)

    users = [tuple['user'] for tuple in tuples if tuple["relation"] == "myself"]

    async with asyncio.TaskGroup() as tg:
        for user in users:
            tg.create_task(client.list_objects(
                user=user,
                relation="can_read",
                object_type="sensor",
            ))
    
if __name__ == "__main__":
    asyncio.run(main())

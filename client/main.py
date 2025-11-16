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

    async with asyncio.TaskGroup() as tg:
        for _ in range(n_parallel_requests):
            tuple = random.choice(tuples)
            tg.create_task(client.check_tuple(**tuple))
    
if __name__ == "__main__":
    asyncio.run(main())

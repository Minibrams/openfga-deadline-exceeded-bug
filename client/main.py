import asyncio
import json
import sync_fga_database
from fga_client import FGAClient

async def main():
    await sync_fga_database.main()
    await FGAClient().load_store_and_model()

    with open("tuples.json", "r") as f:
        tuples = json.load(f)

    users = [tuple['user'] for tuple in tuples if tuple["relation"] == "myself"]

    async def make_queries(user):
        client = FGAClient()
        await client.check_tuple(
            user=user,
            relation="myself",
            object=user,
        )

        await client.list_objects(
            user=user,
            relation="can_read",
            object_type="sensor",
        )

    async with asyncio.TaskGroup() as tg:
        for user in users:
            tg.create_task(make_queries(user))
    
if __name__ == "__main__":
    asyncio.run(main())

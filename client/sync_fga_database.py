import asyncio
import json
import subprocess

from fga_client import FGAClient, create_http_client
import settings


async def get_store(client: FGAClient) -> dict | None:
    stores = await client.list_stores(name=settings.OPENFGA_STORE_NAME)
    if stores:
        return stores[0]

    return None


async def ensure_store(client: FGAClient) -> dict:
    store = await get_store(client)
    if store is None:
        store = await client.create_store(name=settings.OPENFGA_STORE_NAME)
    return store


async def create_model(client: FGAClient, schema: dict) -> str:
    result = await client.create_model(schema)
    return result["authorization_model_id"]


async def load_models(client: FGAClient) -> list[dict]:
    result = await client.get_models()
    return result


async def ensure_seed(client: FGAClient, tuples: list[dict[str, str]]) -> None:
    all_tuples = await client.read_tuples()
    await client.delete_tuples([t["key"] for t in all_tuples])
    await client.write_tuples(tuples, ignore_duplicates=True)


def transform_model_to_json(model_file: str) -> str:
    result = subprocess.check_output(
        ["fga", "model", "transform", "--file", model_file, "--output-format", "json"],
        text=True,
    )

    d = json.loads(result)
    return json.dumps(d, sort_keys=True)


async def ensure_store_and_model(client: FGAClient):
    store = await ensure_store(client)
    client.store_id = store["id"]

    desired_model = transform_model_to_json("model.fga")

    print("Updating authorization model...")

    model_id = await create_model(client, json.loads(desired_model))
    client.model_id = model_id

    print("Authorization model updated.")


async def main():
    async with create_http_client() as http:
        client = FGAClient(http_client=http)
        await ensure_store_and_model(client)

        with open("tuples.json", "r") as f:
            seed_tuples = json.load(f)

        await ensure_seed(client, seed_tuples)
        


if __name__ == "__main__":
    asyncio.run(main())

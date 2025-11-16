import json
from typing import Any

import httpx
import settings


_client: httpx.AsyncClient | None = None
_store_id: str | None = None
_model_id: str | None = None


def _raise_for_status(response: httpx.Response) -> None:
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        raise RuntimeError(
            f"FGA API request failed: {e.response.status_code}\n{json.dumps(e.response.json(), indent=2)}"
        ) from e


def batcherize[T](lst: list[T], n: int):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def dedupe(lst: list[dict[str, str]]) -> list[dict[str, str]]:
    seen = set()
    deduped_list = []
    for item in lst:
        identifier = tuple(item.items())
        if identifier not in seen:
            seen.add(identifier)
            deduped_list.append(item)
    return deduped_list


def create_http_client(base_url: str | None = None) -> httpx.AsyncClient:
    base_url = base_url or settings.FGA_API_URL or ""
    return httpx.AsyncClient(base_url=base_url)


def get_http():
    global _client
    if _client is None:
        _client = create_http_client()
    return _client


class FGAClient:
    def __init__(
        self,
        http_client: httpx.AsyncClient = get_http(),
    ) -> None:
        self.http_client = http_client
        self.store_id = _store_id
        self.model_id = _model_id
        print(f'Using {http_client.base_url}')

    def _get_store_id(self, store_id: str | None = None) -> str:
        store_id = store_id or self.store_id
        if not store_id:
            raise ValueError("store_id must be provided")
        return store_id

    def _get_model_id(self, model_id: str | None = None) -> str:
        model_id = model_id or self.model_id
        if not model_id:
            raise ValueError("model_id must be provided")
        return model_id

    async def _paginate(
        self,
        method: str,
        endpoint: str,
        params: dict | None = None,
        body: dict | None = None,
        response_key: str | None = None,
    ):
        results = []
        continuation_token = None

        while True:
            request_params = params.copy() if params else {}
            if continuation_token:
                if method.upper() == "GET":
                    request_params["continuation_token"] = continuation_token
                else:
                    if body is None:
                        body = {}
                    body["continuation_token"] = continuation_token

            response = await self.http_client.request(
                method,
                endpoint,
                params=request_params,
                json=body,
            )

            _raise_for_status(response)

            data = response.json()
            items = data.get(response_key) if response_key else data
            results.extend(items)

            continuation_token = data.get("continuation_token")
            if not continuation_token:
                break

        return results

    # Default init
    async def load_store_and_model(self):
        """
        Automatically sets the store_id and model_id attributes based on the default store name
        and the latest authorization model in that store.
        """
        global _store_id, _model_id
        store = await self.ensure_store()
        self.store_id = store["id"]
        _store_id = store["id"]
        latest_model = await self.get_latest_model()

        if not latest_model:
            raise ValueError("No authorization model found in store")

        self.model_id = latest_model["id"]
        _model_id = latest_model["id"]

    # Stores
    async def list_stores(self, **params) -> list[dict]:
        return await self._paginate(
            "GET",
            "/stores",
            params=params | {"consistency": "MINIMIZE_LATENCY"},
            response_key="stores",
        )

    async def create_store(self, name: str) -> dict:
        response = await self.http_client.post("/stores", json={"name": name})
        _raise_for_status(response)
        return response.json()

    async def get_store(
        self,
        store_name: str | None = None,
        store_id: str | None = None,
    ) -> dict | None:
        if store_name:
            stores = await self.list_stores(name=store_name)
            if stores:
                return stores[0]
            return None

        store_id = self._get_store_id(store_id)

        response = await self.http_client.get(f"/stores/{store_id}")
        if response.status_code == 404:
            return None

        _raise_for_status(response)
        return response.json()

    async def ensure_store(self, name: str = settings.OPENFGA_STORE_NAME) -> dict:
        store = await self.get_store(store_name=name)
        if store is None:
            store = await self.create_store(name=name)
        return store

    async def delete_store(self, store_id: str | None = None) -> None:
        store_id = self._get_store_id(store_id)

        response = await self.http_client.delete(f"/stores/{store_id}")
        _raise_for_status(response)

    # Authorization Models
    async def get_models(self, store_id: str | None = None) -> list[dict]:
        store_id = self._get_store_id(store_id)
        return await self._paginate(
            "GET",
            f"/stores/{store_id}/authorization-models",
            response_key="authorization_models",
        )

    async def get_model(
        self,
        model_id: str | None = None,
        store_id: str | None = None,
    ) -> dict:
        store_id = self._get_store_id(store_id)
        model_id = self._get_model_id(model_id)

        response = await self.http_client.get(
            f"/stores/{store_id}/authorization-models/{model_id}"
        )
        _raise_for_status(response)
        return response.json()

    async def create_model(
        self,
        schema: dict,
        store_id: str | None = None,
    ) -> dict:
        store_id = self._get_store_id(store_id)

        response = await self.http_client.post(
            f"/stores/{store_id}/authorization-models",
            json=schema,
        )
        _raise_for_status(response)
        return response.json()

    async def get_latest_model(
        self,
        store_id: str | None = None,
    ) -> dict | None:
        stores = await self.get_models(store_id=store_id)
        if not stores:
            return None

        return stores[0]

    # Tuples
    async def write_tuples(
        self,
        tuples: list[dict],
        store_id: str | None = None,
        model_id: str | None = None,
        ignore_duplicates: bool = True,
    ):
        store_id = self._get_store_id(store_id)
        model_id = self._get_model_id(model_id)

        tuples = dedupe(tuples)

        for batch in batcherize(tuples, 100):
            response = await self.http_client.post(
                f"/stores/{store_id}/write",
                json={
                    "writes": {
                        "tuple_keys": batch,
                        "on_duplicate": "ignore" if ignore_duplicates else "error",
                    },
                    "authorization_model_id": model_id,
                },
            )
            _raise_for_status(response)

    async def read_tuples(
        self,
        user: str | None = None,
        relation: str | None = None,
        object: str | None = None,
    ) -> list[dict]:
        store_id = self._get_store_id()

        body: dict[str, Any] = {"consistency": "MINIMIZE_LATENCY"}
        if user or relation or object:
            body["tuple_key"] = {}
            if user:
                body["tuple_key"]["user"] = user
            if relation:
                body["tuple_key"]["relation"] = relation
            if object:
                body["tuple_key"]["object"] = object

        return await self._paginate(
            "POST",
            f"/stores/{store_id}/read",
            body=body,
            response_key="tuples",
        )

    async def delete_tuples(
        self,
        tuples: list[dict],
        store_id: str | None = None,
        model_id: str | None = None,
        ignore_missing: bool = True,
    ):
        store_id = self._get_store_id(store_id)
        model_id = self._get_model_id(model_id)

        tuples = dedupe(tuples)

        for batch in batcherize(tuples, 100):
            response = await self.http_client.post(
                f"/stores/{store_id}/write",
                json={
                    "deletes": {
                        "tuple_keys": batch,
                        "on_missing": "ignore" if ignore_missing else "error",
                    },
                    "authorization_model_id": model_id,
                },
            )
            _raise_for_status(response)

    # Queries
    async def check_tuple(
        self,
        user: str,
        relation: str,
        object: str,
        store_id: str | None = None,
        model_id: str | None = None,
    ) -> dict:
        store_id = self._get_store_id(store_id)
        model_id = self._get_model_id(model_id)

        response = await self.http_client.post(
            f"/stores/{store_id}/check",
            json={
                "tuple_key": {
                    "user": user,
                    "relation": relation,
                    "object": object,
                },
                "authorization_model_id": model_id,
                "consistency": "MINIMIZE_LATENCY",
            },
        )
        _raise_for_status(response)
        return response.json()

    async def is_allowed(
        self,
        user: str,
        relation: str,
        object: str,
        store_id: str | None = None,
        model_id: str | None = None,
    ) -> bool:
        result = await self.check_tuple(
            user=user,
            relation=relation,
            object=object,
            store_id=store_id,
            model_id=model_id,
        )

        return result.get("allowed", False)

    async def list_objects(
        self,
        user: str | None = None,
        relation: str | None = None,
        object_type: str | None = None,
        store_id: str | None = None,
        model_id: str | None = None,
        ids_only: bool = False,
    ) -> list[str]:
        store_id = self._get_store_id(store_id)
        model_id = self._get_model_id(model_id)

        objects = await self._paginate(
            "POST",
            f"/stores/{store_id}/list-objects",
            body={
                "user": user,
                "relation": relation,
                "type": object_type,
                "authorization_model_id": model_id,
                "consistency": "MINIMIZE_LATENCY",
            },
            response_key="objects",
        )

        if ids_only:
            objects = [obj.split(":") for obj in objects]
            objects = [obj_id for _, obj_id in objects]

        return objects

    async def list_users(
        self,
        object: str,
        relation: str,
        store_id: str | None = None,
        model_id: str | None = None,
        ids_only: bool = False,
    ) -> list[str]:
        store_id = self._get_store_id(store_id)
        model_id = self._get_model_id(model_id)

        object_id, object_type = object.split(":", 1)

        users = await self._paginate(
            "POST",
            f"/stores/{store_id}/list-users",
            body={
                "object": {
                    "type": object_type,
                    "id": object_id,
                },
                "relation": relation,
                "authorization_model_id": model_id,
                "consistency": "MINIMIZE_LATENCY",
            },
            response_key="users",
        )

        if ids_only:
            users = [user.split(":") for user in users]
            users = [user_id for _, user_id in users]

        return users


if __name__ == "__main__":

    async def main():
        fga = FGAClient(get_http())
        await fga.load_store_and_model()

        tuples = [
            {
                "user": "user:test",
                "relation": "myself",
                "object": "user:test",
            }
        ]

        await fga.write_tuples(tuples)
        await fga.write_tuples(tuples)

    import asyncio

    asyncio.run(main())

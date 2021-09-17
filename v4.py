import aiohttp
import asyncio
import json
import os
from aiohttp import ClientSession

URL = "https://tsa-usda-match-app.azurewebsites.net/match"


async def get_match_by_product_async(product, session):
    """Get match details by each product with ML Model API (asynchronously)"""
    try:
        response = await session.request(method="POST", url=URL, json={})
        response.raise_for_status()
        print(f"Response status ({URL}): {response.status}")
    except Exception as err:
        print(f"An error ocurred: {err}")

    response_json = await response.json()
    return response_json


async def run_program(product, session):
    """Wrapper for running program in an asynchronous manner"""
    try:
        response = await get_match_by_product_async(product, session)
        print(response.json())
    except Exception as err:
        print(f"Exception occured: {err}")
        pass


async with ClientSession() as session:
    await asyncio.gather(*[run_program(isbn, session) for isbn in LIST_ISBN])

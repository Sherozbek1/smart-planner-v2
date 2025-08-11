import asyncio
from db import init_db


async def main():
    await init_db()
    print("Database initialized!")

if __name__ == "__main__":
    asyncio.run(main())

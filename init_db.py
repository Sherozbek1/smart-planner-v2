# init_db.py
import asyncio
from sqlalchemy import text
from db import engine
from db_models import Base

INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_tasks_user_status    ON tasks (user_id, status);
CREATE INDEX IF NOT EXISTS idx_tasks_deadline       ON tasks (deadline);
CREATE INDEX IF NOT EXISTS idx_tasks_user_deadline  ON tasks (user_id, deadline);

CREATE INDEX IF NOT EXISTS idx_clans_owner          ON clans (owner_id);
CREATE INDEX IF NOT EXISTS idx_clans_approved       ON clans (is_approved);
CREATE INDEX IF NOT EXISTS idx_clan_members_clan    ON clan_members (clan_id);
CREATE INDEX IF NOT EXISTS idx_clan_members_user    ON clan_members (user_id);
CREATE INDEX IF NOT EXISTS idx_clan_apps_status     ON clan_applications (status);
CREATE INDEX IF NOT EXISTS idx_clan_apps_clan       ON clan_applications (clan_id);
"""

async def main():
    async with engine.begin() as conn:
        # 1) create tables
        await conn.run_sync(Base.metadata.create_all)
        # 2) create indexes (split & run each statement)
        for stmt in INDEX_SQL.strip().split(";"):
            s = stmt.strip()
            if not s:
                continue
            # either of these works; exec_driver_sql is simplest for DDL
            await conn.exec_driver_sql(s + ";")
            # alternatively: await conn.execute(text(s))

    print("Tables & indexes created!")

if __name__ == "__main__":
    asyncio.run(main())

# import_xp_from_backup.py
import asyncio, json, sys
from dotenv import load_dotenv

load_dotenv()

from db_helpers import get_or_create_user, get_user, update_xp_streak_completed

async def import_xp(json_path: str, apply: bool = False):
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    users = data.get("users", {})
    if not isinstance(users, dict):
        print("Backup format not recognized: missing 'users' dict")
        return

    total = len(users)
    changed = 0
    skipped = 0

    print(f"Found {total} users in backup.")
    for key, u in users.items():
        uid = str(u.get("user_id") or key)
        name = (u.get("name") or "").strip() or "User"
        xp_old = int(u.get("xp") or 0)

        # ensure user exists in current DB
        cur = await get_user(uid)
        if not cur:
            # create with name; username will be blank (can get updated on first /start)
            await get_or_create_user(uid, name)

        # fetch again to get current XP
        cur = await get_user(uid)
        xp_now = int(cur.xp or 0)
        delta = xp_old - xp_now

        if delta == 0:
            skipped += 1
            continue

        if apply:
            # add only XP, do not touch completed/streak here
            await update_xp_streak_completed(uid, xp_delta=delta, completed_delta=0)
            changed += 1
        else:
            print(f"[DRY RUN] would add {delta:+} XP to {name} ({uid}) -> {xp_now} → {xp_old}")

    if apply:
        print(f"Applied XP to {changed} users, unchanged {skipped}.")
    else:
        print(f"Dry-run complete. Differences for {total - skipped} users printed.")

if __name__ == "__main__":
    # Usage:
    #   python import_xp_from_backup.py /path/to/backup.json           (dry-run)
    #   python import_xp_from_backup.py /path/to/backup.json --apply   (apply)
    args = sys.argv[1:]
    if not args:
        print("Usage: python import_xp_from_backup.py <backup.json> [--apply]")
        sys.exit(1)
    path = args[0]
    apply = ("--apply" in args)
    asyncio.run(import_xp(path, apply=apply))

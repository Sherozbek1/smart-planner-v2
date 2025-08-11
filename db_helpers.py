from sqlalchemy.future import select
from sqlalchemy import select, func, desc, update
from sqlalchemy.orm import joinedload
from db import AsyncSessionLocal
from db_models import User, Task, Clan, ClanMember, ClanApplication, ClanCreationRequest
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from sqlalchemy.orm import selectinload
# --- USER HELPERS ---


async def get_or_create_user(user_id: str, name: str = "Unknown", username: str = ""):
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(User).where(User.id == user_id))
        user = result.scalar_one_or_none()
        if not user:
            user = User(id=user_id, name=name, username=username)
            session.add(user)
            await session.commit()
            await session.refresh(user)
        else:
            if username and user.username != username:
                user.username = username
                await session.commit()
        return user

async def get_user(user_id: str):
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(User).where(User.id == user_id))
        return result.scalar_one_or_none()

async def set_user_title(user_id: str, title: str):
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(User).where(User.id == user_id))
        user = result.scalar_one_or_none()
        if user:
            user.extra_title = title
            await session.commit()
        return user

async def set_user_goal(user_id: str, goal: str):
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(User).where(User.id == user_id))
        user = result.scalar_one_or_none()
        if user:
            user.goal_of_month = goal
            await session.commit()
        return user

async def set_user_about(user_id: str, about: str):
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(User).where(User.id == user_id))
        user = result.scalar_one_or_none()
        if user:
            user.about_me = about
            await session.commit()
        return user

async def update_xp_streak_completed(user_id: str, xp_delta=0, streak_delta=0, completed_delta=0):
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(User).where(User.id == user_id))
        user = result.scalar_one_or_none()
        if user:
            user.xp = max(0, user.xp + xp_delta)
            user.streak = max(0, user.streak + streak_delta)
            user.completed = max(0, user.completed + completed_delta)
            await session.commit()
        return user

async def count_users():
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(func.count(User.id)))
        return result.scalar()


async def get_user_by_id(user_id: str):
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(User).where(User.id == user_id))
        return result.scalar_one_or_none()

# --- TASK HELPERS ---

async def add_task(user_id: str, text: str, **kwargs):
    async with AsyncSessionLocal() as session:
        task = Task(user_id=user_id, text=text, **kwargs)
        session.add(task)
        await session.commit()
        await session.refresh(task)
        return task

async def add_tasks_bulk(user_id: str, texts: list[str], **kwargs):
    async with AsyncSessionLocal() as session:
        tasks = [Task(user_id=user_id, text=text, **kwargs) for text in texts]
        session.add_all(tasks)
        await session.commit()
        return tasks

async def get_tasks(user_id: str, status: str = "pending"):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Task).where(Task.user_id == user_id, Task.status == status)
        )
        return result.scalars().all()

async def mark_task_done(task_id: int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Task).where(Task.id == task_id))
        task = result.scalar_one_or_none()
        if task:
            task.status = "done"
            await session.commit()
        return task

async def delete_task(task_id: int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Task).where(Task.id == task_id))
        task = result.scalar_one_or_none()
        if task:
            await session.delete(task)
            await session.commit()
        return task

async def count_tasks(user_id: str):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(func.count(Task.id)).where(Task.user_id == user_id)
        )
        return result.scalar()

# --- LEADERBOARD & REMINDERS HELPERS ---

async def get_top_users(limit: int = 10):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(User).order_by(User.xp.desc()).limit(limit)
        )
        return result.scalars().all()

async def get_all_users_with_tasks():
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(User, Task)
            .join(Task, (Task.user_id == User.id))
            .where(Task.status == "pending")
        )
        pairs = result.all()

    grouped = {}
    for user, task in pairs:
        grouped.setdefault(user, []).append(task)
    return list(grouped.items())

async def update_task_deadline(task_id: int, new_deadline: str):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Task).where(Task.id == task_id)
        )
        task = result.scalar_one_or_none()
        if task:
            task.deadline = new_deadline
            task.reminders_sent = 0
            await session.commit()
        return task

async def get_task_by_id(task_id: int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Task).where(Task.id == task_id))
        return result.scalar_one_or_none()

async def update_task_priority(task_id: int, priority: str):
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Task).where(Task.id == task_id))
        task = result.scalar_one_or_none()
        if task:
            task.priority = priority
            await session.commit()
        return task

async def update_task_tags(task_id: int, tag: str):
    from db import AsyncSessionLocal
    from db_models import Task
    async with AsyncSessionLocal() as session:
        task = await session.get(Task, task_id)
        if not task:
            return None
        existing = [t for t in (task.tags or "").split(",") if t]
        if tag not in existing:
            existing.append(tag)
        task.tags = ",".join(existing)
        await session.commit()
        return task


async def update_task_text(task_id: int, new_text: str):
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Task).where(Task.id == task_id))
        task = result.scalar_one_or_none()
        if task:
            task.text = new_text
            await session.commit()
        return task
    
# --- reminders_sent persistence ---
async def update_task_reminders_sent(task_id: int, val: int) -> bool:
    from db import AsyncSessionLocal
    from db_models import Task
    async with AsyncSessionLocal() as session:
        task = await session.get(Task, task_id)
        if not task:
            return False
        task.reminders_sent = val
        await session.commit()
        return True


# --- CLANS / STUDY GROUPS HELPERS ---

async def get_all_clans():
    from db import AsyncSessionLocal
    from db_models import Clan, ClanMember
    async with AsyncSessionLocal() as session:
        stmt = (
            select(Clan)
            .options(
                selectinload(Clan.owner),
                selectinload(Clan.members).selectinload(ClanMember.user),
            )
            .order_by(Clan.created_at.desc())
        )
        res = await session.execute(stmt)
        return res.scalars().all()

    
async def get_all_approved_clans():
    # For public browsing
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Clan).options(joinedload(Clan.owner)).where(Clan.is_approved == True)
        )
        return result.scalars().all()

async def get_user_clans(user_id: str):
    from db import AsyncSessionLocal
    from db_models import Clan, ClanMember
    async with AsyncSessionLocal() as session:
        stmt = (
            select(Clan)
            .join(ClanMember, ClanMember.clan_id == Clan.id)
            .where(ClanMember.user_id == user_id)
            .options(
                selectinload(Clan.owner),
                selectinload(Clan.members).selectinload(ClanMember.user),
            )
        )
        res = await session.execute(stmt)
        return res.scalars().all()

async def get_clan_by_id(clan_id: int):
    from db import AsyncSessionLocal
    from db_models import Clan, ClanMember, ClanApplication
    async with AsyncSessionLocal() as session:
        stmt = (
            select(Clan)
            .where(Clan.id == clan_id)
            .options(
                selectinload(Clan.owner),                                  # founder
                selectinload(Clan.members).selectinload(ClanMember.user),  # members -> user
                selectinload(Clan.applications).selectinload(ClanApplication.user),  # pending apps -> user
            )
        )
        res = await session.execute(stmt)
        return res.scalar_one_or_none()


async def create_clan_application(clan_id: int, user_id: str):
    async with AsyncSessionLocal() as session:
        # Check if already applied
        result = await session.execute(
            select(ClanApplication).where(ClanApplication.clan_id == clan_id, ClanApplication.user_id == user_id)
        )
        app = result.scalar_one_or_none()
        if app:
            return app
        app = ClanApplication(clan_id=clan_id, user_id=user_id)
        session.add(app)
        await session.commit()
        return app

async def get_pending_applications_for_founder(founder_id: str):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(ClanApplication, Clan, User)
            .join(Clan, ClanApplication.clan_id == Clan.id)
            .join(User, ClanApplication.user_id == User.id)
            .where(Clan.owner_id == founder_id, ClanApplication.status == "pending")
        )
        return result.all()

async def approve_application(clan_id: int, user_id: str):
    async with AsyncSessionLocal() as session:
        # Mark app as approved and add to ClanMember
        app_result = await session.execute(
            select(ClanApplication).where(
                ClanApplication.clan_id == clan_id,
                ClanApplication.user_id == user_id,
                ClanApplication.status == "pending"
            )
        )
        app = app_result.scalar_one_or_none()
        if not app:
            return False
        app.status = "approved"
        member = ClanMember(clan_id=clan_id, user_id=user_id)
        session.add(member)
        await session.commit()
        return True

async def reject_application(clan_id: int, user_id: str):
    async with AsyncSessionLocal() as session:
        app_result = await session.execute(
            select(ClanApplication).where(
                ClanApplication.clan_id == clan_id,
                ClanApplication.user_id == user_id,
                ClanApplication.status == "pending"
            )
        )
        app = app_result.scalar_one_or_none()
        if not app:
            return False
        app.status = "rejected"
        await session.commit()
        return True

async def get_clans_leaderboard():
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Clan, func.count(ClanMember.id).label('member_count'))
            .outerjoin(Clan.members)
            .group_by(Clan.id)
            .order_by(func.count(ClanMember.id).desc())
        )
        return result.all()

# ADMIN: Get ALL study groups (approved or not)
async def get_all_groups_for_admin():
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Clan)
            .options(joinedload(Clan.owner), joinedload(Clan.members).joinedload(ClanMember.user))
            # no filter on is_approved
        )
        return result.scalars().all()
    
async def update_clan_info(clan_id: int, name: str, description: str, requirements: str, link: str):
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Clan).where(Clan.id == clan_id))
        clan = result.scalar_one_or_none()
        if not clan:
            return False
        clan.name = name
        clan.description = description
        clan.requirements = requirements
        clan.link = link
        await session.commit()
        return True


async def delete_clan(clan_id: int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Clan).where(Clan.id == clan_id))
        clan = result.scalar_one_or_none()
        if not clan:
            return False
        await session.delete(clan)
        await session.commit()
        return True

async def create_clan_creation_request(user_id, username, group_name, description, requirements, link):
    async with AsyncSessionLocal() as session:
        req = ClanCreationRequest(
            user_id=user_id,
            username=username,
            group_name=group_name,
            description=description,
            requirements=requirements,
            link=link,
            status="pending"
        )
        session.add(req)
        await session.commit()
        return req

async def get_pending_clan_creation_requests():
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(ClanCreationRequest).where(ClanCreationRequest.status == "pending")
        )
        return result.scalars().all()


async def reject_clan_creation_request(request_id):
    async with AsyncSessionLocal() as session:
        req = await session.get(ClanCreationRequest, request_id)
        if not req or req.status != "pending":
            return False
        req.status = "rejected"
        await session.commit()
        return True
# Patch for db_helpers.py: auto-add founder as member on clan approval
async def approve_clan_creation_request(request_id):
    async with AsyncSessionLocal() as session:
        req = await session.get(ClanCreationRequest, request_id)
        if not req or req.status != "pending":
            return None

        # PREVENT DUPLICATE GROUP NAME
        existing = await session.execute(
            select(Clan).where(Clan.name == req.group_name)
        )
        if existing.scalar():
            return None  # Duplicate group name!

        # Create the new Clan
        clan = Clan(
            name=req.group_name,
            owner_id=req.user_id,
            description=req.description,
            requirements=req.requirements,
            link=req.link,
            is_approved=True
        )
        session.add(clan)
        await session.flush()  # Ensure clan.id is generated

        # Add founder as first member
        member = ClanMember(clan_id=clan.id, user_id=req.user_id)
        session.add(member)

        # Mark creation request as approved
        req.status = "approved"
        await session.commit()
        await session.refresh(clan)
        return clan

# --- set group cover image (store Telegram file_id) ---
async def set_clan_image(clan_id: int, image_fid: str) -> bool:
    from db import AsyncSessionLocal
    from db_models import Clan
    async with AsyncSessionLocal() as session:
        clan = await session.get(Clan, clan_id)
        if not clan:
            return False
        clan.image_url = image_fid  # store Telegram file_id
        await session.commit()
        return True

    
async def show_group_card_paginated(msg, clans, page):
    total = len(clans)
    clan = clans[page]
    uid = str(msg.from_user.id)
    user_clans = await get_user_clans(uid)
    is_member = any(m.id == clan.id for m in user_clans)
    founder = clan.owner
    desc = (
        f"╓ 🛡️ Study Group - <b>{clan.name}</b>\n"
        f"╙ 🔺 Members: {len(clan.members)}\n"
        f"➖➖➖➖➖➖\n"
        f"🗣️ Description:\n{clan.description or 'No description'}\n"
        f"➖➖➖➖➖➖\n"
        f"📌Requirements:\n{clan.requirements or 'None'}\n"
        f"➖➖➖➖➖➖\n"
        f"👤 Founder: @{founder.username or founder.name}\n"
    )
    if is_member:
        desc += f"🔗 Group Link: {clan.link or 'No link'}\n"
    else:
        desc += f"🔗 Group Link: <i>Join to view</i>\n"
    desc += f"\n({page+1}/{total})"

    buttons = []
    if page > 0:
        buttons.append(InlineKeyboardButton(text="⬅️ Prev", callback_data=f"group_page:{page-1}"))
    if page < total-1:
        buttons.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"group_page:{page+1}"))
    if not is_member:
        buttons.append(InlineKeyboardButton(text="Apply", callback_data=f"apply_clan:{clan.id}"))
    kb = InlineKeyboardMarkup(inline_keyboard=[buttons] if buttons else [])
    await msg.answer(desc, parse_mode="HTML", reply_markup=kb)


#ADMIN PANEL
from sqlalchemy import select, func, desc, update
from db_models import User, Task, Clan, ClanMember, ClanApplication, ClanCreationRequest
from db import AsyncSessionLocal

async def admin_get_counts():
    async with AsyncSessionLocal() as session:
        users = (await session.execute(select(func.count(User.id)))).scalar_one()
        tasks_total = (await session.execute(select(func.count(Task.id)))).scalar_one()
        tasks_done  = (await session.execute(select(func.count()).where(Task.status == "done"))).scalar_one()
        tasks_open  = (await session.execute(select(func.count()).where(Task.status != "done"))).scalar_one()
        clans       = (await session.execute(select(func.count(Clan.id)))).scalar_one()
        clan_apps_pend = (await session.execute(
            select(func.count(ClanApplication.id)).where(ClanApplication.status == "pending")
        )).scalar_one()
        clan_creations_pend = (await session.execute(
            select(func.count(ClanCreationRequest.id)).where(ClanCreationRequest.status == "pending")
        )).scalar_one()
        return {
            "users": users,
            "tasks_total": tasks_total,
            "tasks_done": tasks_done,
            "tasks_open": tasks_open,
            "clans": clans,
            "join_pending": clan_apps_pend,
            "create_pending": clan_creations_pend,
        }

async def get_all_user_ids():
    async with AsyncSessionLocal() as session:
        rows = (await session.execute(select(User.id))).scalars().all()
        return [str(x) for x in rows]

async def get_user_by_username(username: str):
    if username.startswith("@"):
        username = username[1:]
    async with AsyncSessionLocal() as session:
        return (await session.execute(
            select(User).where(User.username == username)
        )).scalars().first()

async def admin_get_groups_page(page: int, per_page: int = 6):
    async with AsyncSessionLocal() as session:
        total = (await session.execute(select(func.count(Clan.id)))).scalar_one()
        rows = (await session.execute(
            select(Clan).order_by(desc(Clan.created_at)).offset(page*per_page).limit(per_page)
        )).scalars().all()
        # pre-load counts
        counts = {}
        if rows:
            clan_ids = [c.id for c in rows]
            res = await session.execute(
                select(ClanMember.clan_id, func.count(ClanMember.id))
                .where(ClanMember.clan_id.in_(clan_ids))
                .group_by(ClanMember.clan_id)
            )
            counts = {cid: cnt for cid, cnt in res.all()}
        return total, rows, counts

async def admin_toggle_group_approved(clan_id: int, flag: bool):
    async with AsyncSessionLocal() as session:
        clan = await session.get(Clan, clan_id)
        if not clan:
            return False
        clan.is_approved = bool(flag)
        await session.commit()
        return True    
    

async def export_users() -> list[dict]:
    from db import AsyncSessionLocal
    from db_models import User
    async with AsyncSessionLocal() as session:
        res = await session.execute(select(User))
        users = res.scalars().all()
        out = []
        for u in users:
            out.append({
                "id": u.id,
                "name": u.name,
                "username": u.username,
                "xp": u.xp,
                "streak": u.streak,
                "completed": u.completed,
                "extra_title": u.extra_title,
                "last_active": u.last_active or "",
            })
        return out

async def export_tasks() -> list[dict]:
    from db import AsyncSessionLocal
    from db_models import Task
    async with AsyncSessionLocal() as session:
        res = await session.execute(select(Task))
        tasks = res.scalars().all()
        out = []
        for t in tasks:
            out.append({
                "id": t.id,
                "user_id": t.user_id,
                "text": t.text,
                "deadline": t.deadline or "",
                "reminders_sent": t.reminders_sent,
                "status": t.status,
                "priority": t.priority,
                "tags": t.tags or "",
                "repeat": t.repeat or "",
                "created_at": t.created_at.isoformat() if t.created_at else "",
            })
        return out

async def export_clans() -> list[dict]:
    from db import AsyncSessionLocal
    from db_models import Clan
    async with AsyncSessionLocal() as session:
        res = await session.execute(select(Clan))
        clans = res.scalars().all()
        out = []
        for c in clans:
            out.append({
                "id": c.id,
                "name": c.name,
                "owner_id": c.owner_id,
                "is_approved": c.is_approved,
                "link": c.link or "",
                "description": c.description or "",
                "requirements": c.requirements or "",
                "image_url": c.image_url or "",
                "created_at": c.created_at.isoformat() if c.created_at else "",
            })
        return out

async def export_members() -> list[dict]:
    from db import AsyncSessionLocal
    from db_models import ClanMember
    async with AsyncSessionLocal() as session:
        res = await session.execute(select(ClanMember))
        rows = res.scalars().all()
        out = []
        for m in rows:
            out.append({
                "clan_id": m.clan_id,
                "user_id": m.user_id,
                "joined_at": m.joined_at.isoformat() if m.joined_at else "",
            })
        return out

async def export_apps() -> list[dict]:
    from db import AsyncSessionLocal
    from db_models import ClanApplication
    async with AsyncSessionLocal() as session:
        res = await session.execute(select(ClanApplication))
        rows = res.scalars().all()
        out = []
        for a in rows:
            out.append({
                "id": a.id,
                "clan_id": a.clan_id,
                "user_id": a.user_id,
                "status": a.status,
                "applied_at": a.applied_at.isoformat() if a.applied_at else "",
                "note": a.note or "",
            })
        return out    
    
async def set_user_username(user_id: str, username: str) -> bool:
    from db import AsyncSessionLocal
    from db_models import User
    async with AsyncSessionLocal() as session:
        u = await session.get(User, user_id)
        if not u:
            return False
        u.username = username or ""
        await session.commit()
        return True    
    

# --- XP award with daily cap (respects xp_today/xp_date) ---
async def award_xp_with_cap(
    user_id: str,
    requested_xp: int,
    completed_delta: int = 0,
    cap: int = 30,
    tz_name: str = "Asia/Tashkent",
):
    """
    Returns (applied_xp, user). Applies a per-day XP cap using User.xp_today/xp_date.
    Does NOT touch streak (leave your existing streak logic elsewhere).
    """
    from datetime import datetime
    import pytz

    async with AsyncSessionLocal() as session:
        user = await session.get(User, user_id)
        if not user:
            return 0, None

        tz = pytz.timezone(tz_name)
        today = datetime.now(tz).strftime("%Y-%m-%d")

        # reset daily bucket if new day
        if (user.xp_date or "") != today:
            user.xp_date = today
            user.xp_today = 0

        already = int(user.xp_today or 0)
        allowed = max(0, cap - already)
        req = max(0, int(requested_xp))
        applied = min(req, allowed)

        user.xp = max(0, int(user.xp or 0) + applied)
        user.xp_today = already + applied
        user.completed = max(0, int(user.completed or 0) + int(completed_delta or 0))

        await session.commit()
        return applied, user


# List members with user rows (ordered by join time)
async def list_clan_members(clan_id: int):
    async with AsyncSessionLocal() as session:
        res = await session.execute(
            select(ClanMember, User)
            .join(User, ClanMember.user_id == User.id)
            .where(ClanMember.clan_id == clan_id)
            .order_by(ClanMember.joined_at.asc())
        )
        return res.all()  # [(ClanMember, User), ...]

# Remove a member from a clan (guard founder)

    
async def remove_member_from_clan(clan_id: int, user_id: str) -> str:
    """
    Returns: "removed" | "founder" | "not_member" | "not_found"
    """
    async with AsyncSessionLocal() as session:
        clan = await session.get(Clan, clan_id)
        if not clan:
            return "not_found"
        if clan.owner_id == user_id:
            return "founder"

        cm = (await session.execute(
            select(ClanMember).where(
                ClanMember.clan_id == clan_id,
                ClanMember.user_id == user_id
            )
        )).scalar_one_or_none()
        if not cm:
            return "not_member"

        await session.delete(cm)
        await session.commit()
        return "removed"    
    

async def get_clans_xp_leaderboard(limit: int = 10, mode: str = "total"):
    from db import AsyncSessionLocal
    from db_models import Clan, ClanMember, User

    metric_total = func.coalesce(func.sum(User.xp), 0).label("total_xp")
    metric_avg   = func.coalesce(func.avg(User.xp), 0).label("avg_xp")
    member_count = func.count(ClanMember.id).label("member_count")

    async with AsyncSessionLocal() as session:
        stmt = (
            select(Clan, member_count, metric_total, metric_avg)
            .join(ClanMember, ClanMember.clan_id == Clan.id, isouter=True)
            .join(User, User.id == ClanMember.user_id, isouter=True)
            .where(Clan.is_approved == True)
            .group_by(Clan.id)
        )
        order_metric = desc("total_xp") if mode == "total" else desc("avg_xp")
        stmt = stmt.order_by(order_metric).limit(limit)
        res = await session.execute(stmt)
        # returns list of tuples: (Clan, member_count, total_xp, avg_xp)
        return res.all()    
    

async def get_clans_xp_leaderboard_page(page: int = 0, per_page: int = 10, mode: str = "total"):
    from db import AsyncSessionLocal
    from db_models import Clan, ClanMember, User

    metric_total = func.coalesce(func.sum(User.xp), 0).label("total_xp")
    metric_avg   = func.coalesce(func.avg(User.xp), 0).label("avg_xp")
    member_count = func.count(ClanMember.id).label("member_count")

    async with AsyncSessionLocal() as session:
        # count approved clans
        total = (await session.execute(select(func.count()).select_from(Clan).where(Clan.is_approved == True))).scalar_one()

        stmt = (
            select(Clan, member_count, metric_total, metric_avg)
            .join(ClanMember, ClanMember.clan_id == Clan.id, isouter=True)
            .join(User, User.id == ClanMember.user_id, isouter=True)
            .where(Clan.is_approved == True)
            .group_by(Clan.id)
        )
        order_metric = desc("total_xp") if mode == "total" else desc("avg_xp")
        stmt = stmt.order_by(order_metric, desc(member_count), Clan.created_at.desc()).offset(page*per_page).limit(per_page)
        res = await session.execute(stmt)
        rows = res.all()   # [(Clan, member_count, total_xp, avg_xp)]
        return total, rows    
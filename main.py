import asyncio
import pytz
import os
import re
from datetime import datetime
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery,
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from dotenv import load_dotenv
from datetime import datetime, timedelta
import io, csv, zipfile
from aiogram.types import BufferedInputFile
from db_helpers import (
    get_or_create_user, get_user, get_user_by_id,
    add_task, add_tasks_bulk, get_tasks, get_task_by_id,
    mark_task_done, delete_task, get_top_users, get_all_users_with_tasks,
    set_user_title, update_xp_streak_completed,
    update_task_text, update_task_deadline,
    update_task_priority, update_task_tags,
    get_all_clans, get_user_clans, get_clan_by_id,
    create_clan_application, get_pending_applications_for_founder,
    approve_application, reject_application, get_clans_leaderboard,
    delete_clan, reject_clan_creation_request, approve_clan_creation_request, get_pending_clan_creation_requests, create_clan_creation_request,
    update_clan_info, show_group_card_paginated, admin_get_counts, get_all_user_ids, get_user_by_username,
    admin_get_groups_page, admin_toggle_group_approved, update_task_reminders_sent, set_clan_image,
    export_users, export_tasks, export_clans, export_members, export_apps, set_user_username, award_xp_with_cap,
    list_clan_members, remove_member_from_clan, get_clans_xp_leaderboard, get_clans_xp_leaderboard_page, get_tasks_for_reminder_window,
    bulk_update_task_reminders_sent
)
from utils.parsing import parse_tasks_text, parse_natural_deadline, deadline_for_scope




load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "5480597971"))

XP_DAILY_CAP = 30
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp  = Dispatcher()
PER_PAGE_CLB = 10


# — State trackers —
user_adding_task    = set()
user_edit_text      = {}
user_edit_deadline  = {}
user_edit_priority  = {}
user_edit_tags      = {}
users_requesting_group = set()
users_editing_group = {}
users_editing_group_photo = {}  # uid -> clan_id
user_tasks_view = {}        # uid -> {"filter": str, "snapshot": [task_ids]}
manage_wait = {}  

manage_select = {}          # uid -> {"action": str, "selected": set[int]}

manage_batch = {}            # uid -> {"action":"prio"|"tag", "ids":[task_ids]}
manage_custom_tag_wait = {} 

# --- Group Pagination State
user_group_page = {}  # user_id: page_number

user_add_scope = {}  # uid -> 'free'|'today'|'week'|'month'

manage_deadline_wait = {}  # uid -> [task_ids] waiting for a single deadline value


# --- Admin states ---
admin_broadcast_wait = set()
admin_addxp_wait     = set()
admin_settitle_wait  = set()

# — Constants & UX Setup —
TASHKENT_TZ = pytz.timezone("Asia/Tashkent")
BUTTONS = {
    "ADD":     "➕ Add Task",
    "LIST":    "📋 List Tasks",
    "PROFILE": "👤 Profile",
    "LB":      "🏅 Leaderboard",
    "REPORT":  "📊 Daily Report",
    "GROUPS":  "📚 Study Groups",
    "HELP":    "❓ Help",            # <— add this
}


VALID_BUTTON_TEXTS = set(BUTTONS.values()) | {
    "🔙 Back", "🌎 Browse Groups", "🌍 Browse Groups",
    "👥 My Study Group", "➕ Request to Create Group", "🏆 Groups Leaderboard",
    # admin panel entries
    "📣 Broadcast", "➕ Add XP", "🏷️ Set Title", "📈 Stats",
    "🗂 Groups (Admin)", "⬅️ Exit Admin"
}


PRIO_ICONS = {
    "low":     "👌",
    "medium":  "⚡️",
    "high":    "🔥"
}

TAG_ICONS = {
    "study":   "🎓",
    "work":    "💼",
    # add more tag→emoji pairs as you like
}

RANKS = [
    (200,          "🎯 Rookie Planner"),
    (500,          "⚡ Achiever"),
    (1200,         "🔥 Crusher"),
    (2500,         "🏆 Master"),
    (float('inf'), "🗽 Legend")
]
def get_rank(xp):
    for thr,name in RANKS:
        if xp < thr:
            return name
    return RANKS[-1][1]

MOTIVATIONS = [
    "🔥 Keep pushing, you're doing amazing!",
    "💪 Small steps every day lead to big success.",
    "🚀 You’re on your way to greatness!",
    "🌟 Consistency is key, stay focused!",
    "🏆 Every completed task is a victory!"
]
TIPS = [
    "💡 Tip: Stay consistent!",
    "💡 Tip: Focus on one task at a time!",
    "💡 Tip: Review your goals daily!"
]

# — Keyboards —
def add_scope_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🌞 Today",      callback_data="addscope:today"),
            InlineKeyboardButton(text="🗓️ This Week", callback_data="addscope:week"),
        ],
        [
            InlineKeyboardButton(text="📆 This Month", callback_data="addscope:month"),
            InlineKeyboardButton(text="📝 No deadline", callback_data="addscope:free"),
        ]
    ])


def main_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [ KeyboardButton(text=BUTTONS["ADD"]),     KeyboardButton(text=BUTTONS["LIST"]) ],
            [ KeyboardButton(text=BUTTONS["PROFILE"]), KeyboardButton(text=BUTTONS["LB"])   ],
            [ KeyboardButton(text=BUTTONS["REPORT"]),  KeyboardButton(text=BUTTONS["GROUPS"]) ],
            [ KeyboardButton(text=BUTTONS["HELP"]) ]   # <— new row
        ],
        resize_keyboard=True
    )


def task_actions_kb(tasks):
    kb = []
    for i,t in enumerate(tasks, 1):
        kb.append([
            InlineKeyboardButton(text=f"✅ {i}", callback_data=f"done:{t.id}"),
            InlineKeyboardButton(text="⚙️ Menu", callback_data=f"menu:{t.id}")
        ])
    kb.append([ InlineKeyboardButton(text="🗑️ Clear All", callback_data="clear_all") ])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def menu_kb(tid: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [ InlineKeyboardButton(text="✏️ Edit Text",     callback_data=f"edit_text:{tid}") ],
        [ InlineKeyboardButton(text="⏰ Edit Deadline", callback_data=f"edit_deadline:{tid}") ],
        [ InlineKeyboardButton(text="⚡ Edit Priority", callback_data=f"edit_prio:{tid}") ],
        [ InlineKeyboardButton(text="🏷️ Edit Tags",     callback_data=f"edit_tags:{tid}") ]
    ])

def priority_kb(tid: int):
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Low ⚪️",    callback_data=f"set_prio:{tid}:low"),
        InlineKeyboardButton(text="Medium 🟠", callback_data=f"set_prio:{tid}:medium"),
        InlineKeyboardButton(text="High 🔴",   callback_data=f"set_prio:{tid}:high"),
    ]])

def tags_kb(tid: int):
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="#work",     callback_data=f"set_tag:{tid}:work"),
        InlineKeyboardButton(text="#study",    callback_data=f"set_tag:{tid}:study"),
        InlineKeyboardButton(text="#personal", callback_data=f"set_tag:{tid}:personal"),
    ]])
def _manage_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Mark done", callback_data="tasks_manage_action:done"),
            InlineKeyboardButton(text="🗑 Delete",    callback_data="tasks_manage_action:delete"),
        ],
        [
            InlineKeyboardButton(text="⚡ Set priority", callback_data="tasks_manage_action:prio"),
            InlineKeyboardButton(text="🏷 Set tag",      callback_data="tasks_manage_action:tag"),
        ],
        [ InlineKeyboardButton(text="⏰ Set deadline", callback_data="tasks_manage_action:deadline") ],
        [ InlineKeyboardButton(text="❌ Cancel",       callback_data="tasks_manage_cancel") ],
    ])


def admin_menu_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📣 Broadcast"), KeyboardButton(text="➕ Add XP")],
            [KeyboardButton(text="🏷️ Set Title"), KeyboardButton(text="📈 Stats")],
            [KeyboardButton(text="🗂 Groups (Admin)"), KeyboardButton(text="📦 Backup")],
            [KeyboardButton(text="⬅️ Exit Admin")]
        ],
        resize_keyboard=True
    )

def admin_groups_kb(page: int, total: int, per_page: int):
    pages = max(1, (total + per_page - 1)//per_page)
    prev_p = (page - 1) % pages
    next_p = (page + 1) % pages
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"⏮ Page {prev_p+1}/{pages}", callback_data=f"admin_groups:{prev_p}"),
         InlineKeyboardButton(text=f"⏭ {next_p+1}/{pages}",      callback_data=f"admin_groups:{next_p}")]
    ])


# — Study Groups (Clans) Menus —

def groups_main_kb(is_in_group: bool):
    # Show "My Study Group" only if user is in a group, otherwise only show Browse/Create
    rows = []
    if is_in_group:
        rows.append([KeyboardButton(text="👥 My Study Group")])
    rows.append([KeyboardButton(text="🌎 Browse Groups")])
    rows.append([KeyboardButton(text="🏆 Groups Leaderboard")])
    if not is_in_group:
        rows.append([KeyboardButton(text="➕ Request to Create Group")])
    rows.append([KeyboardButton(text="🔙 Back")])
    return ReplyKeyboardMarkup(
        keyboard=rows,
        resize_keyboard=True
    )

#filters
FILTERS = [
    ("today", "🗂️ Today"),
    ("week",  "🗓 This Week"),
    ("month", "📆 This Month"),
    ("all",   "📋 All"),
]


def _parse_deadline(dt_str: str):
    if not dt_str:
        return None
    try:
        return TASHKENT_TZ.localize(datetime.strptime(dt_str, "%Y-%m-%d %H:%M"))
    except Exception:
        return None

def _filter_tasks(all_tasks, key: str):
    # all_tasks are pending for the user (from get_tasks)
    now = datetime.now(TASHKENT_TZ)
    today = now.date()

    # week bounds: Monday..Sunday
    monday = today - timedelta(days=today.weekday())
    sunday = monday + timedelta(days=6)

    # month bounds: first..last
    first_day = today.replace(day=1)
    if first_day.month == 12:
        next_first = first_day.replace(year=first_day.year+1, month=1)
    else:
        next_first = first_day.replace(month=first_day.month+1)
    last_day = next_first - timedelta(days=1)

    out = []
    for t in all_tasks:
        if getattr(t, "status", "pending") != "pending":
            continue
        d = _parse_deadline(t.deadline)

        if key == "today":
            if d and d.date() == today:
                out.append(t)
        elif key == "week":
            if d and monday <= d.date() <= sunday:
                out.append(t)
        elif key == "month":
            if d and first_day <= d.date() <= last_day:
                out.append(t)
        elif key == "repeating":
            if (t.repeat or "") in ("daily","weekly","monthly"):
                out.append(t)
        else:  # "all"
            out.append(t)
    return out

def _sort_tasks(tasks):
    # sort by deadline (None at bottom), then created_at
    return sorted(tasks, key=lambda t: (
        0 if _parse_deadline(t.deadline) else 1,
        _parse_deadline(t.deadline) or datetime.max.replace(tzinfo=TASHKENT_TZ),
        t.created_at or datetime.utcnow()
    ))

def _repeat_pill(t):
    rep = (t.repeat or "")
    return f"  🔁 {rep.capitalize()}" if rep in ("daily","weekly","monthly") else ""

def _render_tasks_lines(tasks):
    lines = []
    today = datetime.now(TASHKENT_TZ).date()
    for i, t in enumerate(tasks, 1):
        prio_em  = PRIO_ICONS.get(t.priority or "medium", "⚡️")
        prio     = (t.priority or "medium").capitalize()
        tag_key  = (t.tags or "")
        tag_em   = TAG_ICONS.get(tag_key, "🏷️")
        tag_txt  = tag_key or "None"
        dl_str   = t.deadline or "None"
        try:
            dl_date = datetime.strptime(dl_str, "%Y-%m-%d %H:%M").date() if dl_str != "None" else None
            dl_fmt  = f"<b>{dl_str}</b>" if dl_date and dl_date <= today else dl_str
        except Exception:
            dl_fmt  = dl_str
        lines.append(f"{i}. {_clip(t.text)} — {prio_em} {prio}, {tag_em} {tag_txt} — ⏰ {dl_fmt}{_repeat_pill(t)}")
    return lines


def _filters_kb(current: str):
    row_tabs = [InlineKeyboardButton(
        text=("• " + name + " •") if key == current else name,
        callback_data=f"tasks_filter:{key}"
    ) for key, name in FILTERS]

    kb_rows = [row_tabs[:2], row_tabs[2:]]  # 2 + 2
    kb_rows.append([InlineKeyboardButton(text="🧰 Manage tasks", callback_data="tasks_manage")])
    kb_rows.append([InlineKeyboardButton(text="🗑 Clear All (scope)", callback_data="tasks_clear_confirm")])
    kb_rows.append([InlineKeyboardButton(text="🔙 Back", callback_data="tasks_back")])
    return InlineKeyboardMarkup(inline_keyboard=kb_rows)


async def _show_tasks_list(msg_or_call, uid: str, filt: str):
    all_tasks = await get_tasks(uid)
    filt = filt if filt in {k for k,_ in FILTERS} else "all"
    tasks = _sort_tasks(_filter_tasks(all_tasks, filt))
    snapshot = [t.id for t in tasks]
    user_tasks_view[uid] = {"filter": filt, "snapshot": snapshot}

    name = dict(FILTERS)[filt]
    if not tasks:   
        text = f"📝 <b>Your Tasks</b> — {name}\n\nNothing here yet. Try another filter."
    else:
        blocks = _render_task_blocks(tasks)
        text = f"📝 <b>Your Tasks</b> — {name} • {len(tasks)}\n" + "\n\n".join(blocks)

    kb = _filters_kb(filt)
    if isinstance(msg_or_call, Message):
        await msg_or_call.answer(text, reply_markup=kb)
    else:
        try:
            await msg_or_call.message.edit_text(text, reply_markup=kb)
        except Exception:
            await msg_or_call.message.answer(text, reply_markup=kb)






def _repeat_text(t):
    rep = (t.repeat or "").lower()
    return f"🔁 {rep.capitalize()}" if rep in ("daily", "weekly", "monthly") else ""

def _pretty_deadline_str(dl_str: str) -> str:
    if not dl_str:
        return "No deadline"
    try:
        dt = TASHKENT_TZ.localize(datetime.strptime(dl_str, "%Y-%m-%d %H:%M"))
    except Exception:
        return "No deadline"

    now   = datetime.now(TASHKENT_TZ)
    today = now.date()
    d     = dt.date()

    if d == today:
        label = "Today"
    elif d == today + timedelta(days=1):
        label = "Tomorrow"
    elif 0 <= (d - today).days < 7:
        label = dt.strftime("%a")                 # Mon
    else:
        label = dt.strftime("%b %d")              # Aug 17

    time  = dt.strftime("%H:%M")
    text  = f"{label} {time}"

    # bold if past due or earlier today
    if d < today or (d == today and dt <= now):
        text = f"<b>{text}</b>"
    return text

def _render_task_blocks(tasks):
    """Two-line compact cards with chips."""
    blocks = []
    for i, t in enumerate(tasks, 1):
        title = _clip(t.text, 70)
        prio  = (t.priority or "medium").capitalize()
        tag   = (t.tags or "None")
        due   = _pretty_deadline_str(t.deadline or "")

        line1 = f"{i}) {title}"
        line2_parts = [f"⏰ {due}", f"⚡ {prio}", f"🏷 {tag}"]
        rpt = _repeat_text(t)
        if rpt:
            line2_parts.append(rpt)
        line2 = "   ".join(line2_parts)

        blocks.append(line1 + "\n" + "   " + line2)
    return blocks





# — Handlers —  
@dp.message(Command("start"))
async def cmd_start(msg: Message):
    # ensure a row exists + keep username fresh
    await get_or_create_user(
        str(msg.from_user.id),
        name=msg.from_user.full_name or "Unknown",
        username=(msg.from_user.username or "")
    )

    await msg.answer(
        "🚀 <b>Welcome to Smart Daily Planner V2!</b>\n"
        "<i>Your productivity, now gamified — right inside Telegram.</i>\n\n"
        "Here’s what you can do:\n"
        "• ✍️ <b>Add tasks</b>\n"
        "• ⏰ <b>Get reminders</b> exactly on time\n"
        "• 📋 <b>Manage & organize</b> tasks with deadlines, tags, and priorities\n"
        "• 🏆 <b>Climb the leaderboards</b> solo or with your Study Group\n"
        "• 🎯 <b>Earn XP & streak bonuses</b>\n"
        "• 👥 <b>Join Study Groups</b>\n\n"
        "💡 <i>Tip: Use</i> /help <i>anytime for quick commands & examples.</i>\n\n"
        "Let’s get started! Tap a button below ⤵️",
        reply_markup=main_kb()
    )

@dp.message(F.text == BUTTONS["ADD"])
async def add_task_prompt(msg: Message):
    uid = str(msg.from_user.id)
    user_adding_task.add(uid)
    user_add_scope[uid] = "free"  # default
    await msg.answer("✍️ Send your task(s) now (up to 10 pending). You can paste multiple lines.", reply_markup=main_kb())
    await msg.answer("🎯 When are these for? Pick a scope:", reply_markup=add_scope_kb())
    

@dp.callback_query(F.data.startswith("addscope:"))
async def addscope_cb(cb: CallbackQuery):
    uid = str(cb.from_user.id)
    scope = cb.data.split(":",1)[1]
    if scope not in ("today","week","month","free"):
        return await cb.answer("Unknown", show_alert=True)
    user_add_scope[uid] = scope
    pretty = {"today":"🌞 Today","week":"🗓️ This Week","month":"📆 This Month","free":"📝 No deadline"}[scope]

    await cb.answer(f"Scope: {pretty}")
    try:
        await cb.message.edit_reply_markup(add_scope_kb())
    except:
        pass
    await cb.message.answer(f"✅ Using scope: <b>{pretty}</b>\nNow send your task(s).")

@dp.callback_query(F.data.startswith("tasks_manage_action:"))
async def tasks_manage_choose(cb: CallbackQuery):
    uid = str(cb.from_user.id)
    action = cb.data.split(":",1)[1]  # done|delete|prio|tag|deadline
    view = user_tasks_view.get(uid)
    if not view or not view.get("snapshot"):
        return await cb.answer("Nothing to manage.", show_alert=True)
    if action not in {"done","delete","prio","tag","deadline"}:
        return await cb.answer("Unknown action.", show_alert=True)

    manage_select[uid] = {"action": action, "selected": set()}
    await _render_select_ui(cb, uid)
    await cb.answer()


def _action_name(a: str) -> str:
    return {
        "done":"Mark done",
        "delete":"Delete",
        "prio":"Set priority",
        "tag":"Set tag",
        "deadline":"Set deadline",
    }.get(a, a)

async def _get_snapshot_tasks(uid: str):
    # map snapshot ids -> task objects in current order
    snapshot = (user_tasks_view.get(uid) or {}).get("snapshot", [])
    tasks = await get_tasks(uid)
    by_id = {t.id: t for t in tasks}
    ordered = [by_id[i] for i in snapshot if i in by_id]
    return ordered

def _select_kb(uid: str, tasks, selected: set[int]):
    rows = []
    # one button per task (limit is 10 pending anyway)
    for idx, t in enumerate(tasks, 1):
        mark = "✅" if idx in selected else "⬜"
        label = f"{mark} {idx}. {_clip(t.text, 40)}"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"sel_toggle:{idx}")])
    # controls
    rows.append([
        InlineKeyboardButton(text="Select all", callback_data="sel_all"),
        InlineKeyboardButton(text="Clear",      callback_data="sel_clear"),
    ])
    rows.append([
        InlineKeyboardButton(text="▶️ Proceed",  callback_data="sel_go"),
        InlineKeyboardButton(text="❌ Cancel",   callback_data="tasks_manage_cancel"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)

async def _render_select_ui(cb_or_msg, uid: str):
    state = manage_select.get(uid)
    action = state["action"]
    tasks = await _get_snapshot_tasks(uid)
    title = f"🧰 <b>Manage tasks</b> — {_action_name(action)}\nTap to select items, then <b>Proceed</b>."
    kb = _select_kb(uid, tasks, state["selected"])
    if isinstance(cb_or_msg, Message):
        await cb_or_msg.answer(title, parse_mode=ParseMode.HTML, reply_markup=kb)
    else:
        try:
            await cb_or_msg.message.edit_text(title, parse_mode=ParseMode.HTML, reply_markup=kb)
        except:
            await cb_or_msg.message.answer(title, parse_mode=ParseMode.HTML, reply_markup=kb)

@dp.callback_query(F.data.startswith("sel_toggle:"))
async def sel_toggle(cb: CallbackQuery):
    uid = str(cb.from_user.id)
    st = manage_select.get(uid)
    if not st: return await cb.answer()
    idx = int(cb.data.split(":")[1])
    if idx in st["selected"]:
        st["selected"].remove(idx)
    else:
        st["selected"].add(idx)
    await _render_select_ui(cb, uid)
    await cb.answer()

@dp.callback_query(F.data == "sel_all")
async def sel_all(cb: CallbackQuery):
    uid = str(cb.from_user.id)
    st = manage_select.get(uid)
    if not st: return await cb.answer()
    tasks = await _get_snapshot_tasks(uid)
    st["selected"] = set(range(1, len(tasks)+1))
    await _render_select_ui(cb, uid); await cb.answer()

@dp.callback_query(F.data == "sel_clear")
async def sel_clear(cb: CallbackQuery):
    uid = str(cb.from_user.id)
    st = manage_select.get(uid)
    if not st: return await cb.answer()
    st["selected"].clear()
    await _render_select_ui(cb, uid); await cb.answer()



@dp.callback_query(F.data == "sel_go")
async def sel_go(cb: CallbackQuery):
    uid = str(cb.from_user.id)
    st = manage_select.pop(uid, None)
    if not st: return await cb.answer()
    action = st["action"]
    view = user_tasks_view.get(uid) or {}
    snapshot = view.get("snapshot", [])
    # build ids from selected indexes
    if not st["selected"]:
        return await cb.answer("Select at least one.", show_alert=True)
    indices = sorted(st["selected"])
    ids = [snapshot[i-1] for i in indices if 1 <= i <= len(snapshot)]

    # branch like your old flow
    if action == "prio":
        manage_batch[uid] = {"action":"prio", "ids": ids}
        await cb.message.answer("Choose priority for the selected task(s):", reply_markup=_batch_priority_kb())
        return await cb.answer()

    if action == "tag":
        manage_batch[uid] = {"action":"tag", "ids": ids}
        await cb.message.answer("Choose a tag (or Custom) for the selected task(s):", reply_markup=_batch_tags_kb())
        return await cb.answer()

    if action == "deadline":
        manage_deadline_wait[uid] = ids
        copy_block, reference = build_deadline_examples_text()
        await cb.message.answer(
            "⏰ Send a deadline.\nTap to copy one of these, or type your own:\n"
            + copy_block + "\n" + reference,
            parse_mode=ParseMode.HTML
        )
        return await cb.answer()

    # delete / done
    if action == "delete":
        deleted = 0
        for tid in ids:
            try:
                await delete_task(tid); deleted += 1
            except: pass
        await cb.message.answer(f"🗑 Deleted: {', '.join(map(str, indices))}  (total {deleted})")
        await _show_tasks_list(cb, uid, (view.get("filter") or "all"))
        return await cb.answer("Done")

    # mark done with XP cap (same as your existing code)
    requested = 0; count = 0
    for tid in ids:
        try:
            t = await get_task_by_id(tid)
            if not t: continue
            base = 2
            prio_bonus = {"low":0,"medium":2,"high":4}.get((t.priority or "low"), 0)
            tag_bonus  = 1 if "study" in (t.tags or "") else 0
            requested += (base + prio_bonus + tag_bonus)
            await mark_task_done(tid); count += 1
        except: pass

    applied, _ = await award_xp_with_cap(uid, requested_xp=requested, completed_delta=count, cap=XP_DAILY_CAP, tz_name="Asia/Tashkent")
    if count == 0:
        await cb.message.answer("Nothing changed.", reply_markup=main_kb())
    elif applied == requested:
        await cb.message.answer(f"✅ Done: {', '.join(map(str, indices))}  (+{applied} XP)")
    elif applied > 0:
        await cb.message.answer(f"✅ Done: {', '.join(map(str, indices))}  (+{applied} XP; {requested - applied} XP over daily cap {XP_DAILY_CAP})")
    else:
        await cb.message.answer(f"✅ Done: {', '.join(map(str, indices))}  (0 XP — daily cap {XP_DAILY_CAP}, resets at 00:00)")
    await _show_tasks_list(cb, uid, (view.get("filter") or "all"))
    await cb.answer("Done")



# ===== UI helpers =====
def _clip(text: str, max_len: int = 64) -> str:
    t = (text or "").strip()
    return t if len(t) <= max_len else (t[:max_len-1] + "…")

def _bar10(pct: int) -> str:
    """10-segment progress bar like ▰▰▰▰▱▱▱▱▱▱ 40%"""
    pct = max(0, min(100, int(pct)))
    filled = round(pct / 10)
    return "▰" * filled + "▱" * (10 - filled) + f" {pct}%"

def _rank_progress(xp: int):
    """returns (current_name, next_name, pct_to_next)"""
    prev_thr = 0
    curr_name = RANKS[0][1]
    for thr, name in RANKS:
        if xp < thr:
            curr_name = name
            span = thr - prev_thr if thr != float("inf") else 1
            pct = int(((xp - prev_thr) / span) * 100) if span else 100
            next_name = name  # readable “to next” label below will use the NEXT entry though
            # find the *next* rank name (if any)
            nxt = None
            for t2, n2 in RANKS:
                if t2 > thr:
                    nxt = n2
                    break
            return (curr_name, nxt, pct)
        prev_thr = thr
    return (RANKS[-1][1], None, 100)

def _safe_username(u) -> str:
    h = (getattr(u, "username", "") or "").strip()
    return f"@{h}" if h else "—"





def _batch_priority_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Low ⚪️",    callback_data="tasks_batch_prio:low"),
            InlineKeyboardButton(text="Medium 🟠", callback_data="tasks_batch_prio:medium"),
            InlineKeyboardButton(text="High 🔴",   callback_data="tasks_batch_prio:high"),
        ],
        [InlineKeyboardButton(text="❌ Cancel", callback_data="tasks_batch_cancel")]
    ])

def _batch_tags_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="#work",     callback_data="tasks_batch_tag:work"),
            InlineKeyboardButton(text="#study",    callback_data="tasks_batch_tag:study"),
            InlineKeyboardButton(text="#personal", callback_data="tasks_batch_tag:personal"),
        ],
        [InlineKeyboardButton(text="✏️ Custom",   callback_data="tasks_batch_tag:custom")],
        [InlineKeyboardButton(text="❌ Cancel",   callback_data="tasks_batch_cancel")]
    ])



@dp.message(F.text == BUTTONS["LIST"])
async def list_tasks_v2(msg: Message):
    uid = str(msg.from_user.id)
    await _show_tasks_list(msg, uid, "all")

@dp.message(Command("help"))
@dp.message(F.text == BUTTONS["HELP"])
async def help_screen(msg: Message):
    copy_block, reference = build_deadline_examples_text()
    text = (
        "❓ <b>Smart Daily Planner — Help</b>\n\n"
        "🚀 <b>Quick start</b>\n"
        "• Tap <b>➕ Add Task</b> → choose <i>Today/This Week/This Month/No deadline</i>\n"
        "• Paste multiple lines to add several tasks at once\n\n"
        "🧭 <b>View & manage</b>\n"
        "• Tabs: <i>Today</i>, <i>This Week</i>, <i>This Month</i>, <i>All</i>\n"
        "• <b>🧰 Manage tasks</b>: mark done / delete / set priority / set tag / set deadline in batch\n\n"
        "⏰ <b>Deadlines (type naturally)</b>\n"
        "Tap to copy examples or type your own:\n" + copy_block + "\n" + reference + "\n\n"
        "👥 <b>Study Groups</b>\n"
        "• Browse, view members, apply to join • Founders can edit, set photo, review applications\n\n"
        f"🏆 <b>XP & Ranks</b> • Daily cap: <b>{XP_DAILY_CAP}</b>\n"
        "📊 <b>Daily Report</b> for a quick snapshot\n\n"
        "❓ <b>Need help?</b>\n"
        "• Ping the admin: @hae_sung1\n"
    )
    await msg.answer(text, parse_mode=ParseMode.HTML, reply_markup=main_kb())


    # Optional: Quick templates right under Help
   




@dp.message(F.text == "📦 Backup")
async def admin_backup(msg: Message):
    if msg.from_user.id != ADMIN_ID:
        return
    # fetch data
    users = await export_users()
    tasks = await export_tasks()
    clans = await export_clans()
    members = await export_members()
    apps = await export_apps()

    # build zip in-memory
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        def write_csv(name, rows, headers):
            f = io.StringIO()
            w = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
            z.writestr(name, f.getvalue())

        write_csv("users.csv", users, ["id","name","username","xp","streak","completed","extra_title","last_active"])
        write_csv("tasks.csv", tasks, ["id","user_id","text","deadline","reminders_sent","status","priority","tags","repeat","created_at"])
        write_csv("clans.csv", clans, ["id","name","owner_id","is_approved","link","description","requirements","image_url","created_at"])
        write_csv("clan_members.csv", members, ["clan_id","user_id","joined_at"])
        write_csv("clan_applications.csv", apps, ["id","clan_id","user_id","status","applied_at","note"])

    buf.seek(0)
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    file = BufferedInputFile(buf.getvalue(), filename=f"smart_planner_backup_{ts}.zip")
    await msg.answer_document(file, caption="📦 Backup (CSV files inside)")

@dp.callback_query(F.data.startswith("tasks_filter:"))
async def tasks_filter_cb(cb: CallbackQuery):
    uid = str(cb.from_user.id)
    filt = cb.data.split(":",1)[1]
    await _show_tasks_list(cb, uid, filt)
    await cb.answer()

@dp.callback_query(F.data == "tasks_back")
async def tasks_back_cb(cb: CallbackQuery):
    await cb.message.answer("Returned to main menu.", reply_markup=main_kb())
    await cb.answer()





@dp.callback_query(F.data == "tasks_manage")
async def tasks_manage_open(cb: CallbackQuery):
    uid = str(cb.from_user.id)
    view = user_tasks_view.get(uid)
    if not view or not view.get("snapshot"):
        await cb.answer("Nothing to manage.", show_alert=True)
        return
    await cb.message.answer(
        "🧰 <b>Manage tasks</b>\n"
        "Choose an action, then reply with numbers from the current list (e.g. <code>1 3 5</code> or <code>2-4</code>).",
        reply_markup=_manage_kb()
    )
    await cb.answer()


@dp.callback_query(F.data == "tasks_manage_cancel")
async def tasks_manage_cancel(cb: CallbackQuery):
    manage_wait.pop(str(cb.from_user.id), None)
    await cb.answer("Canceled")



def _parse_selection_numbers(text: str, max_n: int) -> list[int]:
    # returns 1-based indices within [1..max_n]
    nums = set()
    for token in re.split(r"[,\s]+", text.strip()):
        if not token:
            continue
        if "-" in token:
            try:
                a,b = token.split("-",1)
                a=int(a); b=int(b)
                if a>b: a,b = b,a
                for x in range(a,b+1):
                    if 1 <= x <= max_n: nums.add(x)
            except: pass
        else:
            try:
                x=int(token)
                if 1 <= x <= max_n: nums.add(x)
            except: pass
    return sorted(nums)

@dp.message(lambda m: str(m.from_user.id) in manage_wait and m.text not in VALID_BUTTON_TEXTS)
async def manage_apply_numbers(msg: Message):
    uid = str(msg.from_user.id)
    state = manage_wait.pop(uid, None)
    if not state:
        return

    view = user_tasks_view.get(uid) or {}
    snapshot = state.get("snapshot") or []
    max_n = len(snapshot)

    indices = _parse_selection_numbers(msg.text, max_n)
    if not indices:
        return await msg.answer("No valid numbers. Try again from the Manage menu.", reply_markup=main_kb())

    ids = [snapshot[i-1] for i in indices if 1 <= i <= max_n]
    action = state.get("action")

    # ---- edit priority / tag → ask for a choice ----
    if action == "prio":
        manage_batch[uid] = {"action": "prio", "ids": ids}
        return await msg.answer("Choose priority for the selected task(s):", reply_markup=_batch_priority_kb())

    if action == "tag":
        manage_batch[uid] = {"action": "tag", "ids": ids}
        return await msg.answer("Choose a tag (or Custom) for the selected task(s):", reply_markup=_batch_tags_kb())

    # ---- NEW: deadline → show your existing guide and wait for one value ----
    if action == "deadline":
        manage_deadline_wait[uid] = ids
        try:
            copy_block, reference = build_deadline_examples_text()
            await msg.answer(
                "⏰ Send a deadline.\n"
                "Tap to copy one of these, or type your own:\n"
                + copy_block + "\n" + reference,
                parse_mode=ParseMode.HTML
            )
        except Exception:
            await msg.answer(
                "⏰ Send one deadline for the selected task(s) (or type <code>clear</code>).\n"
                "Examples: <code>22:00</code>, <code>tomorrow 09:00</code>, "
                "<code>next mon 14:30</code>, <code>in 2h</code>, <code>YYYY-MM-DD HH:MM</code>",
                parse_mode=ParseMode.HTML
            )
        return

    # ---- delete / done (unchanged) ----
    if action == "delete":
        deleted = 0
        for tid in ids:
            try:
                await delete_task(tid)
                deleted += 1
            except:
                pass
        await msg.answer(f"🗑 Deleted: {', '.join(map(str, indices))}  (total {deleted})")
        return await _show_tasks_list(msg, uid, (view.get("filter") or "all"))

    # "done" with XP cap
    requested = 0
    count = 0
    for tid in ids:
        try:
            t = await get_task_by_id(tid)
            if not t:
                continue
            base       = 2
            prio_bonus = {"low":0,"medium":2,"high":4}.get((t.priority or "low"), 0)
            tag_bonus  = 1 if "study" in (t.tags or "") else 0
            requested += (base + prio_bonus + tag_bonus)
            await mark_task_done(tid)
            count += 1
        except:
            pass

    applied, _ = await award_xp_with_cap(
        uid, requested_xp=requested, completed_delta=count,
        cap=XP_DAILY_CAP, tz_name="Asia/Tashkent"
    )

    if count == 0:
        return await msg.answer("Nothing changed.", reply_markup=main_kb())

    if applied == requested:
        await msg.answer(f"✅ Done: {', '.join(map(str, indices))}  (+{applied} XP)")
    elif applied > 0:
        await msg.answer(
            f"✅ Done: {', '.join(map(str, indices))}  (+{applied} XP; {requested - applied} XP over daily cap {XP_DAILY_CAP})"
        )
    else:
        await msg.answer(
            f"✅ Done: {', '.join(map(str, indices))}  (0 XP — daily cap {XP_DAILY_CAP}, resets at 00:00)"
        )

    await _show_tasks_list(msg, uid, (view.get('filter') or 'all'))




@dp.message(lambda m: str(m.from_user.id) in manage_deadline_wait and m.text)
async def tasks_batch_deadline_text(msg: Message):
    uid = str(msg.from_user.id)
    ids = manage_deadline_wait.pop(uid, [])

    raw = msg.text.strip()

    # try natural language first (tonight, tomorrow 09:00, in 2h, 21:00, etc.)
    dt = parse_natural_deadline(raw, tz_name="Asia/Tashkent")
    iso = None
    if dt:
        iso = dt.strftime("%Y-%m-%d %H:%M")
    else:
        # fallback: strict ISO "YYYY-MM-DD HH:MM" or "YYYY-MM-DD"
        from datetime import datetime as _dt
        try:
            _dt.strptime(raw, "%Y-%m-%d %H:%M")
            iso = raw
        except ValueError:
            try:
                d_only = _dt.strptime(raw, "%Y-%m-%d")
                iso = d_only.strftime("%Y-%m-%d 09:00")
            except ValueError:
                pass

    if not iso:
        return await msg.answer("⚠️ Couldn't parse that. Try e.g. <b>21:00</b>, <b>tomorrow 09:00</b>, "
                                "<b>next mon 14:30</b>, <b>in 2h</b>, or <b>YYYY-MM-DD HH:MM</b>.")

    changed = 0
    for tid in ids:
        try:
            await update_task_deadline(tid, iso)
            changed += 1
        except:
            pass

    await msg.answer(f"⏰ Deadline → <b>{iso}</b> for {changed} task(s).")
    view = user_tasks_view.get(uid) or {}
    await _show_tasks_list(msg, uid, (view.get('filter') or 'all'))





@dp.callback_query(F.data.startswith("tasks_batch_prio:"))
async def tasks_batch_prio(cb: CallbackQuery):
    uid = str(cb.from_user.id)
    data = manage_batch.get(uid)
    if not data or data.get("action") != "prio":
        return await cb.answer("No selection.", show_alert=True)

    prio = cb.data.split(":",1)[1]  # low|medium|high
    ids = data["ids"]
    changed = 0
    for tid in ids:
        try:
            await update_task_priority(int(tid), prio)
            changed += 1
        except:
            pass

    manage_batch.pop(uid, None)
    await cb.message.answer(f"⚡ Priority → <b>{prio.capitalize()}</b> for {changed} task(s).", reply_markup=main_kb())

    view = user_tasks_view.get(uid) or {}
    await _show_tasks_list(cb, uid, (view.get("filter") or "all"))
    await cb.answer("Updated")

@dp.callback_query(F.data.startswith("tasks_batch_tag:"))
async def tasks_batch_tag(cb: CallbackQuery):
    uid = str(cb.from_user.id)
    data = manage_batch.get(uid)
    if not data or data.get("action") != "tag":
        return await cb.answer("No selection.", show_alert=True)

    val = cb.data.split(":",1)[1]  # work|study|personal|custom
    ids = data["ids"]

    if val == "custom":
        manage_custom_tag_wait[uid] = ids
        await cb.message.answer("✏️ Send one tag (e.g., <code>errands</code>). I’ll set it for the selected task(s).")
        await cb.answer()
        return

    # quick tag
    changed = 0
    for tid in ids:
        try:
            await update_task_tags(int(tid), val)
            changed += 1
        except:
            pass

    manage_batch.pop(uid, None)
    await cb.message.answer(f"🏷 Tag → <b>{val}</b> for {changed} task(s).", reply_markup=main_kb())

    view = user_tasks_view.get(uid) or {}
    await _show_tasks_list(cb, uid, (view.get("filter") or "all"))
    await cb.answer("Updated")
@dp.message(lambda m: str(m.from_user.id) in manage_custom_tag_wait and m.text)
async def tasks_batch_tag_custom_text(msg: Message):
    uid = str(msg.from_user.id)
    ids = manage_custom_tag_wait.pop(uid, [])
    raw = msg.text.strip()
    tag = raw.lstrip("#").strip().split()[0].lower()[:30]  # simple, safe

    if not tag:
        return await msg.answer("Empty tag. Try again from Manage → Set tag.", reply_markup=main_kb())

    changed = 0
    for tid in ids:
        try:
            await update_task_tags(int(tid), tag)
            changed += 1
        except:
            pass

    await msg.answer(f"🏷 Tag → <b>{tag}</b> for {changed} task(s).")
    view = user_tasks_view.get(uid) or {}
    await _show_tasks_list(msg, uid, (view.get('filter') or 'all'))

@dp.callback_query(F.data == "tasks_batch_cancel")
async def tasks_batch_cancel(cb: CallbackQuery):
    uid = str(cb.from_user.id)
    manage_batch.pop(uid, None)
    manage_custom_tag_wait.pop(uid, None)
    await cb.answer("Canceled")


@dp.callback_query(F.data == "tasks_clear_confirm")
async def tasks_clear_confirm(cb: CallbackQuery):
    uid = str(cb.from_user.id)
    view = user_tasks_view.get(uid)
    if not view: 
        return await cb.answer("Nothing to clear.", show_alert=True)
    filt = view["filter"]
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Yes, clear", callback_data=f"tasks_clear_do:{filt}"),
            InlineKeyboardButton(text="No",         callback_data=f"tasks_filter:{filt}")
        ]
    ])
    await cb.message.answer(f"❗ This will remove tasks in <b>{dict(FILTERS)[filt]}</b> (pending ones). Continue?", reply_markup=kb)
    await cb.answer()

@dp.callback_query(F.data.startswith("tasks_clear_do:"))
async def tasks_clear_do(cb: CallbackQuery):
    uid  = str(cb.from_user.id)
    filt = cb.data.split(":",1)[1]
    all_tasks = await get_tasks(uid)
    targets = _sort_tasks(_filter_tasks(all_tasks, filt))
    deleted = 0
    for t in targets:
        try:
            await delete_task(t.id)
            deleted += 1
        except: pass
    await cb.message.answer(f"🗑 Cleared {deleted} task(s) from {dict(FILTERS)[filt]}.")
    await _show_tasks_list(cb, uid, filt)
    await cb.answer()


@dp.callback_query(F.data.startswith("done:"))
async def cb_done(call: CallbackQuery):
    tid  = int(call.data.split(":",1)[1])
    task = await get_task_by_id(tid)
    if not task:
        await call.answer("Task not found.", show_alert=True)
        try:
            await call.message.edit_text("⚠️ Task no longer exists.")
        except:
            pass
        return

    # XP calc
    base       = 2
    prio_bonus = {"low":0,"medium":2,"high":4}.get((task.priority or "low"), 0)
    tag_bonus  = 1 if "study" in (task.tags or "") else 0
    xp_gain    = base + prio_bonus + tag_bonus

    # mark done first
    await mark_task_done(tid)

    # award with cap
    applied, _ = await award_xp_with_cap(
        str(call.from_user.id),
        requested_xp=xp_gain,
        completed_delta=1,
        cap=XP_DAILY_CAP,
        tz_name="Asia/Tashkent",
    )

    if applied > 0:
        await call.answer(f"✅ Done! +{applied} XP")
    else:
        await call.answer(f"✅ Done! No XP — daily cap {XP_DAILY_CAP}. Resets at 00:00.")
    await call.message.edit_text("✅ Task marked as done!")



@dp.callback_query(F.data == "clear_all")
async def cb_clear_all(call: CallbackQuery):
    uid   = str(call.from_user.id)
    tasks = await get_tasks(uid)
    for t in tasks:
        await delete_task(t.id)
    await call.answer("🗑️ All cleared!")
    await call.message.answer("Back to main menu.", reply_markup=main_kb())


@dp.callback_query(F.data.startswith("menu:"))
async def cb_menu(call: CallbackQuery):
    tid = int(call.data.split(":",1)[1])
    await call.answer()
    await call.message.answer("⚙️ Task Menu:", reply_markup=menu_kb(tid))

# — Edit flows —
@dp.callback_query(F.data.startswith("edit_text:"))
async def cb_edit_text(call: CallbackQuery):
    tid = int(call.data.split(":",1)[1])
    user_edit_text[str(call.from_user.id)] = tid
    await call.answer()
    await call.message.answer("✏️ Send the new text:", reply_markup=main_kb())

@dp.callback_query(F.data.startswith("edit_deadline:"))
async def cb_edit_deadline(call: CallbackQuery):
    tid = int(call.data.split(":",1)[1])
    user_edit_deadline[str(call.from_user.id)] = tid
    await call.answer()

    copy_block, reference = build_deadline_examples_text()

    await call.message.answer(
        "⏰ Send a deadline.\n"
        "Tap to copy one of these, or type your own:\n"
        + copy_block + "\n" + reference
    )

def build_deadline_examples_text() -> tuple[str, str]:
    """Returns (copy_block_html, reference_lines_html)."""
    now = datetime.now(TASHKENT_TZ)

    # Tonight 21:00 (if past, use tomorrow)
    tonight = now.replace(hour=21, minute=0, second=0, microsecond=0)
    if tonight <= now:
        tonight += timedelta(days=1)

    # Tomorrow 09:00
    tmr = (now + timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)

    # Next Monday 09:00
    days_ahead = (0 - now.weekday()) % 7  # Mon=0
    days_ahead = 7 if days_ahead == 0 else days_ahead
    next_mon = (now + timedelta(days=days_ahead)).replace(hour=9, minute=0, second=0, microsecond=0)

    # Copy-friendly examples (each on its own line)
    copy_block = (
        "<pre>"
        "21:00\n"
        "tomorrow 09:00\n"
        "tonight\n"
        "next mon 14:30\n"
        "in 2h\n"
        "in 30m\n"
        "2025-08-31 19:00\n"
        "2025-08-31"
        "</pre>"
    )

    # Quick reference of what some examples resolve to (dynamic)
    reference = (
        f"e.g. <b>tonight</b> → <code>{tonight.strftime('%Y-%m-%d %H:%M')}</code>\n"
        f"<b>tomorrow 09:00</b> → <code>{tmr.strftime('%Y-%m-%d %H:%M')}</code>\n"
        f"<b>next mon 09:00</b> → <code>{next_mon.strftime('%Y-%m-%d %H:%M')}</code>"
    )
    return copy_block, reference



##HELPER
def uname(u) -> str:
    """'@username' if present, else name (safe)."""
    handle = getattr(u, "username", "") or ""
    return f"@{handle}" if handle else (getattr(u, "name", "") or "")


@dp.message(lambda m: str(m.from_user.id) in user_edit_deadline and m.text not in VALID_BUTTON_TEXTS)
async def handle_edit_deadline(msg: Message):
    uid = str(msg.from_user.id)
    tid = user_edit_deadline.pop(uid)
    raw = msg.text.strip()

    # Try natural-language parsing first
    dt = parse_natural_deadline(raw, tz_name="Asia/Tashkent")
    if dt:
        text = dt.strftime("%Y-%m-%d %H:%M")
        await update_task_deadline(tid, text)
        return await msg.answer(f"✅ Deadline set to <b>{text}</b>", reply_markup=main_kb())

    # Fallback to strict ISO format
    try:
        datetime.strptime(raw, "%Y-%m-%d %H:%M")
        await update_task_deadline(tid, raw)
        return await msg.answer(f"✅ Deadline set to <b>{raw}</b>", reply_markup=main_kb())
    except ValueError:
        pass
    try:
        d_only = datetime.strptime(raw, "%Y-%m-%d")
        text = d_only.strftime("%Y-%m-%d 09:00")
        await update_task_deadline(tid, text)
        return await msg.answer(f"✅ Deadline set to <b>{text}</b>", reply_markup=main_kb())
    except ValueError:
        pass

    await msg.answer(
        "⚠️ Couldn't parse that. Try something like <b>21:00</b>, <b>tomorrow 09:00</b>, <b>next mon 14:30</b>, "
        "<b>in 2h</b>, or the exact <b>YYYY-MM-DD HH:MM</b>. Tap <b>ℹ Examples</b> for more.",
        reply_markup=main_kb()
    )



@dp.callback_query(F.data.startswith("edit_prio:"))
async def cb_edit_prio(call: CallbackQuery):
    tid = int(call.data.split(":",1)[1])
    await call.answer()
    await call.message.answer("Select new priority:", reply_markup=priority_kb(tid))

@dp.callback_query(F.data.startswith("edit_tags:"))
async def cb_edit_tags(call: CallbackQuery):
    tid = int(call.data.split(":",1)[1])
    await call.answer()
    await call.message.answer("Select new tag:", reply_markup=tags_kb(tid))

@dp.callback_query(F.data.startswith("set_prio:"))
async def cb_set_prio(call: CallbackQuery):
    _,tid,prio = call.data.split(":")
    await update_task_priority(int(tid), prio)
    await call.answer(f"Priority → {prio}")
    await call.message.edit_reply_markup()

@dp.callback_query(F.data.startswith("set_tag:"))
async def cb_set_tag(call: CallbackQuery):
    _,tid,tag = call.data.split(":")
    await update_task_tags(int(tid), tag)
    await call.answer(f"Tag → {tag}")
    await call.message.edit_reply_markup()

@dp.callback_query(F.data.startswith("approve_group:"))
async def cb_approve_group(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_ID:
        return await cb.answer("Not allowed.", show_alert=True)

    req_id = int(cb.data.split(":")[1])
    clan = await approve_clan_creation_request(req_id)

    if clan:
        await cb.answer("✅ Group approved and created!")
        await cb.message.edit_text("✅ Group approved and created.")

        # 1) Drop them into Groups menu (has 🔙 Back)
        await bot.send_message(
            chat_id=clan.owner_id,
            text=f"🎉 Your study group '<b>{clan.name}</b>' has been approved and is now live!\n\n"
                 "What would you like to do next?",
            parse_mode=ParseMode.HTML,
            reply_markup=groups_main_kb(True)
        )

        # 2) Also offer the global main menu as an exit
        await bot.send_message(
            chat_id=clan.owner_id,
            text="🏠 Main menu:",
            reply_markup=main_kb()
        )
    else:
        await cb.answer("Failed to approve.", show_alert=True)



@dp.callback_query(F.data.startswith("reject_group:"))
async def cb_reject_group(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_ID:
        return await cb.answer("Not allowed.", show_alert=True)
    req_id = int(cb.data.split(":")[1])
    ok = await reject_clan_creation_request(req_id)
    if ok:
        await cb.answer("Group request rejected.")
        await cb.message.edit_text("❌ Group creation request rejected.")
    else:
        await cb.answer("Failed to reject.", show_alert=True)




@dp.callback_query(F.data == "set_group_photo")
async def cb_set_group_photo(cb: CallbackQuery):
    uid = str(cb.from_user.id)
    user_clans = await get_user_clans(uid)
    if not user_clans or user_clans[0].owner_id != uid:
        return await cb.answer("Only founders can set the cover photo.", show_alert=True)
    clan = user_clans[0]
    users_editing_group_photo[uid] = clan.id
    await cb.message.answer("🖼 Send a <b>photo</b> to use as your group cover.")
    await cb.answer()


@dp.message(lambda m: str(m.from_user.id) in users_editing_group_photo and m.photo)
async def handle_group_photo(msg: Message):
    uid = str(msg.from_user.id)
    clan_id = users_editing_group_photo.pop(uid)
    fid = msg.photo[-1].file_id  # highest resolution
    ok = await set_clan_image(clan_id, fid)
    if ok:
        await msg.answer("✅ Group photo updated!", reply_markup=main_kb())
    else:
        await msg.answer("❌ Failed to update group photo.", reply_markup=main_kb())


@dp.message(lambda m: str(m.from_user.id) in user_edit_text and m.text not in VALID_BUTTON_TEXTS)
async def handle_edit_text(msg: Message):
    uid = str(msg.from_user.id)
    tid = user_edit_text.pop(uid)
    await update_task_text(tid, msg.text)
    await msg.answer("✅ Text updated!", reply_markup=main_kb())



@dp.message(lambda m: str(m.from_user.id) in user_adding_task and m.text not in VALID_BUTTON_TEXTS)
async def catch_add_task(msg: Message):
    uid    = str(msg.from_user.id)
    pending= await get_tasks(uid)
    if len(pending) >= 10:
        user_adding_task.discard(uid)
        user_add_scope.pop(uid, None)
        return await msg.answer("⚠️ You already have 10 pending tasks.", reply_markup=main_kb())

    parsed = parse_tasks_text(msg.text)
    if not parsed:
        return await msg.answer("⚠️ Invalid format. Try again.", reply_markup=main_kb())

    scope = user_add_scope.get(uid, "free")
    added = 0

    if scope == "free":
        await add_tasks_bulk(uid, parsed)
        added = len(parsed)
    else:
        dt = deadline_for_scope(scope, tz_name="Asia/Tashkent")
        deadline_str = dt.strftime("%Y-%m-%d %H:%M")
        for text in parsed:
            try:
                await add_task(uid, text, deadline=deadline_str)
            except TypeError:
                # if your add_task signature is positional (text, deadline)
                await add_task(uid, text, deadline_str)
            added += 1

    user_adding_task.discard(uid)
    user_add_scope.pop(uid, None)
    pretty = {"today":"🌞 Today","week":"🗓️ This Week","month":"📆 This Month","free":"📝 No deadline"}[scope]
    await msg.answer(f"✅ Added {added} task(s) ({pretty}).", reply_markup=main_kb())


# — Profile, Leaderboard, Report —



@dp.message(F.text == BUTTONS["PROFILE"])
async def profile(msg: Message):
    user = await get_user(str(msg.from_user.id))
    curr, nxt, pct = _rank_progress(user.xp)
    bar = _bar10(pct)  # ▰▰▰▱… 10-seg bar
    next_line = f"➡️ Next: {'⚡ ' + nxt if nxt else '—'}"

    text = (
        "👤 <b>Profile</b>\n"
        f"🆔 ID: <code>{user.id}</code>\n"
        f"🪪 Name: {_clip(user.name, 40)}\n"
        f"🔗 Username: {_safe_username(user)}\n"
        f"🏷 Title: {user.extra_title or 'None'}\n"
        f"✅ Completed: {user.completed}\n"
        f"🔥 Streak: {user.streak} days\n"
        f"⚡ XP: {user.xp} ({curr})\n"
        f"{bar}\n"

        f"{next_line}"
    )
    await msg.answer(text, parse_mode=ParseMode.HTML, reply_markup=main_kb())




@dp.message(F.text == BUTTONS["LB"])
async def leaderboard(msg: Message):
    top = await get_top_users(10)
    lines = [
        f"{i+1}. {u.name}{' | ⚜️ '+u.extra_title+' ⚜️' if u.extra_title else ''}"
        f" – {u.xp} XP ({get_rank(u.xp)})"
        for i,u in enumerate(top)
    ]
    await msg.answer(
    "🏅 <b>Top 10 Leaderboard</b>\n" + "\n".join(lines) +
    "\n\n<b>Ranks:</b>\n🎯 Rookie under 200 XP\n⚡ Achiever 200–499\n🔥 Crusher 500–1199\n"
    "🏆 Master 1200–2499\n🌟 Legend 2500+",
    parse_mode=ParseMode.HTML
    )


@dp.message(F.text == BUTTONS["REPORT"])
async def daily_report(msg: Message):
    user  = await get_user(str(msg.from_user.id))
    tasks = await get_tasks(user.id)
    comp  = user.completed
    pend  = len(tasks)
    total = comp + pend
    pct   = int((comp/total)*100) if total else 0

    curr, _, _ = _rank_progress(user.xp)
    bar = _bar10(pct)

    mot = MOTIVATIONS[datetime.now().day % len(MOTIVATIONS)]
    tip = TIPS[datetime.now().day % len(TIPS)]

    text = (
        "📊 <b>Daily Report</b>\n"
        f"✅ Completed: {comp}\n"
        f"🕐 Pending: {pend}\n"
        f"🎯 Progress: {bar}\n"
        f"🔥 Streak: {user.streak} days\n"
        f"⚡ XP: {user.xp} ({curr})\n\n"
        f"{mot}\n"
        f"{tip}"
    )
    await msg.answer(text, parse_mode=ParseMode.HTML, reply_markup=main_kb())


# — Study Groups (Clans) System —

@dp.message(F.text == BUTTONS["GROUPS"])
async def groups_overview(msg: Message):
    uid = str(msg.from_user.id)
    user_clans = await get_user_clans(uid)
    is_in_group = bool(user_clans)

    cta = (
        f"🎉 You’re in <b>{user_clans[0].name}</b> — open <b>👥 My Study Group</b> to see members and tools."
        if is_in_group
        else "🌎 <b>Browse Groups</b> to discover teams, or ✨ <b>Request to Create Group</b> and start your own."
    )

    overview = (
        "📚 <b>Study Groups</b>\n"
        "🤝 Team up, set shared goals, and keep each other accountable.\n"
        "1️⃣ You can be in <b>one</b> group at a time.\n\n"
        "🏆 <b>What you get</b>\n"
        "• 🏅 Leaderboard & group XP\n"
        "• 👥 Member list & invites\n"
        "• 🛡️ Founder tools (approve, edit, kick, set cover photo)\n"
        "• 🔗 Group link & 📌 requirements on the group card\n\n"
        f"{cta}\n\n"
        "💡 <i>Tip:</i> A clear name, short description, and friendly rules attract motivated members!"
    )

    await msg.answer(overview, parse_mode=ParseMode.HTML, reply_markup=groups_main_kb(is_in_group))



@dp.message(F.text.in_(["🌍 Browse Groups", "🌎 Browse Groups"]))
async def browse_groups(msg: Message):
    uid = str(msg.from_user.id)
    all_clans = [c for c in await get_all_clans() if c.is_approved]
    if not all_clans:
        return await msg.answer("No study groups exist yet. You can be the first to create one!")
    user_group_page[uid] = 0
    await show_group_card(msg, all_clans[0], 1, len(all_clans))



@dp.message(lambda m: m.text == "➕ Request to Create Group")
async def request_create_group(msg: Message):
    users_requesting_group.add(str(msg.from_user.id))
    await msg.answer(
        "📝 To request a new Study Group, send the following info as ONE message:\n"
        "- Group Name\n- Description\n- Requirements (optional)\n"
        "- Your Telegram username\n\nExample:\n"
        "<b>Team Infinity</b>\nA group for highly motivated students...\nRequirements: At least 3 tasks/day\n@username"
    )

@dp.message(F.text == "👥 My Study Group")
async def my_study_group(msg: Message):
    uid = str(msg.from_user.id)
    user_clans = await get_user_clans(uid)
    if not user_clans:
        return await msg.answer("You are not in any study group yet.")
    clan = user_clans[0]
    founder = clan.owner
    is_founder = (uid == clan.owner_id)

    # Card content
    desc = (
        f"🛡️ <b>{clan.name}</b>\n"
        f"🔺 Members: {len(clan.members)}\n"
        f"🗣️ {clan.description or 'No description'}\n"
        f"📌 {clan.requirements or 'No requirements'}\n"
        f"👤 Founder: {uname(founder)}\n"
        f"🔗 Link: {clan.link or '—'}\n"
    )

    # Buttons
    rows = [[InlineKeyboardButton(text="👥 Members", callback_data=f"clan_members:{clan.id}:0")]]

    if is_founder:
        pending_apps = await get_pending_applications_for_founder(uid)
        if pending_apps:
            desc += "\n<b>Pending Join Requests</b>:\n" + "\n".join(
                f"• {uname(user)}" for app, _clan, user in pending_apps
            )
        rows.append([InlineKeyboardButton(text="✏️ Edit Group Info", callback_data="edit_group_info")])
        rows.append([InlineKeyboardButton(text="🗑️ Delete Group", callback_data=f"delete_group:{clan.id}")])
    else:
        rows.append([InlineKeyboardButton(text="🚪 Leave Group", callback_data=f"clan_leave:{clan.id}")])

    kb = InlineKeyboardMarkup(inline_keyboard=rows)

    # Prefer showing cover photo if set
    if getattr(clan, "image_url", ""):
        try:
            await msg.answer_photo(clan.image_url, caption=desc, parse_mode=ParseMode.HTML, reply_markup=kb)
            return
        except Exception:
            pass  # fall back to text if photo send fails

    await msg.answer(desc, parse_mode=ParseMode.HTML, reply_markup=kb)



@dp.callback_query(F.data.startswith("clan_leave:"))
async def clan_leave_cb(cb: CallbackQuery):
    uid = str(cb.from_user.id)
    clan_id = int(cb.data.split(":")[1])

    clan = await get_clan_by_id(clan_id)
    if not clan:
        return await cb.answer("Group not found.", show_alert=True)

    if uid == clan.owner_id:
        return await cb.answer("Founders can’t leave. Delete the group instead.", show_alert=True)

    status = await remove_member_from_clan(clan_id, uid)
    if status == "removed":
        await cb.answer("You left the group.")
        return await cb.message.answer("👋 You left the study group.", reply_markup=groups_main_kb(False))
    elif status == "not_member":
        return await cb.answer("You’re not in this group.", show_alert=True)
    else:  # founder/not_found fallback
        return await cb.answer("Couldn’t leave right now.", show_alert=True)



    
@dp.callback_query(F.data == "edit_group_info")
async def cb_edit_group_info(cb: CallbackQuery):
    uid = str(cb.from_user.id)
    user_clans = await get_user_clans(uid)
    if not user_clans or user_clans[0].owner_id != uid:
        return await cb.answer("Only founders can edit group info.", show_alert=True)
    clan = user_clans[0]
    users_editing_group[uid] = clan.id
    await cb.message.answer(
        "Send the new group info as 4 lines:\n"
        "Name\nDescription\nRequirements\nLink"
    )

    
@dp.callback_query(F.data.startswith("group_page:"))
async def group_page_cb(cb: CallbackQuery):
    uid = str(cb.from_user.id)
    page = int(cb.data.split(":")[1])
    all_clans = [c for c in await get_all_clans() if c.is_approved]
    if not all_clans:
        return await cb.answer("No study groups exist yet.", show_alert=True)
    user_group_page[uid] = page
    await cb.message.delete()  # Remove previous card
    await show_group_card_paginated(cb.message, all_clans, page)



@dp.message(lambda m: str(m.from_user.id) in users_editing_group)
async def handle_edit_group_info(msg: Message):
    uid = str(msg.from_user.id)
    clan_id = users_editing_group.pop(uid)
    lines = msg.text.split("\n")
    name = lines[0].strip() if len(lines) > 0 else ""
    desc = lines[1].strip() if len(lines) > 1 else ""
    reqs = lines[2].strip() if len(lines) > 2 else ""
    link = lines[3].strip() if len(lines) > 3 else ""
    await update_clan_info(clan_id, name, desc, reqs, link)
    await msg.answer("Group info updated!", reply_markup=main_kb())


@dp.message(lambda m: str(m.from_user.id) in users_requesting_group)
async def handle_group_request(msg: Message):
    users_requesting_group.discard(str(msg.from_user.id))
    lines = msg.text.strip().split("\n")
    if len(lines) < 2:
        return await msg.answer("Invalid format. Please provide group name and description at least.")
    group_name = lines[0]
    description = lines[1]
    requirements = lines[2] if len(lines) > 2 else ""
    username = msg.from_user.username or ""
    link = ""
    # (You can parse for a group link if provided)
    await create_clan_creation_request(str(msg.from_user.id), username, group_name, description, requirements, link)
    await msg.answer("✅ Your study group creation request was submitted! The admin will review it soon.")

#admin handle
@dp.message(Command("group_requests"))
async def show_pending_group_requests(msg: Message):
    if msg.from_user.id != ADMIN_ID:
        return
    requests = await get_pending_clan_creation_requests()
    if not requests:
        return await msg.answer("No pending study group requests.")
    for req in requests:
        text = (
            f"ID: {req.id}\n"
            f"User: @{req.username or req.user_id}\n"
            f"Name: {req.group_name}\n"
            f"Desc: {req.description}\n"
            f"Reqs: {req.requirements}\n"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Approve", callback_data=f"approve_group:{req.id}"),
                InlineKeyboardButton(text="❌ Reject", callback_data=f"reject_group:{req.id}")
            ]
        ])
        await msg.answer(text, reply_markup=kb)
   

async def show_group_card(msg, clan, idx, total):
    uid = str(msg.from_user.id)
    user_clans = await get_user_clans(uid)
    is_member = any(m.id == clan.id for m in user_clans)
    founder = clan.owner

    caption = (
        f"🛡️ <b>{clan.name}</b>\n"
        f"🔺 Members: {len(clan.members)}\n"
        f"🗣️ {clan.description or 'No description'}\n"
        f"📌 {clan.requirements or 'No requirements'}\n"
        f"👤 Founder: {uname(founder)}\n"
        + (f"🔗 Link: {clan.link or '—'}\n" if is_member else "🔗 Link: <i>Join to view</i>\n")
        + f"\n({idx}/{total})"
    )

    # nav buttons
    prev_id = f"group_page:{(idx-2) % total}"
    next_id = f"group_page:{(idx) % total}"
    nav_row = [
        InlineKeyboardButton(text="⏮ Prev", callback_data=prev_id),
        InlineKeyboardButton(text="⏭ Next", callback_data=next_id),
    ]

    if is_member:
        first_row = [InlineKeyboardButton(text="👥 Members", callback_data=f"clan_members:{clan.id}:0")]
    else:
        first_row = [InlineKeyboardButton(text="Apply", callback_data=f"apply_clan:{clan.id}")]

    kb = InlineKeyboardMarkup(inline_keyboard=[first_row, nav_row])

    if clan.image_url:
        try:
            await msg.answer_photo(clan.image_url, caption=caption, parse_mode=ParseMode.HTML, reply_markup=kb)
            return
        except Exception:
            pass  # fall back to text if sending photo fails

    await msg.answer(caption, parse_mode=ParseMode.HTML, reply_markup=kb)

def _members_page_kb(clan_id: int, page: int, pages: int):
    prev_p = (page - 1) % pages
    next_p = (page + 1) % pages
    rows = [[
        InlineKeyboardButton(text="⏮ Prev", callback_data=f"clan_members:{clan_id}:{prev_p}"),
        InlineKeyboardButton(text=f"{page+1}/{pages}", callback_data="noop"),
        InlineKeyboardButton(text="⏭ Next", callback_data=f"clan_members:{clan_id}:{next_p}"),
    ],
    [InlineKeyboardButton(text="⬅️ Back", callback_data="back_my_group")]]
    return InlineKeyboardMarkup(inline_keyboard=rows)

@dp.callback_query(F.data.startswith("clan_members:"))
async def clan_members_cb(cb: CallbackQuery):
    uid = str(cb.from_user.id)
    parts = cb.data.split(":")
    clan_id = int(parts[1])
    page = int(parts[2]) if len(parts) >= 3 else 0

    # Must be a member to view
    user_clans = await get_user_clans(uid)
    if not any(c.id == clan_id for c in user_clans):
        return await cb.answer("Only members can view the list.", show_alert=True)

    clan = await get_clan_by_id(clan_id)
    if not clan:
        return await cb.answer("Group not found.", show_alert=True)

    is_founder = (uid == clan.owner_id)

    rows = await list_clan_members(clan_id)  # [(ClanMember, User)]
    users = [u for _, u in rows]

    per_page = 10
    pages = max(1, (len(users) + per_page - 1) // per_page)
    page = page % pages
    start = page * per_page
    chunk = users[start:start+per_page]

    lines = []
    for u in chunk:
        tag = " (Founder)" if str(u.id) == clan.owner_id else ""
        handle = f"@{u.username}" if u.username else u.name
        lines.append(f"• {handle}{tag}")

    text = f"👥 <b>{clan.name}</b> — Members ({len(users)})\n\n" + ("\n".join(lines) if lines else "No members yet.")

    # Build keyboard: kick rows if founder, then pager + back
    kb_rows = []
    if is_founder:
        for u in chunk:
            if str(u.id) == clan.owner_id:
                continue
            handle = f"@{u.username}" if u.username else u.name
            kb_rows.append([InlineKeyboardButton(text=f"❌ Kick {handle}", callback_data=f"kick_member:{clan.id}:{u.id}:{page}")])

    prev_p = (page - 1) % pages
    next_p = (page + 1) % pages
    kb_rows.append([
        InlineKeyboardButton(text="⏮ Prev", callback_data=f"clan_members:{clan.id}:{prev_p}"),
        InlineKeyboardButton(text=f"{page+1}/{pages}", callback_data="noop"),
        InlineKeyboardButton(text="⏭ Next", callback_data=f"clan_members:{clan.id}:{next_p}"),
    ])
    kb_rows.append([InlineKeyboardButton(text="⬅️ Back", callback_data="back_my_group")])

    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)

    try:
        await cb.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
    except:
        await cb.message.answer(text, parse_mode=ParseMode.HTML, reply_markup=kb)
    await cb.answer()

@dp.callback_query(F.data.startswith("leave_clan:"))
async def leave_clan_cb(cb: CallbackQuery):
    uid = str(cb.from_user.id)
    clan_id = int(cb.data.split(":")[1])

    # must be a member
    user_clans = await get_user_clans(uid)
    if not any(c.id == clan_id for c in user_clans):
        return await cb.answer("You're not in this group.", show_alert=True)

    status = await remove_member_from_clan(clan_id, uid)
    if status == "founder":
        return await cb.answer("Founders can’t leave. Delete the group or transfer ownership.", show_alert=True)
    if status != "removed":
        return await cb.answer("Couldn’t leave the group.", show_alert=True)

    await cb.answer("You left the group.")
    await cb.message.answer("👋 You left the study group.", reply_markup=main_kb())

    # notify founder (optional)
    clan = await get_clan_by_id(clan_id)
    try:
        await bot.send_message(clan.owner_id, f"ℹ️ User {uid} left <b>{clan.name}</b>.", parse_mode=ParseMode.HTML)
    except:
        pass


@dp.callback_query(F.data.startswith("kick_member:"))
async def kick_member_cb(cb: CallbackQuery):
    # format: kick_member:<clan_id>:<user_id>:<page>
    _, c_str, u_str, p_str = cb.data.split(":")
    clan_id = int(c_str); target_id = u_str; page = int(p_str)

    clan = await get_clan_by_id(clan_id)
    if not clan or str(cb.from_user.id) != clan.owner_id:
        return await cb.answer("Only the founder can kick members.", show_alert=True)
    if target_id == clan.owner_id:
        return await cb.answer("You can’t kick the founder.", show_alert=True)

    status = await remove_member_from_clan(clan_id, target_id)
    if status != "removed":
        return await cb.answer("Couldn’t remove that member.", show_alert=True)

    await cb.answer("Removed.")
    # notify kicked user
    try:
        await bot.send_message(target_id, f"🚫 You were removed from <b>{clan.name}</b> by the founder.", parse_mode=ParseMode.HTML)
    except:
        pass

    # refresh current members page
    await clan_members_cb(
        type("Obj", (), {"data": f"clan_members:{clan_id}:{page}", "from_user": cb.from_user, "message": cb.message})  # quick reuse
    )



@dp.callback_query(F.data == "back_my_group")
async def back_my_group_cb(cb: CallbackQuery):
    # re-render the group overview for the current user's group
    await cb.answer()
    # send fresh details
    await my_study_group(cb.message)


@dp.callback_query(F.data.startswith("apply_clan:"))
async def cb_apply_clan(cb: CallbackQuery):
    clan_id = int(cb.data.split(":")[1])
    uid = str(cb.from_user.id)
    user_clans = await get_user_clans(uid)
    if user_clans:
        return await cb.answer("You can only be in one study group!", show_alert=True)
    await create_clan_application(clan_id, uid)
    await cb.answer("Application sent! The founder will review it.", show_alert=True)
    clan = await get_clan_by_id(clan_id)
    founder_id = clan.owner_id
    applicant = await get_user_by_id(uid)
    accept_cb = f"app_accept:{clan_id}:{uid}"
    reject_cb = f"app_reject:{clan_id}:{uid}"
    await bot.send_message(
        founder_id,
        f"📝 <b>New join request</b> to <b>{clan.name}</b> from @{applicant.username or applicant.name}.",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Accept", callback_data=accept_cb),
                InlineKeyboardButton(text="❌ Reject", callback_data=reject_cb)
            ]
        ])
    )

@dp.callback_query(F.data.startswith("app_accept:"))
async def cb_accept_application(cb: CallbackQuery):
    _, clan_id, user_id = cb.data.split(":")
    clan_id = int(clan_id)
    await approve_application(clan_id, user_id)
    await cb.answer("User accepted!")
    await bot.send_message(user_id, "🎉 You were accepted into the study group! Welcome!")

@dp.callback_query(F.data.startswith("delete_group:"))
async def cb_delete_group(cb: CallbackQuery):
    clan_id = int(cb.data.split(":")[1])
    uid = str(cb.from_user.id)
    # Call helper to delete
    ok = await delete_clan(clan_id)
    if ok:
        await cb.answer("Group deleted.")
        await cb.message.answer("Your study group was deleted.", reply_markup=main_kb())
    else:
        await cb.answer("Failed to delete group. Only the founder can delete.", show_alert=True)


@dp.callback_query(F.data.startswith("app_reject:"))
async def cb_reject_application(cb: CallbackQuery):
    _, clan_id, user_id = cb.data.split(":")
    clan_id = int(clan_id)
    await reject_application(clan_id, user_id)
    await cb.answer("User rejected.")
    await bot.send_message(user_id, "❌ Your study group application was rejected.")



@dp.message(F.text == "📝 Apply to Study Group")
async def apply_group_info(msg: Message):
    await msg.answer(
        "To join a group, browse available study groups with '🌍 Browse Groups' and apply from there. "
        "If you want to create your own group, contact the admin @hae_sung1."
    )


@dp.message(F.text == "🔙 Back")
async def back_to_main(msg: Message):
    await msg.answer("Returned to main menu.", reply_markup=main_kb())


@dp.message(Command("admin"))
async def admin_entry(msg: Message):
    if msg.from_user.id != ADMIN_ID:
        return
    await msg.answer("🛠 <b>Admin Panel</b>", parse_mode=ParseMode.HTML, reply_markup=admin_menu_kb())
#ADMIN ADMIN ADMIN ADMIN 
@dp.message(F.text == "📣 Broadcast")
async def admin_broadcast_start(msg: Message):
    if msg.from_user.id != ADMIN_ID: return
    admin_broadcast_wait.add(msg.from_user.id)
    await msg.answer("Send the <b>message</b> to broadcast to all users.\n(Reply with text/photo/caption)")


@dp.message(lambda m: m.from_user.id == ADMIN_ID and m.from_user.id in admin_broadcast_wait)
async def admin_broadcast_do(msg: Message):
    # exit "waiting" mode
    admin_broadcast_wait.discard(msg.from_user.id)

    user_ids = await get_all_user_ids()
    total = len(user_ids)
    sent = 0
    failed = 0

    # decide what to send once
    is_photo = bool(msg.photo)
    photo_id = msg.photo[-1].file_id if is_photo else None
    text = (msg.caption or "") if is_photo else (msg.text or "")

    for i, uid in enumerate(user_ids, start=1):
        try:
            if is_photo:
                await msg.bot.send_photo(uid, photo_id, caption=text)
            else:
                await msg.bot.send_message(uid, text)
            sent += 1
        except Exception:
            failed += 1
        finally:
            # ~16–20 msgs/s is safe vs Telegram global limits
            await asyncio.sleep(0.06)

        # brief breather every 500 to dodge bursts
        if i % 500 == 0:
            await asyncio.sleep(1.0)

    await msg.answer(
        f"✅ Broadcast finished.\nSent: {sent}/{total}  •  Failed: {failed}",
        reply_markup=admin_menu_kb()
    )


@dp.message(F.text == "➕ Add XP")
async def admin_addxp_start(msg: Message):
    if msg.from_user.id != ADMIN_ID: return
    admin_addxp_wait.add(msg.from_user.id)
    await msg.answer("Send: `<user_id or @username> <xp_delta>`", parse_mode=ParseMode.HTML)

@dp.message(lambda m: m.from_user.id == ADMIN_ID and m.from_user.id in admin_addxp_wait)
async def admin_addxp_do(msg: Message):
    admin_addxp_wait.discard(msg.from_user.id)
    try:
        ident, delta = msg.text.strip().split(maxsplit=1)
        delta = int(delta)
        user = await get_user_by_username(ident) if ident.startswith("@") else await get_user(str(ident))
        if not user:
            return await msg.answer("User not found.", reply_markup=admin_menu_kb())
        await update_xp_streak_completed(user.id, xp_delta=delta)
        await msg.answer(f"✅ Added {delta} XP to {user.name} ({user.id}).", reply_markup=admin_menu_kb())
    except Exception as e:
        await msg.answer(f"Format error. Use: `<id|@username> <xp>`", parse_mode=ParseMode.HTML, reply_markup=admin_menu_kb())

@dp.message(F.text == "🏷️ Set Title")
async def admin_settitle_start(msg: Message):
    if msg.from_user.id != ADMIN_ID: return
    admin_settitle_wait.add(msg.from_user.id)
    await msg.answer("Send: `<user_id or @username> | <title>`", parse_mode=ParseMode.HTML)

@dp.message(lambda m: m.from_user.id == ADMIN_ID and m.from_user.id in admin_settitle_wait)
async def admin_settitle_do(msg: Message):
    admin_settitle_wait.discard(msg.from_user.id)
    if "|" not in msg.text:
        return await msg.answer("Format error. Use: `<id|@username> | <title>`", reply_markup=admin_menu_kb())
    ident, title = [x.strip() for x in msg.text.split("|", 1)]
    user = await get_user_by_username(ident) if ident.startswith("@") else await get_user(str(ident))
    if not user:
        return await msg.answer("User not found.", reply_markup=admin_menu_kb())
    await set_user_title(user.id, title)
    await msg.answer(f"✅ Title set for {user.name}: {title}", reply_markup=admin_menu_kb())

@dp.message(F.text == "⬅️ Exit Admin")
async def admin_exit(msg: Message):
    if msg.from_user.id != ADMIN_ID: return
    await msg.answer("Leaving admin panel.", reply_markup=main_kb())

@dp.message(F.text == "📈 Stats")
async def admin_stats(msg: Message):
    if msg.from_user.id != ADMIN_ID: return
    s = await admin_get_counts()
    text = (
        "📈 <b>Stats</b>\n"
        f"Users: <b>{s['users']}</b>\n"
        f"Tasks: <b>{s['tasks_total']}</b> | ✅ Done: {s['tasks_done']} | ⏳ Open: {s['tasks_open']}\n"
        f"Clans: <b>{s['clans']}</b>\n"
        f"Pending: Join <b>{s['join_pending']}</b>, Create <b>{s['create_pending']}</b>"
    )
    await msg.answer(text, parse_mode=ParseMode.HTML, reply_markup=admin_menu_kb())

@dp.message(F.text == "🗂 Groups (Admin)")
async def admin_groups_root(msg: Message):
    if msg.from_user.id != ADMIN_ID: return
    total, rows, counts = await admin_get_groups_page(page=0)
    if not rows:
        return await msg.answer("No groups yet.", reply_markup=admin_menu_kb())

    # one message per group page – show first page as a list of buttons
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"{'✅' if c.is_approved else '⛔'} {c.name} • {counts.get(c.id,0)}",
            callback_data=f'admin_group_open:{c.id}'
        )] for c in rows
    ])
    nav = admin_groups_kb(0, total, per_page=6)
    kb.inline_keyboard += nav.inline_keyboard
    await msg.answer("🗂 <b>Groups</b> (most recent first)", parse_mode=ParseMode.HTML, reply_markup=kb)

@dp.callback_query(F.data.startswith("admin_groups:"))
async def admin_groups_page(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_ID: return await cb.answer()
    page = int(cb.data.split(":")[1])
    total, rows, counts = await admin_get_groups_page(page=page)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"{'✅' if c.is_approved else '⛔'} {c.name} • {counts.get(c.id,0)}",
            callback_data=f'admin_group_open:{c.id}'
        )] for c in rows
    ])
    nav = admin_groups_kb(page, total, per_page=6)
    kb.inline_keyboard += nav.inline_keyboard
    try:
        await cb.message.edit_reply_markup(kb)
    except:
        await cb.message.edit_text("🗂 Groups", reply_markup=kb)
    await cb.answer()

@dp.callback_query(F.data.startswith("admin_group_open:"))
async def admin_group_open(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_ID: return await cb.answer()
    clan_id = int(cb.data.split(":")[1])
    clan = await get_clan_by_id(clan_id)
    if not clan:
        return await cb.answer("Not found", show_alert=True)
    members = len(clan.members)
    text = (
        f"🛡️ <b>{clan.name}</b>\n"
        f"Owner: <code>{clan.owner_id}</code>\n"
        f"Approved: {'Yes' if clan.is_approved else 'No'}\n"
        f"Members: {members}\n"
        f"Link: {clan.link or '—'}\n"
        f"Created: {clan.created_at}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=("Hide from Browse ⛔" if clan.is_approved else "Approve ✅"),
            callback_data=f"admin_group_toggle:{clan.id}:{0 if clan.is_approved else 1}"
        )],
        [InlineKeyboardButton(text="🗑 Delete", callback_data=f"admin_group_delete:{clan.id}")],
        [InlineKeyboardButton(text="⬅ Back to list", callback_data="admin_groups:0")]
    ])
    await cb.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
    await cb.answer()

@dp.callback_query(F.data.startswith("admin_group_toggle:"))
async def admin_group_toggle(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_ID: return await cb.answer()
    _, cid, flag = cb.data.split(":")
    ok = await admin_toggle_group_approved(int(cid), bool(int(flag)))
    await cb.answer("Updated" if ok else "Failed")
    # re-open details to reflect
    await admin_group_open(cb)

@dp.callback_query(F.data.startswith("admin_group_delete:"))
async def admin_group_delete(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_ID: return await cb.answer()
    cid = int(cb.data.split(":")[1])
    ok = await delete_clan(cid)
    if ok:
        await cb.answer("Deleted")
        await admin_groups_root(cb.message)  # back to list
    else:
        await cb.answer("Failed to delete", show_alert=True)



# — Reminders Loop (SQL-filtered windows) —
async def check_reminders():
    while True:
        try:
            # 1-hour window: [60, 61) minutes (catch the minute edge once)
            pairs_1h = await get_tasks_for_reminder_window(60, 61, sent_flag=0, tz_name="Asia/Tashkent")
            if pairs_1h:
                ids = []
                for usr, t in pairs_1h:
                    try:
                        await bot.send_message(usr.id, f"⏰ 1h to go: {t.text}")
                        ids.append(t.id)
                    except Exception:
                        pass
                await bulk_update_task_reminders_sent(ids, 1)

            # 10-minute window: [10, 11) minutes
            pairs_10m = await get_tasks_for_reminder_window(10, 11, sent_flag=1, tz_name="Asia/Tashkent")
            if pairs_10m:
                ids = []
                for usr, t in pairs_10m:
                    try:
                        await bot.send_message(usr.id, f"⚠️ 10m left: {t.text}")
                        ids.append(t.id)
                    except Exception:
                        pass
                await bulk_update_task_reminders_sent(ids, 2)

        except Exception:
            # optional: print or log
            pass

        # tick every ~60s is enough
        await asyncio.sleep(60)



# --- Groups leaderboard (🎐 Total / 🎏 Avg, spaced layout) ---

@dp.callback_query(F.data == "noop")
async def noop_cb(cb: CallbackQuery):
    await cb.answer()

def _lb_kb(mode: str) -> InlineKeyboardMarkup:
    # highlight the active tab with • •
    left  = "• 🎐 Total XP •" if mode == "total" else "🎐 Total XP"
    right = "• 🎏 Avg XP •"   if mode == "avg"   else "🎏 Avg XP"
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=left,  callback_data="clb:total"),
        InlineKeyboardButton(text=right, callback_data="clb:avg"),
    ]])

def _lb_badge(rank: int) -> str:
    if rank == 1: return "🥇"
    if rank == 2: return "🥈"
    if rank == 3: return "🥉"
    if rank <= 5: return "🏅"
    return "🔹"

async def _send_clans_lb(target, mode: str = "total", limit: int = 10):
    rows = await get_clans_xp_leaderboard(limit=limit, mode=mode)

    title    = "🏆 <b>Study Groups Leaderboard</b>"
    subtitle = "🎐 <b>Total XP</b>" if mode == "total" else "🎏 <b>Average XP</b>"
    metric_e = "⚡" if mode == "total" else "📈"

    lines = []
    for i, (clan, member_count, total_xp, avg_xp) in enumerate(rows, 1):
        badge  = _lb_badge(i)
        metric = int(total_xp) if mode == "total" else int(avg_xp or 0)
        # one item per block, spaced with an empty line
        lines.append(f"{badge} {i}. <b>{clan.name}</b>\n{metric_e} <b>{metric}</b>  •  👥 {member_count}")

    legend = (
        "\n\n<i>Legend:</i> 🎐 Total = sum of members’ XP  •  🎏 Avg = average per member  •  👥 members"
    )

    if lines:
        text = f"{title}\n{subtitle}\n\n" + "\n\n".join(lines) + legend
    else:
        text = f"{title}\n{subtitle}\n\nNo groups yet."

    kb = _lb_kb(mode)

    if isinstance(target, Message):
        await target.answer(text, parse_mode=ParseMode.HTML, reply_markup=kb)
    else:
        try:
            await target.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
        except:
            await target.message.answer(text, parse_mode=ParseMode.HTML, reply_markup=kb)

@dp.message(F.text == "🏆 Groups Leaderboard")
async def groups_leaderboard(msg: Message):
    await _send_clans_lb(msg, mode="total", limit=10)

@dp.callback_query(F.data.startswith("clb:"))
async def groups_leaderboard_toggle(cb: CallbackQuery):
    mode = cb.data.split(":")[1]  # "total" | "avg"
    await _send_clans_lb(cb, mode=mode, limit=10)
    await cb.answer()



# — Run —
async def main():
    print("✅ Smart Planner v2 running…")
    asyncio.create_task(check_reminders())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())

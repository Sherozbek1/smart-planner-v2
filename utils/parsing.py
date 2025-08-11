from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from datetime import datetime, timedelta
import re, pytz
from calendar import monthrange

# — Smart parsing of free-form text into individual tasks —
def parse_tasks_text(text: str) -> list[str]:
    """
    Turn a block of text into a list of task strings.
    Supports:
      • Numbered lines "1. Task" or "1) Task"
      • Bullets "- Task" or "* Task"
      • Plain lines
    """
    lines = text.strip().splitlines()
    tasks: list[str] = []
    numbered = re.compile(r"^\s*(\d+)\s*[\.\)]\s*(.+)")
    for line in lines:
        m = numbered.match(line)
        if m:
            tasks.append(m.group(2).strip())
        elif line.strip().startswith(("-", "*")):
            tasks.append(line.strip()[1:].strip())
        else:
            tasks.append(line.strip())
    return [t for t in tasks if t]

# — Quick-add templates —
TEMPLATES: dict[str, list[str]] = {
    "Add 5 Tasks": [f"Task {i+1}" for i in range(5)],
    "Daily Standup": [
        "Daily standup: share what I did yesterday",
        "Daily standup: plan what I’ll do today",
        "Daily standup: call out blockers"
    ],
}

def templates_menu() -> InlineKeyboardMarkup:
    """
    Build an inline menu so the user can pick a quick-add template.
    """
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=name, callback_data=f"template:{name}")]
        for name in TEMPLATES
    ])
    return kb



WEEKDAYS = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}
PERIODS = {"morning": (9, 0), "afternoon": (14, 0), "evening": (19, 0), "night": (22, 0), "tonight": (22, 0)}


def parse_natural_deadline(s: str, tz_name: str = "Asia/Tashkent") -> datetime | None:
    """
    Supports:
      - "21:00" (today if future, else tomorrow)
      - "YYYY-MM-DD HH:MM"  |  "YYYY-MM-DD" (defaults 09:00)
      - "in 2h" | "in 30m" | "in 2d" | "in 1w"
      - "today 19:00" | "tomorrow 09:00"
      - "today evening" | "tomorrow morning" | "tonight"
      - "mon 14:30" | "next monday 14:30" | "fri evening"
    Returns tz-aware datetime or None.
    """
    tz = pytz.timezone(tz_name)
    now = datetime.now(tz)
    txt = s.strip().lower()

    # 1) HH:MM
    m = re.fullmatch(r"(\d{1,2}):(\d{2})", txt)
    if m:
        hh, mm = map(int, m.groups())
        dt = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if dt <= now:
            dt = dt + timedelta(days=1)
        return dt

    # 2) ISO datetime
    try:
        dt = datetime.strptime(txt, "%Y-%m-%d %H:%M")
        return tz.localize(dt)
    except Exception:
        pass
    # 3) ISO date only -> 09:00
    try:
        d = datetime.strptime(txt, "%Y-%m-%d")
        return tz.localize(d.replace(hour=9, minute=0, second=0, microsecond=0))
    except Exception:
        pass

    # 4) "in N units"
    m = re.fullmatch(r"in\s*(\d+)\s*(h|hr|hour|hours|m|min|minute|minutes|d|day|days|w|week|weeks)", txt)
    if m:
        n = int(m.group(1))
        u = m.group(2)
        if u in ("h", "hr", "hour", "hours"):
            delta = timedelta(hours=n)
        elif u in ("m", "min", "minute", "minutes"):
            delta = timedelta(minutes=n)
        elif u in ("d", "day", "days"):
            delta = timedelta(days=n)
        else:
            delta = timedelta(weeks=n)
        return now + delta

    # 5) today/tomorrow with time or period
    m = re.fullmatch(r"(today|tomorrow)(?:\s+((\d{1,2}):(\d{2})|morning|afternoon|evening|night|tonight))?", txt)
    if m:
        base = now if m.group(1) == "today" else (now + timedelta(days=1))
        hh, mm = 9, 0
        if m.group(2):
            if ":" in m.group(2):
                hh, mm = map(int, m.group(2).split(":"))
            else:
                hh, mm = PERIODS[m.group(2)]
        dt = base.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if dt <= now:
            dt = dt + timedelta(days=1)
        return dt

    # 6) "tonight"
    if txt == "tonight":
        dt = now.replace(hour=PERIODS["tonight"][0], minute=0, second=0, microsecond=0)
        if dt <= now:
            dt = dt + timedelta(days=1)
        return dt

    # 7) weekday (with optional "next") + time or period
    m = re.fullmatch(r"(?:next\s+)?(mon|tue|wed|thu|fri|sat|sun|monday|tuesday|wednesday|thursday|friday|saturday|sunday)"
                     r"(?:\s+((\d{1,2}):(\d{2})|morning|afternoon|evening|night))?", txt)
    if m:
        wd = m.group(1)[:3]
        target = WEEKDAYS[wd]
        days_ahead = (target - now.weekday()) % 7
        # if no "next" in text and it's later today with explicit time, allow 0; else move to next week
        if "next" in txt or days_ahead == 0:
            days_ahead = 7 if days_ahead == 0 else days_ahead
        hh, mm = 9, 0
        if m.group(2):
            if ":" in m.group(2):
                hh, mm = map(int, m.group(2).split(":"))
            else:
                hh, mm = PERIODS[m.group(2)]
        dt = (now + timedelta(days=days_ahead)).replace(hour=hh, minute=mm, second=0, microsecond=0)
        return dt

    return None


def deadline_for_scope(scope: str, tz_name: str = "Asia/Tashkent") -> datetime:
    """
    scope: 'today' | 'week' | 'month'
    today -> tonight 22:00 (if already past, tomorrow 22:00)
    week  -> this Sunday 22:00 (if already past, next Sunday 22:00)
    month -> last day of this month 21:00 (if past, next month's last day 22:00)
    """
    tz = pytz.timezone(tz_name)
    now = datetime.now(tz)
    s = (scope or "").lower()

    if s == "today":
        dt = now.replace(hour=22, minute=0, second=0, microsecond=0)
        if dt <= now:
            dt += timedelta(days=1)
        return dt

    if s == "week":
        # Monday..Sunday week; pick Sunday 22:00
        today = now.date()
        monday = today - timedelta(days=today.weekday())
        sunday = monday + timedelta(days=6)
        dt = tz.localize(datetime(sunday.year, sunday.month, sunday.day, 22, 0))
        if dt <= now:
            dt += timedelta(days=7)
        return dt

    if s == "month":
        y, m = now.year, now.month
        last = monthrange(y, m)[1]
        dt = tz.localize(datetime(y, m, last, 22, 0))
        if dt <= now:
            y2, m2 = (y+1, 1) if m == 12 else (y, m+1)
            last2 = monthrange(y2, m2)[1]
            dt = tz.localize(datetime(y2, m2, last2, 22, 0))
        return dt

    return now  # fallback; caller can ignore or format
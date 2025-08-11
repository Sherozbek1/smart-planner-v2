from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Text, UniqueConstraint, Boolean
from sqlalchemy.orm import declarative_base, relationship
from datetime import datetime

Base = declarative_base()

class User(Base):
    __tablename__ = "users"

    id            = Column(String, primary_key=True)      # Telegram user ID
    name          = Column(String)
    username      = Column(String, default="")            # NEW: Telegram username (for founder/applicant display)
    lang          = Column(String, default="en")
    xp            = Column(Integer, default=0)
    streak        = Column(Integer, default=0)
    completed     = Column(Integer, default=0)
    last_active   = Column(String)
    extra_title   = Column(String, default="")
    about_me      = Column(Text, default="")
    goal_of_month = Column(Text, default="")
    xp_date       = Column(String)
    xp_today      = Column(Integer, default=0)
    bonus_msg     = Column(String, default="")

    # one-to-many: tasks owned by user
    tasks = relationship("Task", back_populates="user")
    # one-to-many: clans owned by user
    clans_owned = relationship("Clan", back_populates="owner")
    # many-to-many: study group memberships
    clan_memberships = relationship("ClanMember", back_populates="user")
    # one-to-many: group join applications
    clan_applications = relationship("ClanApplication", back_populates="user")

class Task(Base):
    __tablename__ = "tasks"

    id             = Column(Integer, primary_key=True, autoincrement=True)
    user_id        = Column(String, ForeignKey("users.id"), index=True)
    text           = Column(Text)
    deadline       = Column(String, index=True)
    reminders_sent = Column(Integer, default=0)
    status         = Column(String, default="pending", index=True)
    priority       = Column(String, default="medium")
    tags           = Column(String, default="")
    repeat         = Column(String, default="")
    notes          = Column(Text, default="")
    attachment     = Column(String, default="")
    created_at     = Column(DateTime, default=datetime.utcnow, index=True)

    user = relationship("User", back_populates="tasks")

class Clan(Base):
    __tablename__ = "clans"
    __table_args__ = (UniqueConstraint('name', name='uq_clan_name'),)

    id          = Column(Integer, primary_key=True, autoincrement=True)
    name        = Column(String, nullable=False)
    owner_id    = Column(String, ForeignKey("users.id"), nullable=False)
    created_at  = Column(DateTime, default=datetime.utcnow, index=True)
    description = Column(Text, default="")         # NEW: Description/goals
    requirements= Column(Text, default="")         # NEW: Requirements/expectations
    link        = Column(String, default="")       # NEW: Telegram group link (only for members)
    is_approved = Column(Boolean, default=False)   # NEW: Admin approval
    image_url   = Column(String, default="")       # NEW: Image URL (for future use)

    owner   = relationship("User", back_populates="clans_owned")
    members = relationship("ClanMember", back_populates="clan", cascade="all, delete-orphan")
    applications = relationship("ClanApplication", back_populates="clan", cascade="all, delete-orphan")

class ClanMember(Base):
    __tablename__ = "clan_members"
    __table_args__ = (UniqueConstraint('clan_id', 'user_id', name='uq_clan_user'),)

    id        = Column(Integer, primary_key=True, autoincrement=True)
    clan_id   = Column(Integer, ForeignKey("clans.id"), nullable=False, index=True)
    user_id   = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    joined_at = Column(DateTime, default=datetime.utcnow)

    clan = relationship("Clan", back_populates="members")
    user = relationship("User", back_populates="clan_memberships")

class ClanApplication(Base):
    __tablename__ = "clan_applications"
    __table_args__ = (UniqueConstraint('clan_id', 'user_id', name='uq_clan_application'),)

    id         = Column(Integer, primary_key=True, autoincrement=True)
    clan_id    = Column(Integer, ForeignKey("clans.id"), nullable=False, index=True)
    user_id    = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    applied_at = Column(DateTime, default=datetime.utcnow)
    status     = Column(String, default="pending")   # "pending", "approved", "rejected"
    note       = Column(Text, default="")            # User note or reason for joining

    clan = relationship("Clan", back_populates="applications")
    user = relationship("User", back_populates="clan_applications")

class ClanCreationRequest(Base):
    __tablename__ = "clan_creation_requests"

    id          = Column(Integer, primary_key=True, autoincrement=True)
    user_id     = Column(String, ForeignKey("users.id"), nullable=False)
    username    = Column(String)
    group_name  = Column(String, nullable=False)
    description = Column(Text, default="")
    requirements= Column(Text, default="")
    link        = Column(String, default="")
    status      = Column(String, default="pending")  # "pending", "approved", "rejected"
    created_at  = Column(DateTime, default=datetime.utcnow)

    user = relationship("User")

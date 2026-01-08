from datetime import datetime
from typing import Optional, Any
from uuid import UUID, uuid4

from sqlalchemy import String, Integer, DateTime, ForeignKey, Index, text, JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.dialects.postgresql import JSONB, UUID as PG_UUID

from app.db.session import Base
from app.domain.states import JobStatus, JobEvent

class Tenant(Base):
    __tablename__ = "tenants"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    
    # Scheduler policies
    weight: Mapped[float] = mapped_column(Integer, default=1)
    max_inflight: Mapped[int] = mapped_column(Integer, default=100)
    
    # Tracking
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=text("now()"))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=text("now()"), onupdate=text("now()"))

    jobs: Mapped[list["Job"]] = relationship("Job", back_populates="tenant")

class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[str] = mapped_column(String, ForeignKey("tenants.id"), index=True, nullable=False)
    
    # Core orchestration fields
    status: Mapped[JobStatus] = mapped_column(String, default=JobStatus.PENDING, index=True)
    priority: Mapped[int] = mapped_column(Integer, default=0)
    
    # Scheduling fields
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=text("now()"))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=text("now()"), onupdate=text("now()"))
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=text("now()"), index=True)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    execution_timeout: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    
    # Retry logic
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, default=3)
    last_error: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    
    # Idempotency
    idempotency_key: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    
    # Payload
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, default={})
    result: Mapped[Optional[dict[str, Any]]] = mapped_column(JSONB, nullable=True)

    # Relationships
    tenant: Mapped["Tenant"] = relationship("Tenant", back_populates="jobs")
    lease: Mapped[Optional["JobLease"]] = relationship("JobLease", back_populates="job", uselist=False, cascade="all, delete-orphan")
    events: Mapped[list["JobEventLog"]] = relationship("JobEventLog", back_populates="job", cascade="all, delete-orphan")

    __table_args__ = (
        # Optimization for "poll" query: status=PENDING + available_at <= now
        Index("ix_jobs_poll", "status", "available_at", postgresql_where=text("status = 'pending'")),
        # Uniqueness for idempotency
        Index("ix_jobs_idempotency", "tenant_id", "idempotency_key", unique=True, postgresql_where=text("idempotency_key IS NOT NULL")),
    )

class JobLease(Base):
    __tablename__ = "job_leases"

    job_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), primary_key=True)
    worker_id: Mapped[str] = mapped_column(String, nullable=False)
    lease_token: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), default=uuid4)
    
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    last_heartbeat_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=text("now()"))

    job: Mapped["Job"] = relationship("Job", back_populates="lease")

class JobEventLog(Base):
    __tablename__ = "job_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), index=True)
    
    event_type: Mapped[JobEvent] = mapped_column(String, nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=text("now()"))
    
    # Context (e.g. worker_id, error message, attempt number)
    meta: Mapped[dict[str, Any]] = mapped_column(JSONB, default={})

    job: Mapped["Job"] = relationship("Job", back_populates="events")

class JobCompletion(Base):
    __tablename__ = "job_completions"

    job_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), primary_key=True)
    idempotency_key: Mapped[str] = mapped_column(String, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=text("now()"))

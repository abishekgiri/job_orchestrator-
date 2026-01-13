from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.settings import settings
from app.api.v1.jobs import router as jobs_router
from app.api.v1.workers import router as workers_router
from app.api.v1.admin import router as admin_router
from app.api.v1.metrics import router as metrics_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    from app.scheduler.service import SchedulerService
    from app.services.outbox import OutboxProcessor
    from app.db.session import AsyncSessionLocal
    from app.db.models import Tenant
    from sqlalchemy import select
    from uuid import uuid4
    import logging

    logger = logging.getLogger("uvicorn")

    # 1. Bootstrap Dev Environment (Retry logic for fresh DBs where migrations haven't run)
    import asyncio
    from sqlalchemy.exc import ProgrammingError
    
    bootstrap_success = False
    for i in range(10):
        try:
            async with AsyncSessionLocal() as session:
                stmt = select(Tenant).where(Tenant.id == "local-dev")
                t = (await session.execute(stmt)).scalar_one_or_none()
                
                if not t:
                    new_key = f"dev-key-{str(uuid4())[:8]}"
                    t = Tenant(id="local-dev", name="Local Dev Tenant", api_key=new_key)
                    session.add(t)
                    await session.commit()
                    logger.info(f"\n{'='*40}\nBOOTSTRAP: Created 'local-dev' Tenant.\nAPI KEY: {new_key}\n{'='*40}\n")
                else:
                    if t.api_key:
                        logger.info(f"\n{'='*40}\nBOOTSTRAP: Found 'local-dev' Tenant.\nAPI KEY: {t.api_key}\n{'='*40}\n")
                    else:
                         logger.info(f"BOOTSTRAP: Found 'local-dev' Tenant (No API Key).")
            # If successful, break
            bootstrap_success = True
            break
        except ProgrammingError:
            # Table doesn't exist yet
            logger.warning(f"Bootstrap: Tables not ready, retrying in 2s... ({i+1}/10)")
            await asyncio.sleep(2)
        except Exception as e:
            logger.error(f"Bootstrap failed: {e}")
            # If unexpected error, maybe best to let it crash or try to proceed? 
            # Proceeding might be dangerous if DB is down.
            await asyncio.sleep(2)

    # 2. Start Scheduler (Reaper/Ticker)
    scheduler = SchedulerService()
    await scheduler.start()
    
    # 3. Start Outbox
    outbox = OutboxProcessor()
    await outbox.start()

    yield
    
    # Shutdown
    await scheduler.stop()
    await outbox.stop()

app = FastAPI(
    title=settings.PROJECT_NAME,
    lifespan=lifespan
)

app.include_router(jobs_router, prefix="/api/v1/jobs", tags=["jobs"])
app.include_router(workers_router, prefix="/api/v1/workers", tags=["workers"])
app.include_router(admin_router, prefix="/api/v1/admin", tags=["admin"])
app.include_router(metrics_router, tags=["metrics"])

@app.get("/health")
async def health():
    return {"status": "ok"}

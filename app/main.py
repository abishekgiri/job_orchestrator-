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
    scheduler = SchedulerService()
    await scheduler.start()
    
    yield
    
    # Shutdown
    await scheduler.stop()

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

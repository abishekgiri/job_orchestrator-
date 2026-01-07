from typing import Annotated, AsyncGenerator

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db_session

# Dependency for DB session
DbSession = Annotated[AsyncSession, Depends(get_db_session)]

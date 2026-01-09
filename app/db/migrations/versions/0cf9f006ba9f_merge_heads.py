"""Merge heads

Revision ID: 0cf9f006ba9f
Revises: 88e726fdc66a, b1b2b3b4b5b6
Create Date: 2026-01-08 22:56:44.731632

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0cf9f006ba9f'
down_revision: Union[str, Sequence[str], None] = ('88e726fdc66a', 'b1b2b3b4b5b6')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass

"""add warehouse to sales_doc

Revision ID: 8c1a_add_warehouse_to_sales_doc
Revises: 607eec1e7939
Create Date: 2026-02-02
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

# revision identifiers, used by Alembic.
revision = "8c1a_add_warehouse_to_sales_doc"
down_revision = "607eec1e7939"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    insp = inspect(bind)
    cols = [c["name"] for c in insp.get_columns("sales_doc")]

    if "warehouse" not in cols:
        op.add_column("sales_doc", sa.Column("warehouse", sa.String(length=100), nullable=True))


def downgrade():
    bind = op.get_bind()
    insp = inspect(bind)
    cols = [c["name"] for c in insp.get_columns("sales_doc")]

    if "warehouse" in cols:
        op.drop_column("sales_doc", "warehouse")

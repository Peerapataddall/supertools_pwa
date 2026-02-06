"""merge heads 8c3a and 8c3c

Revision ID: 6caca8aafa18
Revises: 8c3a_add_sales_item_brand, 8c3c_fix_missing_sales_item_columns_pg
Create Date: 2026-02-06 23:35:43.120827

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '6caca8aafa18'
down_revision = ('8c3a_add_sales_item_brand', '8c3c_fix_missing_sales_item_columns_pg')
branch_labels = None
depends_on = None


def upgrade():
    pass


def downgrade():
    pass

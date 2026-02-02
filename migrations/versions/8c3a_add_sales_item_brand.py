"""add brand to sales_item

Revision ID: 8c3a_add_sales_item_brand
Revises: 8c2a_add_customer_credit_terms
Create Date: 2026-02-02
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "8c3a_add_sales_item_brand"
down_revision = "8c2a_add_customer_credit_terms"
branch_labels = None
depends_on = None


def upgrade():
    # เพิ่มคอลัมน์ brand ในตาราง sales_item
    op.add_column("sales_item", sa.Column("brand", sa.String(length=120), nullable=True))


def downgrade():
    op.drop_column("sales_item", "brand")

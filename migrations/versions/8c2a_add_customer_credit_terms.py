"""add credit_term_days/payment_terms to customer

Revision ID: 8c2a_add_customer_credit_terms
Revises: 8c1a_add_warehouse_to_sales_doc
Create Date: 2026-02-02
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = "8c2a_add_customer_credit_terms"
down_revision = "8c1a_add_warehouse_to_sales_doc"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    insp = inspect(bind)
    cols = [c["name"] for c in insp.get_columns("customer")]

    if "credit_term_days" not in cols:
        op.add_column("customer", sa.Column("credit_term_days", sa.Integer(), nullable=True))

    if "payment_terms" not in cols:
        op.add_column("customer", sa.Column("payment_terms", sa.String(length=255), nullable=True))


def downgrade():
    bind = op.get_bind()
    insp = inspect(bind)
    cols = [c["name"] for c in insp.get_columns("customer")]

    if "payment_terms" in cols:
        op.drop_column("customer", "payment_terms")

    if "credit_term_days" in cols:
        op.drop_column("customer", "credit_term_days")

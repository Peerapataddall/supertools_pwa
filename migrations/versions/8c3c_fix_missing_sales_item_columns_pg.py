"""fix missing sales_item columns on postgres (brand/allocated_skus/line_status/source_qu_item_id)

Revision ID: 8c3c_fix_missing_sales_item_columns_pg
Revises: 8c2a_add_customer_credit_terms
Create Date: 2026-02-02
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "8c3c_fix_missing_sales_item_columns_pg"
down_revision = "8c2a_add_customer_credit_terms"
branch_labels = None
depends_on = None


def upgrade():
    # ทำแบบ Postgres-safe: เพิ่มคอลัมน์ถ้ายังไม่มี (กันรันซ้ำ/กันชน)
    op.execute("ALTER TABLE sales_item ADD COLUMN IF NOT EXISTS brand VARCHAR(120);")
    op.execute("ALTER TABLE sales_item ADD COLUMN IF NOT EXISTS allocated_skus TEXT;")
    op.execute("ALTER TABLE sales_item ADD COLUMN IF NOT EXISTS source_qu_item_id INTEGER;")

    # line_status ถ้าระบบนายใช้ String/Enum ให้เป็น varchar และมีค่า default
    op.execute("ALTER TABLE sales_item ADD COLUMN IF NOT EXISTS line_status VARCHAR(12) DEFAULT 'APPROVED' NOT NULL;")

    # index (รันซ้ำแล้วไม่พัง)
    op.execute("CREATE INDEX IF NOT EXISTS ix_sales_item_line_status ON sales_item (line_status);")
    op.execute("CREATE INDEX IF NOT EXISTS ix_sales_item_source_qu_item_id ON sales_item (source_qu_item_id);")


def downgrade():
    # downgrade แบบปลอดภัย (ถอยจริงค่อยใช้)
    op.execute("DROP INDEX IF EXISTS ix_sales_item_source_qu_item_id;")
    op.execute("DROP INDEX IF EXISTS ix_sales_item_line_status;")

    op.execute("ALTER TABLE sales_item DROP COLUMN IF EXISTS line_status;")
    op.execute("ALTER TABLE sales_item DROP COLUMN IF EXISTS source_qu_item_id;")
    op.execute("ALTER TABLE sales_item DROP COLUMN IF EXISTS allocated_skus;")
    op.execute("ALTER TABLE sales_item DROP COLUMN IF EXISTS brand;")

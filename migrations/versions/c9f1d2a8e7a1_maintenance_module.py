"""maintenance module

Revision ID: c9f1d2a8e7a1
Revises: b87b08c2b4dd
Create Date: 2025-11-10 04:25:00.000000
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'c9f1d2a8e7a1'
down_revision = 'b87b08c2b4dd'
branch_labels = None
depends_on = None


def _has_table(conn, name: str) -> bool:
    insp = sa.inspect(conn)
    return insp.has_table(name)


def _has_index(conn, table: str, index_name: str) -> bool:
    insp = sa.inspect(conn)
    try:
        idx = insp.get_indexes(table)
    except Exception:
        return False
    return any(i.get("name") == index_name for i in idx or [])


def upgrade():
    conn = op.get_bind()

    # ---------- spare_parts ----------
    if not _has_table(conn, "spare_parts"):
        op.create_table(
            'spare_parts',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('code', sa.String(length=32), nullable=False),
            sa.Column('name', sa.String(length=255), nullable=False),
            sa.Column('unit', sa.String(length=32), nullable=True),
            sa.Column('unit_cost', sa.Numeric(12, 2), nullable=False),
            sa.Column('stock_qty', sa.Numeric(12, 2), nullable=False),
            sa.Column('notes', sa.String(length=255), nullable=True),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('code'),
        )
    # index แยกบรรทัด เพื่อจะได้เช็คก่อนสร้าง
    if not _has_index(conn, "spare_parts", "ix_spare_parts_code"):
        op.create_index('ix_spare_parts_code', 'spare_parts', ['code'], unique=False)

    # ---------- maintenance_jobs ----------
    if not _has_table(conn, "maintenance_jobs"):
        op.create_table(
            'maintenance_jobs',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('number', sa.String(length=32), nullable=False),
            sa.Column('created_at', sa.DateTime(), nullable=False),
            sa.Column('job_date', sa.DateTime(), nullable=False),
            # หมายเหตุ: SQLite จะเก็บ Enum เป็น TEXT อยู่แล้ว
            sa.Column('status', sa.Enum('NEW', 'IN_PROGRESS', 'DONE', 'CANCELLED', name='maintenance_status_enum'), nullable=False),
            sa.Column('claim_id', sa.Integer(), nullable=True),
            sa.Column('equipment_id', sa.Integer(), nullable=True),
            sa.Column('warehouse_name', sa.String(length=120), nullable=True),
            sa.Column('symptom', sa.Text(), nullable=True),
            sa.Column('summary', sa.Text(), nullable=True),
            sa.Column('parts_total', sa.Numeric(12, 2), nullable=False),
            sa.Column('labor_total', sa.Numeric(12, 2), nullable=False),
            sa.Column('other_total', sa.Numeric(12, 2), nullable=False),
            sa.Column('grand_total', sa.Numeric(12, 2), nullable=False),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('number'),
        )
    if not _has_index(conn, "maintenance_jobs", "ix_maintenance_jobs_number"):
        op.create_index('ix_maintenance_jobs_number', 'maintenance_jobs', ['number'], unique=False)
    if not _has_index(conn, "maintenance_jobs", "ix_maintenance_jobs_created_at"):
        op.create_index('ix_maintenance_jobs_created_at', 'maintenance_jobs', ['created_at'], unique=False)

    # ---------- maintenance_part_usage ----------
    if not _has_table(conn, "maintenance_part_usage"):
        op.create_table(
            'maintenance_part_usage',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('job_id', sa.Integer(), nullable=False),
            sa.Column('part_id', sa.Integer(), nullable=False),
            sa.Column('qty', sa.Numeric(12, 2), nullable=False),
            sa.Column('unit_cost_snapshot', sa.Numeric(12, 2), nullable=False),
            sa.Column('line_total', sa.Numeric(12, 2), nullable=False),
            sa.Column('note', sa.String(length=255), nullable=True),
            sa.ForeignKeyConstraint(['job_id'], ['maintenance_jobs.id']),
            sa.ForeignKeyConstraint(['part_id'], ['spare_parts.id']),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('job_id', 'part_id', 'note', name='uq_maint_part_usage_dedup'),
        )

    # ---------- maintenance_labor_items ----------
    if not _has_table(conn, "maintenance_labor_items"):
        op.create_table(
            'maintenance_labor_items',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('job_id', sa.Integer(), nullable=False),
            sa.Column('description', sa.String(length=255), nullable=False),
            sa.Column('hours', sa.Numeric(10, 2), nullable=False),
            sa.Column('rate', sa.Numeric(12, 2), nullable=False),
            sa.Column('line_total', sa.Numeric(12, 2), nullable=False),
            sa.ForeignKeyConstraint(['job_id'], ['maintenance_jobs.id']),
            sa.PrimaryKeyConstraint('id'),
        )


def downgrade():
    conn = op.get_bind()

    # ลบแบบมีเช็คก่อน (กัน error กรณีมีบางตารางหาย/ไม่ได้สร้าง)
    insp = sa.inspect(conn)

    if insp.has_table("maintenance_labor_items"):
        op.drop_table('maintenance_labor_items')
    if insp.has_table("maintenance_part_usage"):
        op.drop_table('maintenance_part_usage')

    # ดัชนีของ maintenance_jobs
    if _has_index(conn, "maintenance_jobs", "ix_maintenance_jobs_created_at"):
        op.drop_index('ix_maintenance_jobs_created_at', table_name='maintenance_jobs')
    if _has_index(conn, "maintenance_jobs", "ix_maintenance_jobs_number"):
        op.drop_index('ix_maintenance_jobs_number', table_name='maintenance_jobs')
    if insp.has_table("maintenance_jobs"):
        op.drop_table('maintenance_jobs')

    # ดัชนีของ spare_parts
    if _has_index(conn, "spare_parts", "ix_spare_parts_code"):
        op.drop_index('ix_spare_parts_code', table_name='spare_parts')
    if insp.has_table("spare_parts"):
        op.drop_table('spare_parts')

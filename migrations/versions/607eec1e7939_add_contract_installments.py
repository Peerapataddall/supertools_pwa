"""Add contract/installment billing

Revision ID: 607eec1e7939
Revises: f8075cdc74c3
Create Date: 2026-01-06

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '607eec1e7939'
down_revision = 'f8075cdc74c3'
branch_labels = None
depends_on = None


def upgrade():
    # --- sales_doc new columns ---
    with op.batch_alter_table('sales_doc') as batch_op:
        batch_op.add_column(sa.Column('billing_mode', sa.String(length=12), server_default='ONCE'))
        batch_op.add_column(sa.Column('contract_start', sa.Date(), nullable=True))
        batch_op.add_column(sa.Column('contract_end', sa.Date(), nullable=True))
        batch_op.add_column(sa.Column('installment_count', sa.Integer(), server_default='0'))

    # --- sales_installment table ---
    op.create_table(
        'sales_installment',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('contract_id', sa.Integer(), sa.ForeignKey('sales_doc.id'), nullable=False, index=True),
        sa.Column('installment_no', sa.Integer(), nullable=False),
        sa.Column('period_start', sa.Date(), nullable=False),
        sa.Column('period_end', sa.Date(), nullable=False),
        sa.Column('bill_date', sa.Date(), nullable=False),
        sa.Column('due_date', sa.Date(), nullable=False),
        sa.Column('po_customer_sub', sa.String(length=64), server_default=''),
        sa.Column('status', sa.String(length=20), server_default='PLANNED'),
        sa.Column('amount_subtotal', sa.Float(), server_default='0'),
        sa.Column('amount_vat', sa.Float(), server_default='0'),
        sa.Column('amount_total', sa.Float(), server_default='0'),
        sa.Column('amount_wht', sa.Float(), server_default='0'),
        sa.Column('amount_grand', sa.Float(), server_default='0'),
        sa.Column('bill_id', sa.Integer(), sa.ForeignKey('sales_doc.id')),
        sa.Column('invoice_id', sa.Integer(), sa.ForeignKey('sales_doc.id')),
        sa.Column('receipt_id', sa.Integer(), sa.ForeignKey('sales_doc.id')),
        sa.UniqueConstraint('contract_id', 'installment_no', name='uq_sales_installment_contract_no'),
    )


def downgrade():
    op.drop_table('sales_installment')
    with op.batch_alter_table('sales_doc') as batch_op:
        batch_op.drop_column('installment_count')
        batch_op.drop_column('contract_end')
        batch_op.drop_column('contract_start')
        batch_op.drop_column('billing_mode')

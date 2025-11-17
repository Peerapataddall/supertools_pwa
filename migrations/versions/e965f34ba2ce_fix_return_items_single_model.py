"""fix return items single model

Revision ID: e965f34ba2ce
Revises: a261a5385bc4
Create Date: 2025-11-17 03:58:03.497544

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e965f34ba2ce'
down_revision = 'a261a5385bc4'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('return_docs', schema=None) as batch_op:
        batch_op.create_foreign_key(
            'fk_return_docs_customer',
            'customer',
            ['customer_id'],
            ['id'],
        )
        batch_op.create_foreign_key(
            'fk_return_docs_quote',
            'sales_doc',
            ['quote_id'],
            ['id'],
        )


def downgrade():
    with op.batch_alter_table('return_docs', schema=None) as batch_op:
        batch_op.drop_constraint('fk_return_docs_customer', type_='foreignkey')
        batch_op.drop_constraint('fk_return_docs_quote', type_='foreignkey')

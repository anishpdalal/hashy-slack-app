"""Add upvotes field

Revision ID: 771732fe6781
Revises: efaf115c6266
Create Date: 2022-03-01 21:55:43.824561

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '771732fe6781'
down_revision = 'efaf115c6266'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('query', sa.Column('upvotes', sa.Integer(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('query', 'upvotes')
    # ### end Alembic commands ###

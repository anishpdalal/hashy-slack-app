"""add url to document table

Revision ID: bdc40d6b066a
Revises: 6739cb175b28
Create Date: 2021-11-03 03:25:09.809314

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'bdc40d6b066a'
down_revision = '6739cb175b28'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('document',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('team', sa.String(), nullable=False),
    sa.Column('word_positions', sa.Text(), nullable=False),
    sa.Column('url', sa.String(), nullable=False),
    sa.Column('embeddings', sa.PickleType(), nullable=False),
    sa.Column('time_created', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('time_updated', sa.DateTime(timezone=True), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_document_id'), 'document', ['id'], unique=False)
    op.add_column('query', sa.Column('embedding', sa.PickleType(), nullable=False))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('query', 'embedding')
    op.drop_index(op.f('ix_document_id'), table_name='document')
    op.drop_table('document')
    # ### end Alembic commands ###

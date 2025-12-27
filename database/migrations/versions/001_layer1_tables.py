"""Layer 1: Create import_batches and raw_imports tables

Revision ID: 001
Revises:
Create Date: 2025-01-15

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB

# revision identifiers, used by Alembic.
revision = '001'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Enable UUID extension
    op.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')

    # Create import_batches table
    op.create_table(
        'import_batches',
        sa.Column('batch_id', UUID, primary_key=True, server_default=sa.text('gen_random_uuid()')),
        sa.Column('source_name', sa.Text, nullable=False),
        sa.Column('source_type', sa.VARCHAR(20), nullable=False),
        sa.Column('file_format', sa.VARCHAR(10), nullable=False),
        sa.Column('file_size_bytes', sa.BigInteger),
        sa.Column('status', sa.VARCHAR(20), server_default='processing'),
        sa.Column('total_records', sa.Integer),
        sa.Column('processed_records', sa.Integer, server_default='0'),
        sa.Column('new_records', sa.Integer, server_default='0'),
        sa.Column('duplicate_records', sa.Integer, server_default='0'),
        sa.Column('failed_records', sa.Integer, server_default='0'),
        sa.Column('started_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('completed_at', sa.DateTime(timezone=True)),
        sa.Column('error', sa.Text),
        sa.CheckConstraint("source_type IN ('PARCEL', 'RETR', 'DFI')", name='check_source_type'),
        sa.CheckConstraint("file_format IN ('GDB', 'CSV')", name='check_file_format'),
        sa.CheckConstraint("status IN ('processing', 'completed', 'failed')", name='check_status')
    )

    # Create indexes on import_batches
    op.create_index('idx_import_batches_status', 'import_batches', ['status'])
    op.create_index('idx_import_batches_started', 'import_batches', [sa.text('started_at DESC')])

    # Create raw_imports table
    op.create_table(
        'raw_imports',
        sa.Column('record_id', UUID, primary_key=True, server_default=sa.text('gen_random_uuid()')),
        sa.Column('content_hash', sa.VARCHAR(64), nullable=False),
        sa.Column('import_batch_id', UUID, nullable=False),
        sa.Column('source_type', sa.VARCHAR(20), nullable=False),
        sa.Column('source_file', sa.Text, nullable=False),
        sa.Column('source_row_number', sa.Integer),
        sa.Column('imported_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('raw_data', JSONB, nullable=False),
        sa.Column('processing_status', sa.VARCHAR(20), server_default='pending'),
        sa.Column('processed_at', sa.DateTime(timezone=True)),
        sa.Column('processing_error', sa.Text),
        sa.Column('matched_parcel_id', UUID),
        sa.Column('match_confidence', sa.Numeric(5, 4)),
        sa.Column('match_method', sa.VARCHAR(50)),
        sa.ForeignKeyConstraint(['import_batch_id'], ['import_batches.batch_id'], name='fk_raw_imports_batch'),
        sa.CheckConstraint("processing_status IN ('pending', 'processing', 'processed', 'failed', 'skipped')",
                          name='check_processing_status')
    )

    # Create indexes on raw_imports
    op.create_index('idx_raw_imports_hash', 'raw_imports', ['content_hash'], unique=True)
    op.create_index('idx_raw_imports_batch', 'raw_imports', ['import_batch_id'])
    op.create_index('idx_raw_imports_source_type', 'raw_imports', ['source_type'])
    op.create_index('idx_raw_imports_batch_status', 'raw_imports', ['import_batch_id', 'processing_status'])
    op.create_index('idx_raw_imports_status_pending', 'raw_imports', ['processing_status'],
                   postgresql_where=sa.text("processing_status = 'pending'"))

    # Create duplicate_log table (optional, for analytics)
    op.create_table(
        'duplicate_log',
        sa.Column('log_id', UUID, primary_key=True, server_default=sa.text('gen_random_uuid()')),
        sa.Column('batch_id', UUID, nullable=False),
        sa.Column('content_hash', sa.VARCHAR(64), nullable=False),
        sa.Column('existing_record_id', UUID, nullable=False),
        sa.Column('detected_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('CURRENT_TIMESTAMP'))
    )

    # Create indexes on duplicate_log
    op.create_index('idx_duplicate_log_batch', 'duplicate_log', ['batch_id'])
    op.create_index('idx_duplicate_log_hash', 'duplicate_log', ['content_hash'])


def downgrade() -> None:
    # Drop tables in reverse order
    op.drop_table('duplicate_log')
    op.drop_table('raw_imports')
    op.drop_table('import_batches')
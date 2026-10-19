"""generation_tables: bulk_generation_jobs, generations, token_budgets, prompt_templates, outbox.

``prompt_templates`` carries the AST-16 constraints: unique
``(content_type, name, version) NULLS NOT DISTINCT`` and one active row per
``(content_type, name)`` as a partial unique index. ``outbox`` grants the
relay role (PLT-3) through ``create_outbox``.

Revision ID: 0004_generation_tables
Revises: 0003_conversation_tables
Create Date: 2026-10-15
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from smartretail_common.migrations import create_outbox, drop_outbox

revision: str = "0004_generation_tables"
down_revision: str | None = "0003_conversation_tables"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _timestamps() -> list[sa.Column]:  # type: ignore[type-arg]
    return [
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    ]


def upgrade() -> None:
    op.create_table(
        "bulk_generation_jobs",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("merchant_id", sa.UUID(), nullable=False),
        sa.Column("content_type", sa.Text(), nullable=False, comment="DomainEnum ContentType"),
        sa.Column("product_ids", postgresql.ARRAY(sa.UUID()), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, comment="DomainEnum BulkJobStatus"),
        sa.Column("completed", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("failed", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("idempotency_key", sa.Text(), nullable=False),
        sa.Column("request_hash", sa.Text(), nullable=False),
        sa.Column("inputs", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "item_errors",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        *_timestamps(),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_bulk_generation_jobs")),
        sa.UniqueConstraint(
            "merchant_id",
            "idempotency_key",
            name=op.f("uq_bulk_generation_jobs_merchant_id_idempotency_key"),
        ),
    )
    op.create_index("ix_bulk_generation_jobs_merchant_id", "bulk_generation_jobs", ["merchant_id"])

    op.create_table(
        "generations",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("merchant_id", sa.UUID(), nullable=False),
        sa.Column("product_id", sa.UUID(), nullable=False),
        sa.Column("content_type", sa.Text(), nullable=False, comment="DomainEnum ContentType"),
        sa.Column(
            "parameters",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("prompt_tokens", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("completion_tokens", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("cost_usd", sa.Numeric(10, 6), server_default=sa.text("0"), nullable=False),
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column("finish_reason", sa.Text(), nullable=True, comment="DomainEnum FinishReason"),
        sa.Column("applied", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("bulk_job_id", sa.UUID(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["bulk_job_id"],
            ["bulk_generation_jobs.id"],
            name=op.f("fk_generations_bulk_job_id_bulk_generation_jobs"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_generations")),
    )
    op.create_index(
        "ix_generations_merchant_id_created_at", "generations", ["merchant_id", "created_at"]
    )
    op.create_index("ix_generations_product_id", "generations", ["product_id"])
    op.create_index("ix_generations_bulk_job_id", "generations", ["bulk_job_id"])
    op.create_index(
        "uq_generations_bulk_job_id_product_id",
        "generations",
        ["bulk_job_id", "product_id"],
        unique=True,
        postgresql_where=sa.text("bulk_job_id IS NOT NULL"),
    )

    op.create_table(
        "token_budgets",
        sa.Column("scope", sa.Text(), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("tokens_used", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("requests", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("cost_usd", sa.Numeric(10, 6), server_default=sa.text("0"), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("scope", "date", name=op.f("pk_token_budgets")),
    )

    op.create_table(
        "prompt_templates",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("content_type", sa.Text(), nullable=True, comment="DomainEnum ContentType"),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_prompt_templates")),
    )
    op.create_index("ix_prompt_templates_name", "prompt_templates", ["name"])
    op.execute(
        "ALTER TABLE prompt_templates ADD CONSTRAINT uq_prompt_templates_content_type_name_version"
        " UNIQUE NULLS NOT DISTINCT (content_type, name, version)"
    )
    op.execute(
        "CREATE UNIQUE INDEX uq_prompt_templates_active ON prompt_templates (content_type, name)"
        " NULLS NOT DISTINCT WHERE is_active"
    )

    create_outbox(op)


def downgrade() -> None:
    drop_outbox(op)
    op.drop_table("prompt_templates")
    op.drop_table("token_budgets")
    op.drop_table("generations")
    op.drop_table("bulk_generation_jobs")

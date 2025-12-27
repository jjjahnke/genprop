@docs/PROJECT_STRUCTURE.md


## Key Instructions
- **Plan First**: Before implementation, always create a phase-by-phase plan.
- **Service Isolation**: Each service has its own rules, place the a new CLAUDE.md file for each one.
- **Tech Stack**: Python 3.12+, Pydantic v2 (models), Pytest (testing), and Ruff (linting).

## Core Commands
- **Install All**: `pip install -e .` (using a monorepo manager like Hatch or Poetry)
- **Run All Tests**: `pytest`
- **Lint Code**: `ruff check . --fix`
- **Start Chat Dev**: `fastapi dev apps/chat-api/main.py`

## Implementation Guardrails
- **No Mocking unless required**: Prefer integration tests for transformation logic.
- **Type Safety**: Use strict type hints. Every function in `apps/transformer` must have types.
- **CLAUDE.md Updates**: If you change the data schema, update the @services/shared docs immediately.

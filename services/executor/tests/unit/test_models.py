from app.models.execution import Execution


def test_execution_columns():
    cols = set(Execution.__table__.columns.keys())
    assert {
        "id",
        "endpoint",
        "payload",
        "status",
        "created_at",
        "updated_at",
        "logs",
    } <= cols

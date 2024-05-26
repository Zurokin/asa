import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
from datetime import datetime
from main import app, get_db, RollStats
from itertools import count

client = TestClient(app)

# Создаем фикстуру для мокирования CRUDRoll
@pytest.fixture
def mock_crud_roll():
    with patch("main.CRUDRoll") as MockCRUD:
        mock = MockCRUD.return_value
        yield mock

# Фикстура для замены зависимости get_db на замокированный CRUDRoll
@pytest.fixture
def override_get_db(mock_crud_roll):
    def _get_db_override():
        yield mock_crud_roll
    app.dependency_overrides[get_db] = _get_db_override
    yield
    app.dependency_overrides.pop(get_db, None)

def test_create_roll(override_get_db, mock_crud_roll):
    mock_crud_roll.create_roll.return_value = {
        "id": 1,
        "length": 10.0,
        "weight": 20.0,
        "date_added": datetime.now().isoformat(),
        "date_removed": None
    }

    response = client.post("/rolls/", json={"length": 10.0, "weight": 20.0})
    assert response.status_code == 200
    assert response.json()["id"] == 1

def test_delete_roll(override_get_db, mock_crud_roll):
    mock_crud_roll.delete_roll.return_value = {
        "id": 1,
        "length": 10.0,
        "weight": 20.0,
        "date_added": datetime.now(),
        "date_removed": datetime.now()
    }

    response = client.delete("/rolls/1")
    assert response.status_code == 500


id_generator = count(start=1)

def test_get_roll(override_get_db, mock_crud_roll):
    # Устанавливаем side_effect для get_roll
    mock_crud_roll.get_roll.side_effect = lambda roll_id: {
        "id": next(id_generator),
        "length": 10.0,
        "weight": 20.0,
        "date_added": datetime.now(),
        "date_removed": None
    }

    response = client.get("/rolls/1")
    assert response.status_code == 200
    assert response.json()["id"] == 1

    response = client.get("/rolls/2")
    assert response.status_code == 200
    assert response.json()["id"] == 2

def test_get_roll_stats(override_get_db, mock_crud_roll):
    # Создаем мок объект RollStats
    mock_roll_stats = RollStats(
        added_count=10,
        removed_count=5,
        avg_length=15.0,
        avg_weight=25.0,
        min_length=5.0,
        max_length=20.0,
        min_weight=10.0,
        max_weight=30.0,
        total_weight=250.0,
        min_gap=1.0,
        max_gap=10.0
    )
    # Устанавливаем возвращаемое значение для метода get_stats
    mock_crud_roll.get_stats.return_value = mock_roll_stats

    # Остальной код теста
    response = client.get("/rolls/stats/?start_date=2024-01-01T00:00:00&end_date=2024-12-31T23:59:59")
    assert response.status_code == 200
    stats = response.json()
    assert stats["added_count"] == 10

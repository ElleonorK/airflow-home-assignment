import json
import pytest
from flask import Flask, request


app = Flask(__name__)

@pytest.fixture
# @app.route("/", methods=["POST"])
def test_development():
    data = request.json({"conf": {"environment_type": "development"}})

    response = app.test_client().post("/api/v1/dags/", data=json.dumps(data), content_type='application/json')

    assert response.status_code == 200
    # assert response.get_data() == json({"execution_date":"2020-11-11T18:45:05+00:00","message":"Created <DagRun
    #     test_dag @ 2020-11-11 18:45:05+00:00: manual__2020-11-11T18:45:05+00:00,
    #     externally triggered: True>"})
from pathlib import Path

from prefect.deployments import DeploymentSpec
from prefect.flow_runners.subprocess import SubprocessFlowRunner

venv_path = Path(
    r"/home/kevin/coding_projects/noisy_stocks/noisy_stocks_data_orchestrator/.venv"
)

DeploymentSpec(
    flow_name="stock_correlation_flow",
    flow_location="./main_flow.py",
    flow_runner=SubprocessFlowRunner(virtualenv=venv_path),
)

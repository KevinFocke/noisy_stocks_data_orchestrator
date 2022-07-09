from pathlib import Path

from prefect.deployments import DeploymentSpec
from prefect.flow_runners.subprocess import SubprocessFlowRunner
from prefect.orion.schemas.schedules import CronSchedule

venv_path = Path(
    r"/home/kevin/coding_projects/noisy_stocks/noisy_stocks_data_orchestrator/.venv"
)

DeploymentSpec(
    flow_name="correlate_and_publish",
    flow_location="./main_flow.py",
    flow_runner=SubprocessFlowRunner(virtualenv=venv_path),
    schedule=CronSchedule(
        cron=r"0 8 */1 * *", timezone=r"Europe/Brussels", day_or=True
    ),
)

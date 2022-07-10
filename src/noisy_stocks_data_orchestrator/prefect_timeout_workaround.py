"""workaround for Prefect server timeout for long running tasks https://discourse.prefect.io/t/timeout-error-for-long-running-tasks-running-prefect-2-0-in-wsl/639/6"""


import os
import subprocess
from threading import Thread
from time import sleep


def keepalive():
    """ping the orion server to stop connection timeout"""

    def target():
        while True:
            subprocess.Popen("prefect storage ls >/dev/null", shell=True)
            sleep(50)

    Thread(target=target, daemon=True).start()


def keepalive2():
    """ping the orion server to stop connection timeout"""

    os.system("prefect storage ls >/dev/null")

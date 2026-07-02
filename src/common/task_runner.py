import subprocess
from pathlib import Path


def run_python_script(script: str):

    script_path = Path("/opt") / script

    print(f"Running {script_path}")

    subprocess.run(
        ["python", str(script_path)],
        check=True
    )
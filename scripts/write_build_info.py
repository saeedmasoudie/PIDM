from __future__ import annotations

import os
import re
from pathlib import Path


repository = os.environ.get("GITHUB_REPOSITORY", "").strip()
ref_name = os.environ.get("GITHUB_REF_NAME", "").strip()
run_number = os.environ.get("GITHUB_RUN_NUMBER", "0").strip()
version = (
    ref_name.lstrip("v")
    if re.fullmatch(r"v?\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?", ref_name)
    else f"2.0.0-dev.{run_number}"
)

Path("core/build_info.py").write_text(
    f'APP_VERSION = "{version}"\n'
    f'GITHUB_REPOSITORY = "{repository}"\n',
    encoding="utf-8",
)
print(f"Embedded PIDM {version} for {repository or 'unconfigured repository'}")

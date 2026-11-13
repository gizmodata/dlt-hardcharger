"""Settings every test needs before dlt is imported: no telemetry, dlt's files in a scratch dir."""

from __future__ import annotations

import atexit
import os
import shutil
import tempfile

_SCRATCH = tempfile.mkdtemp(prefix="dlt-hardcharger-tests-")
atexit.register(shutil.rmtree, _SCRATCH, ignore_errors=True)

os.environ["RUNTIME__DLTHUB_TELEMETRY"] = "false"
os.environ["DLT_DATA_DIR"] = os.path.join(_SCRATCH, "data")
os.environ["DLT_PROJECT_DIR"] = os.path.join(_SCRATCH, "project")
os.makedirs(os.environ["DLT_PROJECT_DIR"], exist_ok=True)
# Credentials are given by each test: nothing may come from the developer's own environment.
for _name in [name for name in os.environ if name.startswith(("DESTINATION__", "SCHEMA__"))]:
    del os.environ[_name]

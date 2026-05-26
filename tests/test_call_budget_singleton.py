"""The budget store must be a single module object under both import names.

The codebase loads modules both as a package (`src.call_budget`, under pytest
and `python -m src.runner`) and top-level (`call_budget`, via the stages'
sys.path shim that imports moonshot_client bare). If those resolved to two
distinct module objects, the Moonshot chokepoint and the CLI chokepoints would
count into separate maps and reset_all() would clear only one — a budget
split-brain. call_budget self-aliases to prevent this.
"""

import sys
from pathlib import Path


def test_both_import_paths_are_one_module():
    src_dir = Path(__file__).resolve().parent.parent / "src"
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))

    import src.call_budget as via_pkg
    import call_budget as via_top

    assert via_pkg is via_top  # same object, not two split-brain copies

    via_pkg.reset_all()
    via_pkg.try_consume("dev-singleton")
    assert via_top.get_count("dev-singleton") == 1  # shared _counts map
    via_pkg.reset_all()

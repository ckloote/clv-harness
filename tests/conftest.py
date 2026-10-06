# Interim: lets tests import from src/ before pyproject.toml exists.
# Remove once pyproject.toml sets [tool.pytest.ini_options] pythonpath = ["src"].
import pathlib, sys
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

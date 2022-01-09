from tog import __version__
from tog.cli import get_version


def test_version():
    assert __version__ == get_version()


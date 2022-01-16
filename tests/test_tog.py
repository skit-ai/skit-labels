from tog import __version__
from tog import utils


def test_version():
    assert __version__ == utils.get_version()

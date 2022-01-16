from skit_labels import __version__, utils


def test_version():
    assert __version__ == utils.get_version()

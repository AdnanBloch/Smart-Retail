import pytest

from tests.conftest import make_settings


@pytest.fixture
def settings(keys_dir, keypair):
    return make_settings(keys_dir, keypair.kid)

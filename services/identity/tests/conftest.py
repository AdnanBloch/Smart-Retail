import pytest

from app.configs.env import Settings
from smartretail_common.testing import KeyPair, generate_keypair


@pytest.fixture(scope="session")
def keypair() -> KeyPair:
    return generate_keypair("20261003-test0001")


@pytest.fixture(scope="session")
def keys_dir(tmp_path_factory, keypair: KeyPair) -> str:
    directory = tmp_path_factory.mktemp("jwt")
    (directory / f"{keypair.kid}.pem").write_bytes(keypair.private_pem())
    (directory / "jwt_public.pem").write_bytes(keypair.public_pem())
    return str(directory)


def make_settings(
    keys_dir: str, kid: str, database_url: str = "postgresql+asyncpg://x/y"
) -> Settings:
    # Cheap Argon2 parameters keep the suite fast; the parameter test below
    # checks the hasher honours whatever the settings say.
    return Settings(
        service_name="identity",
        database_url=database_url,
        jwt_keys_dir=keys_dir,
        jwt_active_kid=kid,
        argon2_time_cost=1,
        argon2_memory_cost_kib=1024,
        argon2_parallelism=1,
    )

import uuid

import pytest

from smartretail_common.testing import KeyPair, generate_keypair, mint_token


@pytest.fixture(scope="session")
def keypair() -> KeyPair:
    return generate_keypair("notification-test")


class Callers:
    """Bearer headers per caller; each has a fixed subject so rows can be seeded for them."""

    def __init__(self, keypair: KeyPair) -> None:
        self.keypair = keypair
        self.customer_id = str(uuid.uuid4())
        self.other_id = str(uuid.uuid4())
        self.merchant_id = str(uuid.uuid4())
        self.admin_id = str(uuid.uuid4())

    def headers(self, sub: str, role: str = "customer", **kwargs) -> dict[str, str]:
        return {"Authorization": f"Bearer {mint_token(self.keypair, sub=sub, role=role, **kwargs)}"}

    @property
    def customer(self) -> dict[str, str]:
        return self.headers(self.customer_id)

    @property
    def other(self) -> dict[str, str]:
        return self.headers(self.other_id)

    @property
    def merchant(self) -> dict[str, str]:
        return self.headers(self.merchant_id, role="merchant")

    @property
    def admin(self) -> dict[str, str]:
        return self.headers(self.admin_id, role="admin")


@pytest.fixture
def callers(keypair) -> Callers:
    return Callers(keypair)

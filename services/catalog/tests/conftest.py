import uuid

import pytest

from smartretail_common.testing import KeyPair, generate_keypair, mint_token


@pytest.fixture(scope="session")
def keypair() -> KeyPair:
    return generate_keypair("catalog-test")


class Callers:
    """Bearer headers for each role. Merchant ids equal the token subject (I4)."""

    def __init__(self, keypair: KeyPair) -> None:
        self.keypair = keypair
        self.merchant_a_id = str(uuid.uuid4())
        self.merchant_b_id = str(uuid.uuid4())
        self.admin_id = str(uuid.uuid4())
        self.customer_id = str(uuid.uuid4())

    def _h(self, sub: str, role: str | None) -> dict[str, str]:
        return {"Authorization": f"Bearer {mint_token(self.keypair, sub=sub, role=role)}"}

    @property
    def admin(self) -> dict[str, str]:
        return self._h(self.admin_id, "admin")

    @property
    def merchant_a(self) -> dict[str, str]:
        return self._h(self.merchant_a_id, "merchant")

    @property
    def merchant_b(self) -> dict[str, str]:
        return self._h(self.merchant_b_id, "merchant")

    @property
    def customer(self) -> dict[str, str]:
        return self._h(self.customer_id, "customer")

    @property
    def anonymous(self) -> dict[str, str]:
        return {}


@pytest.fixture
def callers(keypair) -> Callers:
    return Callers(keypair)

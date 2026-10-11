import uuid

import pytest

from smartretail_common.testing import KeyPair, generate_keypair, mint_service_token, mint_token


@pytest.fixture(scope="session")
def keypair() -> KeyPair:
    return generate_keypair("inventory-test")


class Callers:
    """Bearer headers per caller. Merchant ids equal the token subject (I4)."""

    def __init__(self, keypair: KeyPair) -> None:
        self.keypair = keypair
        self.merchant_a_id = str(uuid.uuid4())
        self.merchant_b_id = str(uuid.uuid4())

    def _h(self, token: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {token}"}

    @property
    def service(self) -> dict[str, str]:
        return self._h(mint_service_token(self.keypair, scopes=("inventory:internal",)))

    @property
    def wrong_scope(self) -> dict[str, str]:
        return self._h(mint_service_token(self.keypair, scopes=("catalog:internal",)))

    @property
    def admin(self) -> dict[str, str]:
        return self._h(mint_token(self.keypair, role="admin"))

    @property
    def merchant_a(self) -> dict[str, str]:
        return self._h(mint_token(self.keypair, sub=self.merchant_a_id, role="merchant"))

    @property
    def merchant_b(self) -> dict[str, str]:
        return self._h(mint_token(self.keypair, sub=self.merchant_b_id, role="merchant"))

    @property
    def customer(self) -> dict[str, str]:
        return self._h(mint_token(self.keypair, role="customer"))


@pytest.fixture
def callers(keypair) -> Callers:
    return Callers(keypair)

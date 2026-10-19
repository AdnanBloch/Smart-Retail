import uuid

import pytest

from smartretail_common.testing import KeyPair, generate_keypair, mint_service_token, mint_token


@pytest.fixture(scope="session")
def keypair() -> KeyPair:
    return generate_keypair("assistant-test")


class Callers:
    """Bearer headers per caller. Merchant ids equal the token subject (I4)."""

    def __init__(self, keypair: KeyPair) -> None:
        self.keypair = keypair
        self.customer_id = str(uuid.uuid4())
        self.customer_b_id = str(uuid.uuid4())
        self.merchant_a_id = str(uuid.uuid4())
        self.merchant_b_id = str(uuid.uuid4())
        self.admin_id = str(uuid.uuid4())

    def _h(self, token: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {token}"}

    def token(self, sub: str, role: str) -> str:
        return mint_token(self.keypair, sub=sub, role=role)

    @property
    def customer(self) -> dict[str, str]:
        return self._h(self.token(self.customer_id, "customer"))

    @property
    def customer_b(self) -> dict[str, str]:
        return self._h(self.token(self.customer_b_id, "customer"))

    @property
    def merchant_a(self) -> dict[str, str]:
        return self._h(self.token(self.merchant_a_id, "merchant"))

    @property
    def merchant_b(self) -> dict[str, str]:
        return self._h(self.token(self.merchant_b_id, "merchant"))

    @property
    def admin(self) -> dict[str, str]:
        return self._h(self.token(self.admin_id, "admin"))

    @property
    def service(self) -> dict[str, str]:
        return self._h(mint_service_token(self.keypair, scopes=("assistant:internal",)))


@pytest.fixture
def callers(keypair) -> Callers:
    return Callers(keypair)

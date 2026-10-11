import uuid

import pytest

from smartretail_common.testing import KeyPair, generate_keypair, mint_service_token, mint_token


@pytest.fixture(scope="session")
def keypair() -> KeyPair:
    return generate_keypair("order-test")


class Callers:
    """Bearer headers per caller. Merchant ids equal the token subject (I4)."""

    def __init__(self, keypair: KeyPair) -> None:
        self.keypair = keypair
        self.customer_a_id = str(uuid.uuid4())
        self.customer_b_id = str(uuid.uuid4())
        self.merchant_a_id = str(uuid.uuid4())
        self.merchant_b_id = str(uuid.uuid4())
        self.admin_id = str(uuid.uuid4())

    def _h(self, sub: str, role: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {mint_token(self.keypair, sub=sub, role=role)}"}

    @property
    def customer_a(self) -> dict[str, str]:
        return self._h(self.customer_a_id, "customer")

    @property
    def customer_b(self) -> dict[str, str]:
        return self._h(self.customer_b_id, "customer")

    @property
    def merchant_a(self) -> dict[str, str]:
        return self._h(self.merchant_a_id, "merchant")

    @property
    def merchant_b(self) -> dict[str, str]:
        return self._h(self.merchant_b_id, "merchant")

    @property
    def admin(self) -> dict[str, str]:
        return self._h(self.admin_id, "admin")

    @property
    def service(self) -> dict[str, str]:
        token = mint_service_token(self.keypair, scopes=("inventory:internal",))
        return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def callers(keypair) -> Callers:
    return Callers(keypair)

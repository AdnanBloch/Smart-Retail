from fastapi import APIRouter, Depends, status

from app.api.handlers import auth, merchants, users
from app.api.handlers.utils import require_admin
from app.models.auth import TokenPair
from app.models.merchant_profile import MerchantOut
from app.models.user import UserDetailOut, UserOut
from smartretail_common.pagination import Page

# Route: /api/v1/
router = APIRouter(prefix="/api/v1")

# Route: /auth/
auth.router.post("/register", status_code=status.HTTP_201_CREATED, response_model=UserOut)(
    auth.register
)
auth.router.post("/login", response_model=TokenPair)(auth.login)
auth.router.post("/refresh", response_model=TokenPair)(auth.refresh)
auth.router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)(auth.logout)
auth.router.get("/jwks", responses={304: {"description": "Not modified"}})(auth.jwks)

router.include_router(auth.router, prefix="/auth", tags=["auth"])

# Route: /users/
users.router.get("/me", response_model=UserOut)(users.get_me)
users.router.patch("/me", response_model=UserOut)(users.update_me)
users.router.post("/me/password", status_code=status.HTTP_204_NO_CONTENT)(users.change_password)
users.router.get("", response_model=Page[UserOut], dependencies=[Depends(require_admin)])(
    users.list_users
)
users.router.get("/{user_id}", response_model=UserDetailOut, dependencies=[Depends(require_admin)])(
    users.get_user
)
users.router.patch(
    "/{user_id}", response_model=UserDetailOut, dependencies=[Depends(require_admin)]
)(users.update_user)

router.include_router(users.router, prefix="/users", tags=["users"])

# Route: /merchants/
merchants.router.get("", response_model=Page[MerchantOut], dependencies=[Depends(require_admin)])(
    merchants.list_merchants
)
# Registered before /{merchant_id} so "me" is not parsed as an id.
merchants.router.get("/me", response_model=MerchantOut)(merchants.get_my_profile)
merchants.router.patch("/me", response_model=MerchantOut)(merchants.update_my_profile)
merchants.router.get("/{merchant_id}", response_model=MerchantOut)(merchants.get_merchant)

router.include_router(merchants.router, prefix="/merchants", tags=["merchants"])

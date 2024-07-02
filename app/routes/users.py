import logging
import sys

import jwt
from fastapi import APIRouter, Depends, HTTPException
import os

logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)

router = APIRouter()
mode = os.environ.get("MODE")

# oauth2_scheme = "Bearer"
#
#
# def verify_token(token: str = Depends(oauth2_scheme)):
#     try:
#         secret_key = os.getenv("SECRET_AUTK_KEY")
#     except Exception as e:
#         print("Error retrieving auth secret key from os")
#         logger.error("Invalid auth key from OS env")
#         raise HTTPException(500, "Improper env setup - contact administrator")
#     try:
#         jwt.decode(token, secret_key, algorithms=['HS256'])
#         return True
#     except jwt.PyJWTError as e:
#         logger.error(f"PyJWT error: {e}")
#         raise HTTPException(status_code=401, detail="Invalid token")

#
# @router.get("/secure_data")
# async def get_secured_data(valid_token: bool = Depends(verify_token)):
#     return {"message": "Authorized access to secure data"}

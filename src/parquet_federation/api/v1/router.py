from __future__ import annotations

from fastapi import APIRouter

from parquet_federation.api.v1.federation import router as federation_router

router = APIRouter()
router.include_router(federation_router)

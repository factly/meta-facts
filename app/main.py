from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.api_v1.routers.meta_data import meta_data_router
from app.core.config import Settings

settings = Settings()

app = FastAPI(
    title=settings.PROJECT_NAME, docs_url=settings.DOCS_URL, debug=True
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_methods=settings.CORS_METHODS,
    allow_credentials=settings.CORS_ALLOWED_CREDENTIALS,
    allow_headers=settings.CORS_HEADERS,
)


@app.get("/")
async def root():
    return {"message": "Server is up"}


app.include_router(
    meta_data_router.router,
    tags=["Meta-Data"],
    prefix="/meta-data",
)

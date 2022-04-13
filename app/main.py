from fastapi import FastAPI

from app.api.api_v1.routers.meta_data import meta_data_router
from app.core.config import Settings

settings = Settings()

app = FastAPI(title="Finding Meta", docs_url="/api/docs")


@app.get("/")
async def root():
    return {"message": "Server is up"}


app.include_router(
    meta_data_router,
    tags=["Meta-Data"],
    prefix="/meta-data",
)

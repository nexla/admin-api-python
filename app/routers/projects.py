from fastapi import APIRouter

router = APIRouter()

# Placeholder for projects endpoints
# TODO: Implement projects CRUD operations
@router.get("/")
async def list_projects():
    return {"message": "Projects endpoint - to be implemented"}
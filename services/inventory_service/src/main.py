from fastapi import FastAPI
from src.events.producer import start_producer, stop_producer
from src.api import inventory

app = FastAPI(
    title="Inventory Management",
    description="API for operational inventory and analytics",
    version="1.0.0",
    docs_url="/docs",        
    redoc_url="/redoc",   
    openapi_url="/openapi.json"
)


app.include_router(inventory.router)
# app.include_router(analytics.router)

# Optional: Root endpoint
@app.get("/")
async def root():
    return {"message": "Inventory Management API is running!"}

# Trigger reload
@app.on_event("startup")
async def startup_event():
    from src.repositories.scylla_repo import init_scylla
    print("Initializing ScyllaDB...")
    init_scylla()
    print("ScyllaDB Initialized.")
    
    print("Starting Kafka Producer...")
    await start_producer()
    print("Kafka Producer Started.")

@app.on_event("shutdown")
async def shutdown_event():
    print("Stopping Kafka Producer...")
    await stop_producer()
    print("Kafka Producer Stopped.")

# !/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

echo "Starting Inventory Management project folder setup..."
echo "Creating project directories..."

mkdir -p scripts
mkdir -p services/inventory_service/src/{api,core,domain,repositories,services,events}
mkdir -p services/inventory_service/tests
mkdir -p frontend/src/{api,providers/inventory-provider,ws,components,pages}
mkdir -p infrastructure/docker

echo "Creating placeholder files..."

# Backend
touch services/inventory_service/src/app.py
touch services/inventory_service/src/api/{inventory.py,analytics.py}
touch services/inventory_service/src/core/{inventory_service.py,analytics_service.py}
touch services/inventory_service/src/domain/{inventory.py,movement.py}
touch services/inventory_service/src/repositories/{scylla_repo.py,postgres_repo.py}
touch services/inventory_service/src/services/{ws_service.py,kafka_service.py}
touch services/inventory_service/src/events/{producer.py,consumer.py,handlers.py}
touch services/inventory_service/Dockerfile

# Frontend
touch frontend/src/api/{inventoryApi.ts,analyticsApi.ts}
touch frontend/src/providers/inventory-provider/{context.ts,actions.ts,reducer.ts,thunks.ts,InventoryProvider.tsx}
touch frontend/src/ws/inventorySocket.ts
touch frontend/src/components/{InventoryList.tsx,Dashboard.tsx,StockChart.tsx}
touch frontend/src/pages/{DashboardPage.tsx,InventoryPage.tsx}
touch frontend/package.json
touch frontend/tailwind.config.js
touch frontend/tsconfig.json

# Infrastructure
touch infrastructure/docker/docker-compose.yml

# Root files
touch README.md

echo "Project folder structure created successfully!"
echo "Next step: Add code to your backend and frontend files."




class WebSocketManager:
    def __init__(self):
        self.connections = set()

    async def connect(self, ws):
        await ws.accept()
        self.connections.add(ws)

    async def broadcast(self, msg):
        for ws in self.connections:
            await ws.send_json(msg)

ws_manager = WebSocketManager()

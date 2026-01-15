
export const connectInventorySocket = (dispatch: any) => {
    const ws = new WebSocket("ws://localhost:5000/ws");
    ws.onmessage = e => dispatch({ type: "APPLY_LIVE_UPDATE", payload: JSON.parse(e.data) });
    return ws;
};

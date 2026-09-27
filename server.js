const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;
const wss = new WebSocket.Server({ port: PORT });

const rooms = new Map();

function getRoomId(code, sessionId) {
    return `${code}_${sessionId}`;
}

function getOrCreateRoom(roomId) {
    if (!rooms.has(roomId)) {
        rooms.set(roomId, {
            controlWs: null,
            remoteWs: null,
            offer: null,
            answer: null,
            controlCandidates: [],
            remoteCandidates: []
        });
    }
    return rooms.get(roomId);
}

function safeSend(ws, data) {
    if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(data));
    }
}

wss.on('connection', (ws) => {
    let clientRoomId = null;
    let clientRole = null;

    console.log('🔌 New client connected');

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message.toString());
            const { type, code, sessionId, role } = data;
            const roomId = getRoomId(code, sessionId);

            switch (type) {

                case 'join': {
                    clientRoomId = roomId;
                    clientRole = role;
                    const room = getOrCreateRoom(roomId);

                    if (role === 'control') {
                        // ================================================================
                        // FIX: ControlApp reconnect — reset room state for fresh negotiation
                        // ================================================================
                        if (room.controlWs && room.controlWs !== ws) {
                            console.log(`🔄 ControlApp reconnected for room: ${roomId} — resetting room state`);
                            room.offer = null;
                            room.answer = null;
                            room.controlCandidates = [];
                            room.remoteCandidates = [];

                            // Notify RemoteApp to reset WebRTC state
                            safeSend(room.remoteWs, { type: 'peer-reconnected', roomId });
                        }

                        room.controlWs = ws;
                        console.log(`📱 ControlApp joined room: ${roomId}`);

                        // Send cached remote ICE candidates if any
                        if (room.remoteCandidates.length > 0) {
                            console.log(`📦 Flushing ${room.remoteCandidates.length} cached remote candidates to ControlApp`);
                            room.remoteCandidates.forEach((cand) => safeSend(ws, cand));
                        }

                        if (room.answer) {
                            safeSend(ws, room.answer);
                        }

                    } else if (role === 'remote') {
                        room.remoteWs = ws;
                        console.log(`💻 RemoteApp joined room: ${roomId}`);

                        if (room.offer) {
                            console.log(`📦 Sending cached offer to RemoteApp for room: ${roomId}`);
                            safeSend(ws, room.offer);
                        }

                        if (room.controlCandidates.length > 0) {
                            console.log(`📦 Flushing ${room.controlCandidates.length} cached control candidates to RemoteApp`);
                            room.controlCandidates.forEach((cand) => safeSend(ws, cand));
                        }
                    }

                    safeSend(ws, { type: 'joined', role, roomId });
                    break;
                }

                case 'offer': {
                    const room = getOrCreateRoom(roomId);
                    room.offer = data;
                    // Reset answer and candidates for fresh negotiation
                    room.answer = null;
                    room.remoteCandidates = [];
                    console.log(`📤 Offer received from ControlApp for room: ${roomId}`);

                    if (room.remoteWs && room.remoteWs.readyState === WebSocket.OPEN) {
                        safeSend(room.remoteWs, data);
                        console.log(`➡️ Offer forwarded directly to RemoteApp`);
                    } else {
                        console.log(`⏳ RemoteApp not connected yet; offer cached.`);
                    }
                    break;
                }

                case 'answer': {
                    const room = getOrCreateRoom(roomId);
                    room.answer = data;
                    console.log(`📥 Answer received from RemoteApp for room: ${roomId}`);

                    if (room.controlWs && room.controlWs.readyState === WebSocket.OPEN) {
                        safeSend(room.controlWs, data);
                        console.log(`➡️ Answer forwarded directly to ControlApp`);
                    } else {
                        console.log(`⏳ ControlApp not connected yet; answer cached.`);
                    }
                    break;
                }

                case 'ice-candidate': {
                    const room = getOrCreateRoom(roomId);

                    if (role === 'control') {
                        if (room.remoteWs && room.remoteWs.readyState === WebSocket.OPEN) {
                            safeSend(room.remoteWs, data);
                            console.log(`🧊 Control ICE → RemoteApp`);
                        } else {
                            room.controlCandidates.push(data);
                            console.log(`🧊 Control ICE cached (${room.controlCandidates.length})`);
                        }
                    } else if (role === 'remote') {
                        if (room.controlWs && room.controlWs.readyState === WebSocket.OPEN) {
                            safeSend(room.controlWs, data);
                            console.log(`🧊 Remote ICE → ControlApp`);
                        } else {
                            room.remoteCandidates.push(data);
                            console.log(`🧊 Remote ICE cached (${room.remoteCandidates.length})`);
                        }
                    }
                    break;
                }

                case 'leave': {
                    console.log(`🚪 Client leave for room: ${roomId}`);
                    if (rooms.has(roomId)) {
                        const room = rooms.get(roomId);
                        const otherWs = role === 'control' ? room.remoteWs : room.controlWs;
                        safeSend(otherWs, { type: 'peer-left', roomId });
                        rooms.delete(roomId);
                    }
                    break;
                }

                default:
                    console.log(`⚠️ Unknown message type: ${type}`);
            }

        } catch (err) {
            console.error('❌ Error handling message:', err.message);
        }
    });

    ws.on('close', () => {
        console.log(`🔌 Client disconnected (${clientRole || 'unknown'})`);
        if (clientRoomId && rooms.has(clientRoomId)) {
            const room = rooms.get(clientRoomId);
            if (clientRole === 'control' && room.controlWs === ws) {
                room.controlWs = null;
            } else if (clientRole === 'remote' && room.remoteWs === ws) {
                room.remoteWs = null;
            }
            if (!room.controlWs && !room.remoteWs) {
                rooms.delete(clientRoomId);
                console.log(`🧹 Room ${clientRoomId} cleaned up`);
            }
        }
    });

    ws.on('error', (err) => {
        console.error('❌ WebSocket error:', err.message);
    });
});

console.log(`🚀 WebRTC Signaling Server on port ${PORT}`);
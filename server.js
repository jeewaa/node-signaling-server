const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;
const wss = new WebSocket.Server({ port: PORT });

// Active rooms map: roomId -> { controlWs, remoteWs, offer, answer, controlCandidates: [], remoteCandidates: [] }
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

  console.log('🔌 New client connected via WebSocket');

  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message.toString());
      const { type, code, sessionId, role } = data;
      const roomId = getRoomId(code, sessionId);

      switch (type) {
        // ----------------------------------------------------
        // 1. JOIN ROOM
        // ----------------------------------------------------
        case 'join': {
          clientRoomId = roomId;
          clientRole = role; // 'control' or 'remote'
          const room = getOrCreateRoom(roomId);

          if (role === 'control') {
            room.controlWs = ws;
            console.log(`📱 ControlApp joined room: ${roomId}`);

            // If remote has already joined and sent ICE candidates, send them to control
            if (room.remoteCandidates.length > 0) {
              console.log(`📦 Flushing ${room.remoteCandidates.length} cached remote candidates to ControlApp`);
              room.remoteCandidates.forEach((cand) => safeSend(ws, cand));
            }

            // If remote has already sent answer, forward it
            if (room.answer) {
              safeSend(ws, room.answer);
            }

          } else if (role === 'remote') {
            room.remoteWs = ws;
            console.log(`💻 RemoteApp joined room: ${roomId}`);

            // If control already sent offer, forward it immediately to remote!
            if (room.offer) {
              console.log(`📦 Sending cached offer to RemoteApp for room: ${roomId}`);
              safeSend(ws, room.offer);
            }

            // If control already sent candidates, flush them to remote
            if (room.controlCandidates.length > 0) {
              console.log(`📦 Flushing ${room.controlCandidates.length} cached control candidates to RemoteApp`);
              room.controlCandidates.forEach((cand) => safeSend(ws, cand));
            }
          }

          safeSend(ws, { type: 'joined', role, roomId });
          break;
        }

        // ----------------------------------------------------
        // 2. OFFER (ControlApp -> RemoteApp)
        // ----------------------------------------------------
        case 'offer': {
          const room = getOrCreateRoom(roomId);
          room.offer = data;
          console.log(`📤 Offer received from ControlApp for room: ${roomId}`);

          if (room.remoteWs) {
            safeSend(room.remoteWs, data);
            console.log(`➡️ Offer forwarded directly to RemoteApp`);
          } else {
            console.log(`⏳ RemoteApp not connected yet; offer cached.`);
          }
          break;
        }

        // ----------------------------------------------------
        // 3. ANSWER (RemoteApp -> ControlApp)
        // ----------------------------------------------------
        case 'answer': {
          const room = getOrCreateRoom(roomId);
          room.answer = data;
          console.log(`📥 Answer received from RemoteApp for room: ${roomId}`);

          if (room.controlWs) {
            safeSend(room.controlWs, data);
            console.log(`➡️ Answer forwarded directly to ControlApp`);
          } else {
            console.log(`⏳ ControlApp not connected yet; answer cached.`);
          }
          break;
        }

        // ----------------------------------------------------
        // 4. ICE CANDIDATE EXCHANGE
        // ----------------------------------------------------
        case 'ice-candidate': {
          const room = getOrCreateRoom(roomId);

          if (role === 'control') {
            if (room.remoteWs && room.remoteWs.readyState === WebSocket.OPEN) {
              safeSend(room.remoteWs, data);
            } else {
              room.controlCandidates.push(data);
            }
          } else if (role === 'remote') {
            if (room.controlWs && room.controlWs.readyState === WebSocket.OPEN) {
              safeSend(room.controlWs, data);
            } else {
              room.remoteCandidates.push(data);
            }
          }
          break;
        }

        // ----------------------------------------------------
        // 5. LEAVE / REMOVE SESSION
        // ----------------------------------------------------
        case 'leave': {
          console.log(`🚪 Client requested leave for room: ${roomId}`);
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
      // If both sockets closed, clean up room
      if (!room.controlWs && !room.remoteWs) {
        rooms.delete(clientRoomId);
        console.log(`🧹 Room ${clientRoomId} completely cleaned up`);
      }
    }
  });

  ws.on('error', (err) => {
    console.error('❌ WebSocket error:', err.message);
  });
});

console.log(`🚀 WebRTC Node.js Signaling Server listening on port ${PORT}`);
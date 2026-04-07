const WebSocket = require('ws');
const http = require('http');
const server = http.createServer((req, res) => {
    res.writeHead(200);
    res.end('On My Mark relay server running!');
});
const wss = new WebSocket.Server({ server });
const parties = new Map();
const clientInfo = new Map();
wss.on('connection', (ws) => {
    console.log('New connection');
    ws.on('message', (data) => {
        try {
            const msg = JSON.parse(data.toString());
            if (msg.type === 'join') {
                const { partyCode, playerName } = msg;
                clientInfo.set(ws, { partyCode, playerName });
                if (!parties.has(partyCode)) {
                    parties.set(partyCode, new Set());
                }
                parties.get(partyCode).add(ws);
                console.log(`${playerName} joined party ${partyCode}`);
            } else if (msg.type === 'position') {
                const info = clientInfo.get(ws);
                if (!info) return;
                const { partyCode, playerName } = info;
                const party = parties.get(partyCode);
                if (!party) return;
                const payload = JSON.stringify({
                    type: 'position',
                    playerName,
                    x: msg.x,
                    y: msg.y,
                    z: msg.z,
                    dim: msg.dim || 'O'
                });
                for (const member of party) {
                    if (member !== ws && member.readyState === WebSocket.OPEN) {
                        member.send(payload);
                    }
                }
            }
        } catch (e) {
            console.error('Error handling message:', e);
        }
    });
    ws.on('close', () => {
        const info = clientInfo.get(ws);
        if (info) {
            const party = parties.get(info.partyCode);
            if (party) {
                party.delete(ws);
                if (party.size === 0) {
                    parties.delete(info.partyCode);
                }
            }
            clientInfo.delete(ws);
            console.log(`${info.playerName} disconnected`);
        }
    });
});
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Relay server listening on port ${PORT}`);
});

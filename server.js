/**
 * Minimal party relay for the Fabric client.
 *
 * - First message from each socket must be: {"type":"join","partyCode":"...","playerName":"..."}
 * - Then clients may send:
 *   - {"type":"position","x":...,"y":...,"z":...,"dim":"O"|"N"|"E"}
 *   - {"type":"chat","playerName":"...","text":"..."}
 *
 * The server adds playerName to position broadcasts (the mod expects it).
 * Chat messages are forwarded to everyone else in the same partyCode.
 */
const http = require("http");
const WebSocket = require("ws");

/** @type {Map<import('ws'), { partyCode: string, playerName: string }>} */
const clients = new Map();

const server = http.createServer((_req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("omm-relay ok\n");
});

const wss = new WebSocket.Server({ server });

wss.on("connection", (ws) => {
  ws.on("message", (raw) => {
    let data;
    try {
      data = JSON.parse(raw.toString());
    } catch {
      return;
    }
    const type = data && data.type;

    if (type === "join") {
      const partyCode = String(data.partyCode ?? "").trim();
      const playerName = String(data.playerName ?? "").trim();
      if (!partyCode || !playerName) return;
      clients.set(ws, { partyCode, playerName });
      return;
    }

    const info = clients.get(ws);
    if (!info) return;

    if (type === "position") {
      const out = JSON.stringify({
        type: "position",
        playerName: info.playerName,
        x: data.x,
        y: data.y,
        z: data.z,
        dim: data.dim != null ? String(data.dim) : "O",
      });
      broadcastToParty(info.partyCode, ws, out);
      return;
    }

    if (type === "chat") {
      const out = JSON.stringify({
        type: "chat",
        playerName: String(data.playerName ?? info.playerName),
        text: String(data.text ?? ""),
      });
      broadcastToParty(info.partyCode, ws, out);
      return;
    }
  });

  ws.on("close", () => {
    clients.delete(ws);
  });

  ws.on("error", () => {
    clients.delete(ws);
  });
});

/**
 * Send to every other connected client in the same party (not the sender).
 */
function broadcastToParty(partyCode, senderWs, message) {
  for (const [otherWs, info] of clients) {
    if (otherWs === senderWs) continue;
    if (info.partyCode !== partyCode) continue;
    if (otherWs.readyState === WebSocket.OPEN) {
      otherWs.send(message);
    }
  }
}

const port = Number(process.env.PORT) || 8080;
server.listen(port, () => {
  console.log(`omm-relay listening on ${port}`);
});

import express from "express";
import cors from "cors";
import { createServer } from "node:http";
import { randomUUID } from "node:crypto";
import { WebSocketServer } from "ws";

const PORT = Number(process.env.PORT || 10000);

const app = express();
app.use(cors());
app.use(express.json({ limit: "256kb" }));

const reports = new Map();
const votes = new Map();
const presence = new Map();
const channels = new Map();

const REPORT_TYPES = new Set([
  "accident",
  "police",
  "speed_camera",
  "roadworks",
  "traffic_jam",
  "vehicle",
  "damaged_road",
  "animal",
  "weather",
  "other"
]);

function clean(value, max = 120) {
  return typeof value === "string"
    ? value.trim().slice(0, max)
    : "";
}

function number(value) {
  return typeof value === "number" && Number.isFinite(value)
    ? value
    : null;
}

function distanceMeters(lat1, lon1, lat2, lon2) {
  const R = 6371000;
  const rad = Math.PI / 180;

  const p1 = lat1 * rad;
  const p2 = lat2 * rad;

  const dLat = (lat2 - lat1) * rad;
  const dLon = (lon2 - lon1) * rad;

  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(p1) *
      Math.cos(p2) *
      Math.sin(dLon / 2) ** 2;

  return 2 * R * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function nearbyReports(lat, lon, radius) {
  const now = Date.now();

  return [...reports.values()]
    .filter(report => {
      return (
        report.active &&
        now - report.createdAt < 24 * 60 * 60 * 1000
      );
    })
    .map(report => ({
      ...report,
      distance: Math.round(
        distanceMeters(
          lat,
          lon,
          report.lat,
          report.lon
        )
      )
    }))
    .filter(report => report.distance <= radius)
    .sort((a, b) => a.distance - b.distance);
}

function broadcast(message) {
  const data = JSON.stringify(message);

  for (const room of channels.values()) {
    for (const socket of room.clients) {
      if (socket.readyState === 1) {
        socket.send(data);
      }
    }
  }
}

/*
--------------------------------
HEALTH
--------------------------------
*/

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    service: "aurix-backend",
    time: new Date().toISOString(),
    reports: reports.size,
    onlineDrivers: [...presence.values()]
      .filter(x => x.online).length
  });
});

/*
--------------------------------
REPORTS
--------------------------------
*/

app.post("/reports", (req, res) => {
  const {
    type,
    lat,
    lon,
    accuracy,
    deviceId
  } = req.body || {};

  if (!REPORT_TYPES.has(type)) {
    return res.status(400).json({
      error: "Invalid report type"
    });
  }

  if (number(lat) === null || number(lon) === null) {
    return res.status(400).json({
      error: "Valid latitude and longitude are required"
    });
  }

  const now = Date.now();

  const report = {
    id: randomUUID(),
    type,
    lat,
    lon,
    accuracy: number(accuracy),
    deviceId: clean(deviceId),
    createdAt: now,
    reportedAt: new Date(now).toISOString(),
    active: true,
    confirms: 0,
    rejects: 0
  };

  reports.set(report.id, report);

  broadcast({
    type: "report_created",
    report
  });

  res.status(201).json(report);
});

/*
--------------------------------
GET NEARBY REPORTS
--------------------------------
*/

app.get("/reports/nearby", (req, res) => {
  const lat = Number(req.query.lat);
  const lon = Number(req.query.lon);

  let radius = Number(req.query.radius || 5000);

  if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
    return res.status(400).json({
      error: "lat and lon are required"
    });
  }

  radius = Math.min(
    Math.max(radius, 100),
    50000
  );

  res.json({
    reports: nearbyReports(
      lat,
      lon,
      radius
    )
  });
});

/*
--------------------------------
CONFIRM / REJECT
--------------------------------
*/

function vote(req, res, confirm) {
  const reportId = clean(req.params.id);
  const deviceId = clean(req.body?.deviceId);

  const report = reports.get(reportId);

  if (!report || !report.active) {
    return res.status(404).json({
      error: "Report not found"
    });
  }

  if (!deviceId) {
    return res.status(400).json({
      error: "deviceId is required"
    });
  }

  const voteKey =
    `${reportId}:${deviceId}`;

  if (votes.has(voteKey)) {
    return res.status(409).json({
      error: "This device already voted"
    });
  }

  votes.set(
    voteKey,
    confirm ? "confirm" : "reject"
  );

  if (confirm) {
    report.confirms++;
  } else {
    report.rejects++;
  }

  /*
   * Event disappears after
   * 5 distinct rejects.
   */
  if (report.rejects >= 5) {
    report.active = false;
  }

  broadcast({
    type: "report_updated",
    report
  });

  res.json(report);
}

app.post(
  "/reports/:id/confirm",
  (req, res) => vote(req, res, true)
);

app.post(
  "/reports/:id/reject",
  (req, res) => vote(req, res, false)
);

/*
--------------------------------
CB PRESENCE
--------------------------------
*/

app.post("/presence", (req, res) => {
  const deviceId = clean(
    req.body?.deviceId
  );

  if (!deviceId) {
    return res.status(400).json({
      error: "deviceId is required"
    });
  }

  const item = {
    deviceId,
    online: Boolean(req.body?.online),
    channel: clean(req.body?.channel, 60),
    lat: number(req.body?.lat),
    lon: number(req.body?.lon),
    updatedAt: Date.now()
  };

  presence.set(deviceId, item);

  broadcast({
    type: "cb_presence",
    channel: item.channel,
    online: countOnline(item.channel)
  });

  res.json(item);
});

function countOnline(channel) {
  return [...presence.values()]
    .filter(
      x =>
        x.online &&
        (!channel || x.channel === channel)
    ).length;
}

app.get("/presence", (req, res) => {
  const channel = clean(
    req.query.channel,
    60
  );

  const drivers = [...presence.values()]
    .filter(
      x =>
        x.online &&
        (!channel || x.channel === channel)
    );

  res.json({
    online: drivers.length,
    drivers
  });
});

/*
--------------------------------
WEBSOCKET / REALTIME
--------------------------------
*/

const server = createServer(app);

const wss = new WebSocketServer({
  server,
  path: "/ws"
});

function getChannel(name) {
  if (!channels.has(name)) {
    channels.set(name, {
      clients: new Set()
    });
  }

  return channels.get(name);
}

wss.on("connection", (socket, request) => {
  const url = new URL(
    request.url,
    "http://localhost"
  );

  const channel =
    clean(
      url.searchParams.get("channel"),
      60
    ) || "general";

  const deviceId =
    clean(
      url.searchParams.get("deviceId")
    ) || randomUUID();

  const room = getChannel(channel);

  room.clients.add(socket);

  socket.send(
    JSON.stringify({
      type: "connected",
      channel,
      deviceId,
      online: countOnline(channel)
    })
  );

  broadcast({
    type: "cb_presence",
    channel,
    online: countOnline(channel)
  });

  socket.on("message", raw => {
    try {
      const message =
        JSON.parse(raw.toString());

      /*
       * CB ONLINE / OFFLINE
       */
      if (message.type === "presence") {
        presence.set(deviceId, {
          deviceId,
          online: Boolean(message.online),
          channel,
          lat: number(message.lat),
          lon: number(message.lon),
          updatedAt: Date.now()
        });

        broadcast({
          type: "cb_presence",
          channel,
          online: countOnline(channel)
        });
      }

      /*
       * WebRTC signaling
       */
      if (
        message.type === "offer" ||
        message.type === "answer" ||
        message.type === "ice"
      ) {
        const packet =
          JSON.stringify({
            type: message.type,
            from: deviceId,
            to: clean(message.to),
            data: message.data
          });

        for (const peer of room.clients) {
          if (
            peer !== socket &&
            peer.readyState === 1
          ) {
            peer.send(packet);
          }
        }
      }
    } catch {
      // Ignore malformed WebSocket messages.
    }
  });

  socket.on("close", () => {
    room.clients.delete(socket);

    const driver =
      presence.get(deviceId);

    if (driver) {
      driver.online = false;
      driver.updatedAt = Date.now();
      presence.set(
        deviceId,
        driver
      );
    }

    broadcast({
      type: "cb_presence",
      channel,
      online: countOnline(channel)
    });
  });
});

/*
--------------------------------
START
--------------------------------
*/

server.listen(PORT, () => {
  console.log(
    `AURIX backend listening on port ${PORT}`
  );
});

const http = require('http');
const express = require('express');
const { Server } = require('socket.io');
const path = require('path');
const QRCode = require('qrcode');

const PORT = process.env.PORT || 3075;

const app = express();
app.use(express.static(path.join(__dirname, 'public')));

app.get('/qr', async (req, res) => {
  const url = req.query.url;
  if (!url) return res.status(400).end();
  const png = await QRCode.toBuffer(url, { width: 200, margin: 2 });
  res.setHeader('Content-Type', 'image/png');
  res.send(png);
});

app.get('/discover', (req, res) => {
  const ip = (req.headers['x-forwarded-for'] || '').split(',')[0].trim() || req.socket.remoteAddress;
  const entry = Object.entries(rooms).find(([, r]) => r.ip === ip && r.receivers.size > 0);
  res.json({ room: entry ? entry[0] : null });
});

const server = http.createServer(app);
const io = new Server(server);

// rooms[code] = { receivers: Map<socketId,{}>, senders: Set<socketId>, ip, pendingApproval: {receiverId, mode, expiresAt} | null }
const rooms = {};

function clientIP(socket) {
  return (socket.handshake.headers['x-forwarded-for'] || '').split(',')[0].trim()
    || socket.handshake.address;
}

function getRoom(code) {
  if (!rooms[code]) rooms[code] = { receivers: new Map(), senders: new Set(), ip: null, pendingApproval: null };
  return rooms[code];
}

io.on('connection', (socket) => {
  socket.on('register', ({ role, room }) => {
    socket.role = role;
    socket.room = room;
    socket.join(room);
    const r = getRoom(room);

    if (role === 'receiver') {
      r.receivers.set(socket.id, {});
      r.ip = clientIP(socket);
      console.log(`[${room}] Receiver joined (${r.receivers.size} total)`);
      r.senders.forEach(sid => io.to(sid).emit('receiver-joined', { receiverId: socket.id }));

    } else if (role === 'sender') {
      r.senders.add(socket.id);
      console.log(`[${room}] Sender connected`);
      if (r.receivers.size > 0) socket.emit('receivers-online', { count: r.receivers.size });

      // Deliver a pending approval if receiver is still connected
      if (r.pendingApproval && Date.now() < r.pendingApproval.expiresAt) {
        const { receiverId } = r.pendingApproval;
        if (io.sockets.sockets.has(receiverId)) {
          console.log(`[${room}] delivering pending approval → new sender`);
          socket.emit('receiver-approved', { receiverId });
          r.pendingApproval = null;
        } else {
          r.pendingApproval = null;
        }
      }
    }
  });

  socket.on('connection-request', ({ mode } = {}) => {
    const r = rooms[socket.room];
    if (!r) return;
    // Clear any stale pending approval when sender requests fresh
    r.pendingApproval = null;
    console.log(`[${socket.room}] connection-request mode=${mode} receivers=${r.receivers.size}`);
    r.receivers.forEach((_, receiverId) => {
      io.to(receiverId).emit('connection-request', { senderId: socket.id, mode });
    });
  });

  socket.on('connection-approved', ({ senderId }) => {
    const r = rooms[socket.room];
    const senderAlive = io.sockets.sockets.has(senderId);
    console.log(`[${socket.room}] connection-approved sender=${senderId} alive=${senderAlive}`);

    if (senderAlive) {
      io.to(senderId).emit('receiver-approved', { receiverId: socket.id });
    } else {
      // Sender dropped — store approval for 30s so it's delivered on reconnect
      if (r) {
        r.pendingApproval = { receiverId: socket.id, expiresAt: Date.now() + 30000 };
        console.log(`[${socket.room}] approval stored, waiting for sender to reconnect`);
      }
    }
  });

  socket.on('connection-denied', ({ senderId }) => {
    const r = rooms[socket.room];
    if (r) r.pendingApproval = null;
    console.log(`[${socket.room}] connection-denied sender=${senderId}`);
  });

  socket.on('client-log', (msg) => {
    console.log(`[CLIENT ${socket.id.slice(-4)}] ${msg}`);
  });

  socket.on('offer', ({ offer, receiverId }) => {
    console.log(`[${socket.room}] offer → ${receiverId}`);
    io.to(receiverId).emit('offer', { offer, senderId: socket.id });
  });

  socket.on('answer', ({ answer, senderId }) => {
    console.log(`[${socket.room}] answer → ${senderId}`);
    io.to(senderId).emit('answer', { answer, receiverId: socket.id });
  });

  socket.on('ice-candidate', ({ candidate, target }) => {
    const dest = target === '__receiver__'
      ? [...(rooms[socket.room]?.receivers.keys() || [])]?.[0]
      : target;
    if (dest) {
      io.to(dest).emit('ice-candidate', { candidate, from: socket.id });
    }
  });

  socket.on('media-control', ({ action, receiverId, page }) => {
    io.to(receiverId).emit('media-control', { action, page });
  });

  socket.on('pdf-ready', ({ pages, senderId }) => {
    io.to(senderId).emit('pdf-ready', { pages });
  });

  socket.on('youtube-url', ({ url }) => {
    console.log(`[${socket.room}] youtube-url broadcast`);
    socket.to(socket.room).emit('youtube-url', { url });
  });

  socket.on('disconnect', () => {
    const room = socket.room;
    if (!room || !rooms[room]) return;
    const r = rooms[room];

    if (socket.role === 'receiver') {
      r.receivers.delete(socket.id);
      // If this receiver had a pending approval, cancel it
      if (r.pendingApproval?.receiverId === socket.id) r.pendingApproval = null;
      console.log(`[${room}] Receiver left (${r.receivers.size} remaining)`);
      r.senders.forEach(sid => io.to(sid).emit('receiver-left', { receiverId: socket.id }));
      if (r.receivers.size === 0) r.senders.forEach(sid => io.to(sid).emit('receivers-offline'));
    } else if (socket.role === 'sender') {
      r.senders.delete(socket.id);
    }
  });
});

server.listen(PORT, () => {
  console.log(`Church Mirror running on port ${PORT}`);
});

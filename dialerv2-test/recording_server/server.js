// jambonz listen-recorder (local filesystem sink)
// Deps: npm i ws
// Run : SAVE_DIR=./recordings CUSTOMER_CHANNEL=right MAX_SECONDS=15 node server.js

import http from 'http';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { WebSocketServer } from 'ws';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// -------------------------
// Configuration (env vars)
// -------------------------
const PORT = parseInt(process.env.PORT || '8080', 10);
const SAVE_DIR = process.env.SAVE_DIR || path.join(__dirname, 'recordings');
const CUSTOMER_CHANNEL = (process.env.CUSTOMER_CHANNEL || 'right').toLowerCase(); // 'left' or 'right'
const MAX_SECONDS = Number(process.env.MAX_SECONDS || 3600); // hard stop after N seconds of mono audio
const MAX_CALLS = parseInt(process.env.MAX_CALLS || '2500', 10); // concurrency cap
const ASSUME_BIG_ENDIAN = (process.env.ASSUME_BIG_ENDIAN || 'true').toLowerCase() === 'true'; // jambonz L16 is BE

// -------------------------
// Initialize resources
// -------------------------
fs.mkdirSync(SAVE_DIR, { recursive: true });

// -------------------------
// Helpers (WAV)
// -------------------------
function writeWavHeader(fd, sampleRate, dataBytes) {
  const header = Buffer.alloc(44);
  header.write('RIFF', 0);
  header.writeUInt32LE(36 + dataBytes, 4);
  header.write('WAVE', 8);
  header.write('fmt ', 12);
  header.writeUInt32LE(16, 16);
  header.writeUInt16LE(1, 20);      // PCM
  header.writeUInt16LE(1, 22);      // mono
  header.writeUInt32LE(sampleRate, 24);
  header.writeUInt32LE(sampleRate * 2, 28);
  header.writeUInt16LE(2, 32);
  header.writeUInt16LE(16, 34);
  header.write('data', 36);
  header.writeUInt32LE(dataBytes, 40);
  fs.writeSync(fd, header, 0, 44, 0);
}

function patchWavHeaderSizes(filePath, actualDataBytes, sampleRate) {
  const fd = fs.openSync(filePath, 'r+');
  try {
    const header = Buffer.alloc(44);
    fs.readSync(fd, header, 0, 44, 0);
    header.writeUInt32LE(36 + actualDataBytes, 4);
    header.writeUInt32LE(sampleRate, 24);
    header.writeUInt32LE(sampleRate * 2, 28);
    header.writeUInt16LE(2, 32);
    header.writeUInt16LE(16, 34);
    header.writeUInt32LE(actualDataBytes, 40);
    fs.writeSync(fd, header, 0, 44, 0);
  } finally {
    fs.closeSync(fd);
  }
}

// WebSocket server
// -------------------------
const server = http.createServer();
const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  // Hard cap on active sessions
  if (wss.clients.size > MAX_CALLS) {
    console.warn(`Rejecting connection: active=${wss.clients.size} cap=${MAX_CALLS}`);
    ws.close(1013, 'Server overloaded'); // 1013: Try Again Later
    return;
  }

  let meta = null;
  let wavStream = null;
  let filePath = null;
  let bytesWritten = 0;
  let maxMonoBytes = 0;
  let swapBuf = null;
  let finalized = false; // one-shot guard
  const channelIndex = CUSTOMER_CHANNEL === 'left' ? 0 : 1; // 0=L, 1=R

  function openFileAndInit(m) {
    const sampleRate = Number(m.sampleRate || 8000);
    const from = (m.from || '').toString().replace(/[^\d+]/g, '');
    const to   = (m.to   || '').toString().replace(/[^\d+]/g, '');
    const callSid = (m.call_sid || 'unknown').toString();
    const when = new Date().toISOString().replace(/[:.]/g, '-');
    const baseName = `${when}_${from}_to_${to}_${callSid}_${CUSTOMER_CHANNEL}.wav`;
    filePath = path.join(SAVE_DIR, baseName);

    maxMonoBytes = sampleRate * MAX_SECONDS * 2; // 2 bytes/sample (mono)
    const fd = fs.openSync(filePath, 'w');
    writeWavHeader(fd, sampleRate, maxMonoBytes);
    fs.closeSync(fd);

    wavStream = fs.createWriteStream(filePath, { flags: 'r+', start: 44 });

    console.log(`🟢 listen start: ${from} → ${to} (${callSid}) @ ${sampleRate}Hz, channel=${CUSTOMER_CHANNEL}, target=${MAX_SECONDS}s, active=${wss.clients.size}`);
  }

  async function finalizeOnce(reason = '') {
    if (finalized) return;
    finalized = true;

    if (!filePath || !meta) return;
    const sampleRate = Number(meta.sampleRate || 8000);

    // Patch header in case we wrote < max
    patchWavHeaderSizes(filePath, bytesWritten, sampleRate);

    console.log(`✅ recording saved at ${filePath}${reason ? ` | reason=${reason}` : ''}`);
  }

  ws.on('message', async (data, isBinary) => {
    try {
      // First frame: metadata JSON
      if (!meta && !isBinary) {
        meta = JSON.parse(data.toString('utf8'));
        if (!meta || typeof meta !== 'object') throw new Error('invalid metadata frame');
        if (!meta.mixType) meta.mixType = 'stereo';
        openFileAndInit(meta);
        return;
      }

      // Binary frames: L16 PCM
      if (isBinary && wavStream) {
        const buf = Buffer.from(data);
        const isStereo = String(meta.mixType || '').toLowerCase() === 'stereo';

        if (isStereo) {
          // Interleaved stereo; copy only desired channel
          const outSamples = Math.floor(buf.length / 4);
          let out = Buffer.allocUnsafe(outSamples * 2);
          for (let i = 0, o = 0; i + 3 < buf.length; i += 4, o += 2) {
            const src = channelIndex === 0 ? i : i + 2;
            out[o] = buf[src];
            out[o + 1] = buf[src + 1];
          }
          if (ASSUME_BIG_ENDIAN) {
            if (!swapBuf || swapBuf.length !== out.length) swapBuf = Buffer.allocUnsafe(out.length);
            for (let i = 0; i < out.length; i += 2) {
              swapBuf[i] = out[i + 1];
              swapBuf[i + 1] = out[i];
            }
            out = swapBuf;
          }
          const remaining = maxMonoBytes - bytesWritten;
          if (remaining > 0) {
            const slice = remaining < out.length ? out.subarray(0, remaining) : out;
            wavStream.write(slice);
            bytesWritten += slice.length;
          }
        } else {
          // Mono input
          let out = buf;
          if (ASSUME_BIG_ENDIAN) {
            if (!swapBuf || swapBuf.length !== out.length) swapBuf = Buffer.allocUnsafe(out.length);
            for (let i = 0; i < out.length; i += 2) {
              swapBuf[i] = out[i + 1];
              swapBuf[i + 1] = out[i];
            }
            out = swapBuf;
          }
          const remaining = maxMonoBytes - bytesWritten;
          if (remaining > 0) {
            const slice = remaining < out.length ? out.subarray(0, remaining) : out;
            wavStream.write(slice);
            bytesWritten += slice.length;
          }
        }

        // reached target duration?
        if (bytesWritten >= maxMonoBytes) {
          try { ws.send(JSON.stringify({ type: 'disconnect' })); } catch {}
          wavStream.end(async () => {
            try { await finalizeOnce('limit'); }
            catch (e) { console.error('finalize error:', e); }
            finally {
              console.log('⛔ closing socket (limit reached)');
              try { ws.close(1000, 'done'); } catch {}
            }
          });
        }
      }
    } catch (err) {
      console.error('frame handling error:', err);
      try { ws.close(1011, 'internal error'); } catch {}
      if (wavStream) { try { wavStream.end(); } catch {} }
    }
  });

  ws.on('close', async () => {
    if (wavStream && !wavStream.closed) {
      await new Promise((r) => wavStream.end(r));
    }
    if (bytesWritten > 0) {
      try { await finalizeOnce('socket-close'); }
      catch (e) { console.error('finalize error (on close):', e); }
    }
  });

  ws.on('error', (err) => {
    console.error('WS error:', err);
    if (wavStream) { try { wavStream.end(); } catch {} }
  });
});

// graceful shutdown
process.on('SIGINT', async () => {
  console.log('\n🛑 shutting down...');
  wss.clients.forEach((c) => { try { c.terminate(); } catch {} });
  server.close(() => console.log('HTTP server closed'));
  process.exit(0);
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`🎧 Recorder listening on 0.0.0.0:${PORT}`);
  console.log(`SAVE_DIR=${SAVE_DIR}  CUSTOMER_CHANNEL=${CUSTOMER_CHANNEL}  MAX_SECONDS=${MAX_SECONDS}  MAX_CALLS=${MAX_CALLS}`);
  console.log(`Recordings saved locally under ${SAVE_DIR}`);
});

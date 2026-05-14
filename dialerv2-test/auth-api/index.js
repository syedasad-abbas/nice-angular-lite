"use strict";

const express = require("express");
const cors = require("cors");
const axios = require("axios");
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");
const crypto = require("crypto");
const { createClient } = require("redis");

const config = {
  port: parseInt(process.env.AUTH_API_PORT || "3100", 10),
  redisUri: process.env.REDIS_URI || "redis://127.0.0.1:6379",
  jwtSecret: process.env.JWT_SECRET || "replace-me",
  jwtExpiresIn: process.env.JWT_EXPIRES_IN || "12h",
  sipDomain: process.env.SIP_DOMAIN || "127.0.0.1",
  sipWsUrl: process.env.SIP_WS_URL || "wss://127.0.0.1:7443",
  seedAdminOnStart:
    String(process.env.SEED_ADMIN_ON_START || "false").toLowerCase() === "true",
  seedAdminUsername: process.env.SEED_ADMIN_USERNAME || "admin",
  seedAdminPassword: process.env.SEED_ADMIN_PASSWORD || "Pass@123",
  seedAdminName: process.env.SEED_ADMIN_NAME || "Portal Admin",
  seedAdminExtension: process.env.SEED_ADMIN_EXTENSION || "1001",
  activeLoginTtlSec: parseInt(process.env.ACTIVE_LOGIN_TTL_SEC || "43200", 10),
  dialerIngestUrl:
    process.env.DIALER_INGEST_URL || "http://dialer-aio:3000/v1/test-call",
  dialerIngestApiKey: process.env.DIALER_INGEST_API_KEY || "",
  manualCallDefaultPrefix: process.env.MANUAL_CALL_DEFAULT_PREFIX || "",
  manualCallDestinationAddress:
    process.env.MANUAL_CALL_DESTINATION_ADDRESS ||
    "sip:1000@127.0.0.1",
  manualCallWavAnnounce:
    process.env.MANUAL_CALL_WAV_ANNOUNCE || "https://example.com/announce.wav",
  manualCallWavVm:
    process.env.MANUAL_CALL_WAV_VM || "https://example.com/vm.wav",
  manualCallWavContinue:
    process.env.MANUAL_CALL_WAV_CONTINUE || "https://example.com/continue.wav",
  manualCallWavOptOut:
    process.env.MANUAL_CALL_WAV_OPTOUT || "https://example.com/optout.wav",
  manualCallDigitContinue: process.env.MANUAL_CALL_DIGIT_CONTINUE || "1",
  manualCallDigitOptOut: process.env.MANUAL_CALL_DIGIT_OPTOUT || "2",
  manualCallTimeoutMs: parseInt(process.env.MANUAL_CALL_TIMEOUT_MS || "10000", 10),
  manualCallRateWindowSec: parseInt(
    process.env.MANUAL_CALL_RATE_WINDOW_SEC || "60",
    10
  ),
  manualCallRateMax: parseInt(process.env.MANUAL_CALL_RATE_MAX || "3", 10),
  manualCallActiveLockSec: parseInt(
    process.env.MANUAL_CALL_ACTIVE_LOCK_SEC || "180",
    10
  ),
};

const app = express();
app.use(cors());
app.use(express.json({ limit: "256kb" }));

const redis = createClient({ url: config.redisUri });
redis.on("error", (err) => console.error("redis error", err.message));

function nowIso() {
  return new Date().toISOString();
}

function normalize(text) {
  return String(text || "").trim().toLowerCase();
}

function userKey(userId) {
  return `portal:user:${userId}`;
}

function usernameKey(username) {
  return `portal:user:username:${normalize(username)}`;
}

function emailKey(email) {
  return `portal:user:email:${normalize(email)}`;
}

function manualCallRateKey(agentId) {
  return `manualcall:rate:${agentId}`;
}

function manualCallActiveKey(agentId) {
  return `manualcall:active:${agentId}`;
}

function roleKey(roleId) {
  return `portal:role:${roleId}`;
}

function permissionKey(permissionId) {
  return `portal:permission:${permissionId}`;
}

function normalizePermissionId(input) {
  return normalize(input).replace(/\s+/g, "-");
}

function parsePermissions(input) {
  if (Array.isArray(input)) {
    return input
      .map((p) => normalizePermissionId(String(p || "")))
      .filter(Boolean);
  }
  if (typeof input === "string" && input.trim()) {
    try {
      const parsed = JSON.parse(input);
      if (Array.isArray(parsed)) {
        return parsed
          .map((p) => normalizePermissionId(String(p || "")))
          .filter(Boolean);
      }
    } catch {
      return input
        .split(",")
        .map((p) => normalizePermissionId(p))
        .filter(Boolean);
    }
  }
  return [];
}

function serializeRole(role) {
  return {
    ...role,
    permissions: parsePermissions(role.permissions),
  };
}

async function getUserById(userId) {
  if (!userId) return null;
  const data = await redis.hGetAll(userKey(userId));
  if (!data || !Object.keys(data).length) return null;
  return data;
}

async function getUserByUsernameOrEmail(identifier) {
  const id = normalize(identifier);
  if (!id) return null;

  let userId = await redis.get(usernameKey(id));
  if (!userId) userId = await redis.get(emailKey(id));
  if (!userId) return null;

  return getUserById(userId);
}

function publicUser(user) {
  return {
    id: user.id,
    username: user.username,
    email: user.email || "",
    name: user.name || "",
    role: user.role || "agent",
    extension: user.extension || "",
    status: user.status || "enabled",
  };
}

function normalizeDialNumber(value) {
  return String(value || "").replace(/[^\d+]/g, "").trim();
}

function isValidManualNumber(value) {
  const n = normalizeDialNumber(value);
  return /^\+?\d{8,15}$/.test(n);
}

async function enforceManualRateLimit(agentId) {
  const key = manualCallRateKey(agentId);
  const n = await redis.incr(key);
  if (n === 1) {
    await redis.expire(key, Math.max(1, config.manualCallRateWindowSec));
  }
  if (n > config.manualCallRateMax) {
    const ttl = await redis.ttl(key);
    return { ok: false, retryAfterSec: ttl > 0 ? ttl : config.manualCallRateWindowSec };
  }
  return { ok: true };
}

async function acquireManualActiveLock(agentId, transactionId) {
  const key = manualCallActiveKey(agentId);
  const ok = await redis.set(key, transactionId, {
    NX: true,
    EX: Math.max(30, config.manualCallActiveLockSec),
  });
  return ok === "OK";
}

async function releaseManualActiveLock(agentId, transactionId) {
  const key = manualCallActiveKey(agentId);
  const current = await redis.get(key);
  if (current && current === transactionId) {
    await redis.del(key);
  }
}

function buildPublicWsUrl(req, path, fallback) {
  try {
    const hostHeader = String(
      req.headers["x-forwarded-host"] || req.headers.host || ""
    ).trim();
    if (!hostHeader) return fallback;
    const forwardedProto = String(req.headers["x-forwarded-proto"] || "")
      .split(",")[0]
      .trim()
      .toLowerCase();
    const reqProto = (req.protocol || "").toLowerCase();
    const httpProto = forwardedProto || reqProto || "http";
    const wsProto = httpProto === "https" ? "wss" : "ws";
    const safePath = path.startsWith("/") ? path : `/${path}`;
    return `${wsProto}://${hostHeader}${safePath}`;
  } catch {
    return fallback;
  }
}

function sipPayload(user, req) {
  const ws = buildPublicWsUrl(req, "/sip-ws", config.sipWsUrl);
  return {
    uri: `sip:${user.extension}@${config.sipDomain}`,
    password: user.sipPassword || "",
    ws,
  };
}

function signToken(user) {
  return jwt.sign(
    {
      sub: user.id,
      role: user.role,
      username: user.username,
      extension: user.extension || "",
    },
    config.jwtSecret,
    { expiresIn: config.jwtExpiresIn }
  );
}

function authRequired(req, res, next) {
  const raw = String(req.headers.authorization || "");
  const token = raw.startsWith("Bearer ") ? raw.slice(7) : "";

  if (!token) return res.status(401).json({ error: "missing-token" });

  try {
    req.auth = jwt.verify(token, config.jwtSecret);
    return next();
  } catch {
    return res.status(401).json({ error: "invalid-token" });
  }
}

function adminRequired(req, res, next) {
  if (!req.auth || req.auth.role !== "admin") {
    return res.status(403).json({ error: "forbidden" });
  }
  return next();
}

async function createPortalUser(input) {
  const username = normalize(input.username || input.email);
  const email = normalize(input.email);
  const password = String(input.password || "");
  const role = String(input.role || "agent");
  const extension = String(input.extension || "").trim();
  const name = String(input.name || "").trim();
  const status = String(input.status || "enabled");

  // ✅ REQUIRED FIELD VALIDATION
  if (!username || !password || !role || !extension) {
    throw new Error("missing-required-fields");
  }

  // ❌ STRICT EXTENSION VALIDATION
  if (!/^\d{3,6}$/.test(extension)) {
    throw new Error("invalid-extension");
  }

  if (!["admin", "agent", "user"].includes(role)) {
    throw new Error("invalid-role");
  }

  const existsByUsername = await redis.get(usernameKey(username));
  if (existsByUsername) throw new Error("username-exists");

  if (email) {
    const existsByEmail = await redis.get(emailKey(email));
    if (existsByEmail) throw new Error("email-exists");
  }

  const id = input.id || `agent-${extension}`;

  const passwordHash = await bcrypt.hash(password, 10);
  const createdAt = nowIso();

  // 🔥 SIP PASSWORD SAFETY FIX (CRITICAL)
  const sipPassword =
    input.sipPassword?.trim() ||
    password ||
    crypto.randomBytes(6).toString("hex");

  const user = {
    id,
    username,
    email,
    name,
    role,
    extension,
    status,
    passwordHash,
    sipPassword,
    createdAt,
    updatedAt: createdAt,
  };

  await redis.hSet(userKey(id), user);
  await redis.set(usernameKey(username), id);

  if (email) {
    await redis.set(emailKey(email), id);
  }

  await redis.set(`agent:ext:${extension}`, id, { EX: 86400 });

  // FreeSWITCH directory user (SIP provisioning)
  await redis.hSet(`fsdir:user:${extension}`, {
    password: sipPassword,
    userContext: "default",
    callerIdName: name || extension,
    callerIdNumber: extension,
    agentId: id,
  });

  // Agent presence record
  await redis.hSet(`agent:${id}`, {
    extension,
    portalOnline: "false",
    sipRegistered: "false",
    availableInbound: "true",
    availableOutbound: "true",
    inCall: "false",
    lastAssignedAt: String(Date.now()),
  });

  // 🔍 DEBUG LOG (IMPORTANT FOR SIP TROUBLESHOOTING)
  console.log("[SIP][USER_CREATED]", {
    id,
    username,
    extension,
    hasSipPassword: !!sipPassword,
    fsdirKey: `fsdir:user:${extension}`,
  });

  return user;
}

async function deletePortalUserById(userId) {
  const user = await getUserById(userId);
  if (!user) return { deleted: false, reason: "user-not-found" };

  const extension = String(user.extension || "").trim();
  const username = normalize(user.username);
  const email = normalize(user.email);

  const keysToDelete = [
    userKey(userId),
    username ? usernameKey(username) : "",
    email ? emailKey(email) : "",
    extension ? `agent:ext:${extension}` : "",
    extension ? `fsdir:user:${extension}` : "",
    `agent:${userId}`,
    `portal:login:active:${userId}`,
    `manualcall:rate:${userId}`,
    `manualcall:active:${userId}`,
  ].filter(Boolean);

  if (keysToDelete.length) {
    await redis.del(keysToDelete);
  }

  return { deleted: true, user };
}

app.get("/healthz", async (_req, res) => {
  const ok = redis.isReady;
  res.status(ok ? 200 : 503).json({
    status: ok ? "ok" : "degraded",
    redis: ok,
  });
});

app.post("/auth/login", async (req, res) => {
  const identifier = String(req.body?.username || req.body?.email || "").trim();
  const password = String(req.body?.password || "");

  if (!identifier || !password) {
    return res.status(400).json({ error: "missing-credentials" });
  }

  const user = await getUserByUsernameOrEmail(identifier);
  if (!user) return res.status(401).json({ error: "invalid-credentials" });

  const ok = await bcrypt.compare(password, user.passwordHash || "");
  if (!ok) return res.status(401).json({ error: "invalid-credentials" });

  const token = signToken(user);

  // Track active dashboard logins with TTL so stats reflect real portal usage.
  await redis.set(`portal:login:active:${user.id}`, "1", { EX: config.activeLoginTtlSec });

  return res.status(200).json({
    token,
    user: publicUser(user),
    sip: sipPayload(user, req),
  });
});

app.post("/admin/users", authRequired, adminRequired, async (req, res) => {
  try {
    const user = await createPortalUser(req.body || {});
    return res.status(201).json({ user: publicUser(user) });
  } catch (err) {
    const map = {
      "missing-required-fields": 400,
      "invalid-extension": 400,
      "invalid-role": 400,
      "username-exists": 409,
      "email-exists": 409,
    };

    const code = map[err.message] || 500;

    return res.status(code).json({
      error: err.message || "internal-error",
    });
  }
});

app.get("/admin/users", authRequired, adminRequired, async (_req, res) => {
  try {
    const all = await redis.keys("portal:user:*");

    // Keep only: portal:user:<id>
    // Exclude: portal:user:username:* and portal:user:email:*
    const userKeys = all.filter((k) => k.split(":").length === 3);

    if (!userKeys.length) return res.status(200).json([]);

    const rows = await Promise.all(
      userKeys.map(async (k) => {
        try {
          return await redis.hGetAll(k);
        } catch {
          return null;
        }
      })
    );

    const users = rows
      .filter((u) => u && Object.keys(u).length && u.id)
      .map((u) => publicUser(u))
      .sort((a, b) => (a.username || "").localeCompare(b.username || ""));

    return res.status(200).json(users);
  } catch (err) {
    console.error("list users failed", err);
    return res.status(500).json({ error: "internal-error" });
  }
});

app.delete("/admin/users/:id", authRequired, adminRequired, async (req, res) => {
  try {
    const targetId = String(req.params.id || "").trim();
    if (!targetId) {
      return res.status(400).json({ error: "missing-user-id" });
    }

    if (req.auth?.sub && String(req.auth.sub) === targetId) {
      return res.status(400).json({ error: "cannot-delete-self" });
    }

    const out = await deletePortalUserById(targetId);
    if (!out.deleted) {
      return res.status(404).json({ error: out.reason || "user-not-found" });
    }

    return res.status(200).json({
      status: "deleted",
      user: publicUser(out.user),
    });
  } catch (err) {
    console.error("delete user failed", err);
    return res.status(500).json({ error: "internal-error" });
  }
});

app.put("/admin/users/:id", authRequired, adminRequired, async (req, res) => {
  try {
    const targetId = String(req.params.id || "").trim();
    if (!targetId) return res.status(400).json({ error: "missing-user-id" });

    const existing = await getUserById(targetId);
    if (!existing) return res.status(404).json({ error: "user-not-found" });

    const updates = {};
    if (req.body.name !== undefined) updates.name = String(req.body.name).trim();
    if (req.body.email !== undefined) updates.email = String(req.body.email).trim();
    if (req.body.role !== undefined) updates.role = String(req.body.role).trim();
    if (req.body.status !== undefined) updates.status = String(req.body.status).trim();
    
    if (req.body.password) {
      updates.passwordHash = await bcrypt.hash(String(req.body.password), 10);
    }
    if (req.body.sipPassword) {
      updates.sipPassword = String(req.body.sipPassword).trim();
      // Need to update FreeSWITCH dir as well
      await redis.hSet(`fsdir:user:${existing.extension}`, "password", updates.sipPassword);
    }

    const updatedUser = { ...existing, ...updates, updatedAt: nowIso() };
    await redis.hSet(userKey(targetId), updatedUser);
    
    return res.status(200).json({ user: publicUser(updatedUser) });
  } catch (err) {
    console.error("update user failed", err);
    return res.status(500).json({ error: "internal-error" });
  }
});

// --- ROLES API ---

app.get("/admin/roles", authRequired, adminRequired, async (_req, res) => {
  try {
    const all = await redis.keys("portal:role:*");
    if (!all.length) return res.status(200).json([]);

    const rows = await Promise.all(
      all.map(async (k) => {
        try {
          return await redis.hGetAll(k);
        } catch {
          return null;
        }
      })
    );

    const roles = rows
      .filter((r) => r && Object.keys(r).length && r.id)
      .map((r) => serializeRole(r))
      .sort((a, b) => (a.name || "").localeCompare(b.name || ""));

    return res.status(200).json(roles);
  } catch (err) {
    console.error("list roles failed", err);
    return res.status(500).json({ error: "internal-error" });
  }
});

app.post("/admin/roles", authRequired, adminRequired, async (req, res) => {
  try {
    const { name, description, permissions } = req.body || {};
    if (!name) return res.status(400).json({ error: "missing-name" });

    const id = normalize(name).replace(/\s+/g, "-");
    const existing = await redis.hGetAll(roleKey(id));
    if (existing && Object.keys(existing).length) {
      return res.status(409).json({ error: "role-exists" });
    }

    const normalizedPermissions = parsePermissions(permissions);
    const role = {
      id,
      name,
      description: description || "",
      permissions: JSON.stringify(normalizedPermissions),
      createdAt: nowIso(),
    };

    await redis.hSet(roleKey(id), role);
    return res.status(201).json(serializeRole(role));
  } catch (err) {
    console.error("create role failed", err);
    return res.status(500).json({ error: "internal-error" });
  }
});

app.put("/admin/roles/:id", authRequired, adminRequired, async (req, res) => {
  try {
    const roleId = req.params.id;
    const { name, description, permissions } = req.body || {};

    const existing = await redis.hGetAll(roleKey(roleId));
    if (!existing || !Object.keys(existing).length) {
      return res.status(404).json({ error: "role-not-found" });
    }

    const normalizedPermissions =
      permissions === undefined
        ? parsePermissions(existing.permissions)
        : parsePermissions(permissions);

    const updated = {
      ...existing,
      name: name || existing.name,
      description: description !== undefined ? description : existing.description,
      permissions: JSON.stringify(normalizedPermissions),
      updatedAt: nowIso(),
    };

    await redis.hSet(roleKey(roleId), updated);
    return res.status(200).json(serializeRole(updated));
  } catch (err) {
    console.error("update role failed", err);
    return res.status(500).json({ error: "internal-error" });
  }
});

// --- PERMISSIONS API ---
app.get("/admin/permissions", authRequired, adminRequired, async (_req, res) => {
  try {
    const all = await redis.keys("portal:permission:*");
    if (!all.length) return res.status(200).json([]);

    const rows = await Promise.all(
      all.map(async (k) => {
        try {
          return await redis.hGetAll(k);
        } catch {
          return null;
        }
      })
    );

    const permissions = rows
      .filter((r) => r && Object.keys(r).length && r.id)
      .sort((a, b) => (a.name || "").localeCompare(b.name || ""));

    return res.status(200).json(permissions);
  } catch (err) {
    console.error("list permissions failed", err);
    return res.status(500).json({ error: "internal-error" });
  }
});

app.post("/admin/permissions", authRequired, adminRequired, async (req, res) => {
  try {
    const { name, description } = req.body || {};
    if (!name) return res.status(400).json({ error: "missing-name" });

    const id = normalizePermissionId(name);
    const existing = await redis.hGetAll(permissionKey(id));
    if (existing && Object.keys(existing).length) {
      return res.status(409).json({ error: "permission-exists" });
    }

    const permission = {
      id,
      name,
      description: description || "",
      createdAt: nowIso(),
    };

    await redis.hSet(permissionKey(id), permission);
    return res.status(201).json(permission);
  } catch (err) {
    console.error("create permission failed", err);
    return res.status(500).json({ error: "internal-error" });
  }
});

app.delete("/admin/roles/:id", authRequired, adminRequired, async (req, res) => {
  try {
    const roleId = req.params.id;
    if (["admin", "agent", "user"].includes(roleId)) {
      return res.status(400).json({ error: "cannot-delete-system-role" });
    }

    const existing = await redis.hGetAll(roleKey(roleId));
    if (!existing || !Object.keys(existing).length) {
      return res.status(404).json({ error: "role-not-found" });
    }

    await redis.del(roleKey(roleId));
    return res.status(200).json({ status: "deleted", id: roleId });
  } catch (err) {
    console.error("delete role failed", err);
    return res.status(500).json({ error: "internal-error" });
  }
});

app.get("/admin/dashboard", authRequired, adminRequired, async (_req, res) => {
  try {
    const all = await redis.keys("portal:user:*");
    const userKeys = all.filter((k) => k.split(":").length === 3);

    if (!userKeys.length) {
      const answeredAgentCallsRaw = await redis.get("dialer:stats:agent_answered_calls_total");
      const answeredAgentCalls = parseInt(answeredAgentCallsRaw || "0", 10) || 0;
      return res.status(200).json({ totalUsers: 0, loggedInUsers: 0, answeredAgentCalls });
    }

    const userRows = await Promise.all(
      userKeys.map(async (k) => {
        try {
          return await redis.hGetAll(k);
        } catch {
          return null;
        }
      })
    );

    const users = userRows.filter((u) => u && Object.keys(u).length && u.id);
    const totalUsers = users.length;

    // "Logged-in Users" = active portal logins (TTL-based), not SIP/WS presence.
    const loginKeys = await redis.keys("portal:login:active:*");
    const loggedInUsers = loginKeys.length;
    const answeredAgentCallsRaw = await redis.get("dialer:stats:agent_answered_calls_total");
    const answeredAgentCalls = parseInt(answeredAgentCallsRaw || "0", 10) || 0;

    return res.status(200).json({ totalUsers, loggedInUsers, answeredAgentCalls });
  } catch (err) {
    console.error("dashboard stats failed", err);
    return res.status(500).json({ error: "internal-error" });
  }
});

app.get("/admin/dashboard/answered-calls-series", authRequired, adminRequired, async (req, res) => {
  try {
    const range = String(req.query.range || "7d").toLowerCase(); // 24h | 7d | 1m | 1y
    const userId = String(req.query.userId || "all").trim(); // all | agent-1001 ...
    const isAllUsers = !userId || userId === "all";

    const labels = [];
    const series = [];
    const now = new Date();

    // 24h -> hourly buckets
    if (range === "24h") {
      for (let i = 23; i >= 0; i--) {
        const dt = new Date(now.getTime() - i * 60 * 60 * 1000);
        const hourKey = dt.toISOString().slice(0, 13); // YYYY-MM-DDTHH
        labels.push(`${hourKey.slice(11)}:00`);

        if (isAllUsers) {
          const v = await redis.get(`dialer:stats:agent_answered_calls_hourly:${hourKey}`);
          series.push(parseInt(v || "0", 10) || 0);
        } else {
          const v = await redis.hGet(`dialer:stats:agent_answered_calls_by_agent_hourly:${hourKey}`, userId);
          series.push(parseInt(v || "0", 10) || 0);
        }
      }
      return res.status(200).json({ labels, series });
    }

    // 7d / 1m / 1y -> daily buckets
    const days = range === "1m" ? 30 : range === "1y" ? 365 : 7;
    for (let i = days - 1; i >= 0; i--) {
      const dt = new Date(now);
      dt.setUTCDate(dt.getUTCDate() - i);
      const day = dt.toISOString().slice(0, 10); // YYYY-MM-DD
      labels.push(day.slice(5)); // MM-DD

      if (isAllUsers) {
        const v = await redis.get(`dialer:stats:agent_answered_calls_daily:${day}`);
        series.push(parseInt(v || "0", 10) || 0);
      } else {
        const v = await redis.hGet(`dialer:stats:agent_answered_calls_by_agent_daily:${day}`, userId);
        series.push(parseInt(v || "0", 10) || 0);
      }
    }

    return res.status(200).json({ labels, series });
  } catch (err) {
    console.error("answered calls series failed", err);
    return res.status(500).json({ error: "internal-error" });
  }
});

app.post("/agent/manual-call", authRequired, async (req, res) => {
  let transactionId = "";
  let agentId = "";
  try {
    const authUser = await getUserById(req.auth?.sub);
    if (!authUser) return res.status(401).json({ error: "invalid-user" });

    if (!["agent", "admin"].includes(String(authUser.role || ""))) {
      return res.status(403).json({ error: "forbidden" });
    }

    agentId = String(authUser.id || "").trim();
    const numberTo = normalizeDialNumber(req.body?.numberTo);
    if (!isValidManualNumber(numberTo)) {
      return res.status(400).json({ error: "invalid-numberTo" });
    }

    const agentExtension = String(authUser.extension || "").trim();
    if (!/^\d{3,6}$/.test(agentExtension)) {
      return res.status(400).json({ error: "invalid-agent-extension" });
    }

    const rate = await enforceManualRateLimit(agentId);
    if (!rate.ok) {
      res.setHeader("Retry-After", String(rate.retryAfterSec));
      return res.status(429).json({ error: "rate-limit-exceeded" });
    }

    transactionId = `manual-${agentId}-${Date.now()}-${crypto
      .randomBytes(3)
      .toString("hex")}`;

    const hasLock = await acquireManualActiveLock(agentId, transactionId);
    if (!hasLock) {
      return res.status(409).json({ error: "agent-already-has-active-call" });
    }

    const payload = {
      numberTo,
      numberFrom: agentExtension,
      transactionId,
      prefix: config.manualCallDefaultPrefix,
      wavUrlAnnounce: config.manualCallWavAnnounce,
      wavUrlVM: config.manualCallWavVm,
      wavUrlContinue: config.manualCallWavContinue,
      wavUrlOptOut: config.manualCallWavOptOut,
      digitContinue: config.manualCallDigitContinue,
      digitOptOut: config.manualCallDigitOptOut,
      destinationAddress: config.manualCallDestinationAddress,
      routeToAgent: true,
      isAgentCall: true,
      callType: "outbound",
      requestedBy: agentId,
    };

    const headers = { "Content-Type": "application/json" };
    if (config.dialerIngestApiKey) {
      headers["x-api-key"] = config.dialerIngestApiKey;
    }

    const dialerRes = await axios.post(config.dialerIngestUrl, payload, {
      headers,
      timeout: config.manualCallTimeoutMs,
      validateStatus: () => true,
    });

    if (dialerRes.status < 200 || dialerRes.status >= 300) {
      await releaseManualActiveLock(agentId, transactionId);
      return res.status(502).json({
        error: "dialer-rejected",
        dialerStatus: dialerRes.status,
      });
    }

    return res.status(202).json({
      status: "accepted",
      transactionId,
      numberTo,
    });
  } catch (err) {
    if (agentId && transactionId) {
      await releaseManualActiveLock(agentId, transactionId);
    }
    console.error("manual-call failed", err.message);
    return res.status(500).json({ error: "manual-call-failed" });
  }
});



app.get("/auth/me", authRequired, async (req, res) => {
  const user = await getUserById(req.auth.sub);
  if (!user) return res.status(404).json({ error: "user-not-found" });

  return res.status(200).json({
    user: publicUser(user),
    sip: sipPayload(user),
  });
});

async function seedPermissionsIfNeeded() {
  const perms = [
    { id: "dashboard:view", name: "Dashboard View", description: "View dashboard stats" },
    { id: "dialer:view", name: "Dialer View", description: "Access dialer interface" },
    { id: "user:view", name: "User View", description: "List and view users" },
    { id: "user:edit", name: "User Edit", description: "Create and update users" },
    { id: "user:delete", name: "User Delete", description: "Remove users" },
    { id: "role:create", name: "Role Create", description: "Create new roles" },
    { id: "role:edit", name: "Role Edit", description: "Update roles and permissions" },
    { id: "role:delete", name: "Role Delete", description: "Delete roles" },
    { id: "permissions:view", name: "View Permissions", description: "View system permissions list" },
  ];

  for (const p of perms) {
    const existing = await redis.hGetAll(permissionKey(p.id));
    if (!existing || !Object.keys(existing).length) {
      await redis.hSet(permissionKey(p.id), {
        ...p,
        createdAt: nowIso(),
      });
      console.log("seeded permission", p.id);
    }
  }
}

async function seedRolesIfNeeded() {
  const allPermissions = [
    "dashboard:view",
    "dialer:view",
    "user:view",
    "user:edit",
    "user:delete",
    "role:create",
    "role:edit",
    "role:delete",
    "permissions:view",
  ];

  const roles = [
    {
      id: "admin",
      name: "Administrator",
      description: "Full system access",
      permissions: allPermissions,
    },
    {
      id: "agent",
      name: "Agent",
      description: "Dialer and call handling",
      permissions: ["dashboard:view", "dialer:view"],
    },
    {
      id: "user",
      name: "User",
      description: "Standard portal access",
      permissions: ["dashboard:view"],
    },
  ];

  for (const r of roles) {
    const existing = await redis.hGetAll(roleKey(r.id));
    // If doesn't exist OR permissions are missing/empty, update it
    if (!existing || !Object.keys(existing).length || !existing.permissions || existing.permissions === "[]") {
      await redis.hSet(roleKey(r.id), {
        ...r,
        permissions: JSON.stringify(r.permissions),
        createdAt: existing.createdAt || nowIso(),
        updatedAt: nowIso(),
      });
      console.log("seeded/updated role", r.id);
    }
  }
}

async function seedAdminIfNeeded() {
  if (!config.seedAdminOnStart) return;

  const existing = await redis.get(
    usernameKey(config.seedAdminUsername)
  );

  if (existing) return;

  await createPortalUser({
    id: `agent-${config.seedAdminExtension}`,
    username: config.seedAdminUsername,
    password: config.seedAdminPassword,
    role: "admin",
    extension: config.seedAdminExtension,
    name: config.seedAdminName,
    sipPassword: config.seedAdminPassword,
  });

  console.log("seed admin created", config.seedAdminUsername);
}

async function main() {
  await redis.connect();
  await seedPermissionsIfNeeded();
  await seedRolesIfNeeded();
  await seedAdminIfNeeded();

  app.listen(config.port, "0.0.0.0", () => {
    console.log(`auth-api listening on :${config.port}`);
  });
}

main().catch((err) => {
  console.error("fatal", err);
  process.exit(1);
});

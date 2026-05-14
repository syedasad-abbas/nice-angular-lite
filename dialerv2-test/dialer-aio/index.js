'use strict';

const esl        = require('modesl');
const amqplib    = require('amqplib');
const { createClient } = require('redis');
const axios      = require('axios');
const crypto     = require('crypto');
const http       = require('http');
const { WebSocketServer, WebSocket } = require('ws');
const { parsePhoneNumberFromString } = require('libphonenumber-js');

function parseBool(value, defaultValue = false) {
  if (value == null) return defaultValue;
  return String(value).toLowerCase() === 'true';
}

// ═══════════════════════════════════════════════════════════════════════════════
//  CONFIGURATION  — every value from ENV for Kubernetes deployment
// ═══════════════════════════════════════════════════════════════════════════════

const config = {
  freeswitch: {
    host:             process.env.FREESWITCH_HOST            || '127.0.0.1',
    port:         parseInt(process.env.FREESWITCH_PORT       || '8021', 10),
    password:         process.env.FREESWITCH_PASSWORD        || 'ClueCon',
    sipDomain:        process.env.SIP_DOMAIN                 || '127.0.0.1',
    gateway:          process.env.SIP_GATEWAY                || '',
    profile:          process.env.SIP_PROFILE                || 'external',
    codecString:      process.env.CODEC_STRING               || 'PCMU,PCMA',
    originateTimeout:
                  parseInt(process.env.ORIGINATE_TIMEOUT_SEC || '60', 10),
    reconnectMs:
                  parseInt(process.env.ESL_RECONNECT_MS      || '5000', 10),
  },

  // Round-robin outbound trunk IPs
  trunkIps: (process.env.SIP_TRUNK_IPS || '169.197.85.202,169.197.85.203,169.197.85.204')
    .split(',').map(s => s.trim()).filter(Boolean),

  rabbitmq: {
    uri:              process.env.RABBITMQ_URI               || 'amqp://guest:guest@localhost:5672',
    queue:            process.env.RABBITMQ_CALLS_QUEUE       || 'calls_queue',
    prefetch:     parseInt(process.env.RABBITMQ_PREFETCH     || '60', 10),
    queueType:        process.env.RABBITMQ_QUEUE_TYPE        || 'quorum',
    reconnectMs:
                  parseInt(process.env.RABBITMQ_RECONNECT_MS || '5000', 10),
  },

  redis: {
    uri:              process.env.REDIS_URI                  || 'redis://localhost:6379',
  },

    // === NEW: Agent Pool Configuration (for browser agents) ===
  agent: {
    portalWsPort:     parseInt(process.env.PORTAL_WS_PORT       || '8081', 10),
    lockTtlSec:       parseInt(process.env.AGENT_LOCK_TTL_SEC   || '30', 10),
    cooldownSec:      parseInt(process.env.AGENT_COOLDOWN_SEC   || '5', 10),
    poolStrategy:     (process.env.AGENT_POOL_STRATEGY          || 'lru').toLowerCase(), // 'lru' or 'random'
    // === NEW: Retry & Ring Timeout Settings ===
    ringTimeoutSec:       parseInt(process.env.AGENT_RING_TIMEOUT_SEC    || '18', 10),
    maxAttemptsPerCall:   parseInt(process.env.AGENT_MAX_ATTEMPTS_PER_CALL || '4', 10),
    // === NEW: Inbound Queue Settings (Fix for when no agent available) ===
    inboundMaxWaitSec:  parseInt(process.env.INBOUND_MAX_WAIT_SEC || '45', 10),
    inboundRetryGapMs:  parseInt(process.env.INBOUND_RETRY_GAP_MS || '1200', 10),
    inboundHoldPrompt:  process.env.INBOUND_HOLD_PROMPT || 'ivr/queue-hold.wav',
  },

  api: {
    baseUrl:          process.env.CALL_STATUS_API_BASE_URL   || 'https://portal.dialytica.com/api',
    timeout:      parseInt(process.env.CALL_STATUS_API_TIMEOUT || '5000', 10),
    dispositionMaxRetries:
                 parseInt(process.env.DISPOSITION_MAX_RETRIES || '8', 10),
    dispositionRetryBaseMs:
                 parseInt(process.env.DISPOSITION_RETRY_BASE_MS || '1000', 10),
    dispositionRetryMaxMs:
                 parseInt(process.env.DISPOSITION_RETRY_MAX_MS || '60000', 10),
    dispositionRetryJitterMs:
                 parseInt(process.env.DISPOSITION_RETRY_JITTER_MS || '500', 10),
    dispositionQueueMax:
                 parseInt(process.env.DISPOSITION_QUEUE_MAX || '50000', 10),
    dispositionConcurrency:
                 parseInt(process.env.DISPOSITION_CONCURRENCY || '16', 10),
    dispositionDrainTimeoutMs:
                 parseInt(process.env.DISPOSITION_DRAIN_TIMEOUT_MS || '15000', 10),
  },

  calls: {
    ratePerSecond:parseInt(process.env.CALLS_PER_SECOND      || '30', 10),
    // DTMF gather timeout AFTER the announcement finishes playing (matches IVR-app default)
    dtmfGatherTimeout:
                  parseInt(process.env.DTMF_GATHER_TIMEOUT   || '15', 10),
    // Avoid '#' here: some FS API command parsers treat it as comment token.
    dtmfTerminators:  process.env.DTMF_TERMINATORS          || '*',
    // Safety-net: max seconds for any non-transferred call
    maxCallDurationMs:
                  parseInt(process.env.MAX_CALL_DURATION_MS  || '180000', 10),
    // Redis TTLs (same scheme as the IVR-app)
    answeredKeyTtl:
                  parseInt(process.env.ANSWERED_CALL_KEY_TTL || '14760', 10),
    inflightKeyTtl:
                  parseInt(process.env.INFLIGHT_KEY_TTL      || '120', 10),
    defaultPrefix:    process.env.DEFAULT_NUMBER_PREFIX      || '',
    // FreeSWITCH mod_http_cache prefix for remote WAV files
    audioCachePrefix: process.env.AUDIO_CACHE_PREFIX         || 'http_cache://',
    // Small settle delay after breaking IVR gather before starting VM playback
    // avoids clipping the first audio frames on some trunk/media paths.
    vmPlaybackStartDelayMs:
                  parseInt(process.env.VM_PLAYBACK_START_DELAY_MS || '180', 10),
    // Wait for remote-side silence before VM playback to avoid clipping if
    // voicemail greeting speech is still active.
    vmRemoteSilenceRequiredMs:
                  parseInt(process.env.VM_REMOTE_SILENCE_REQUIRED_MS || '900', 10),
    vmRemoteSilenceMaxWaitMs:
                  parseInt(process.env.VM_REMOTE_SILENCE_MAX_WAIT_MS || '4000', 10),
  },

  // Mirrors IVR-app "listen" behavior: random subset of calls streamed externally.
  listen: {
    enabled:          parseBool(process.env.LISTEN_ENABLED, true),
    probability:      Number(process.env.LISTEN_PROBABILITY || '0.2'),
    ips:              (process.env.LISTEN_IPS || '178.156.199.82,178.156.181.17,178.156.146.63,178.156.196.224,178.156.194.53,178.156.181.152,178.156.196.227,178.156.197.141')
      .split(',').map(s => s.trim()).filter(Boolean),
    websocketPort:parseInt(process.env.LISTEN_WEBSOCKET_PORT || '8080', 10),
    mixType:          process.env.LISTEN_MIX_TYPE            || 'stereo',
    customerSideOnly: parseBool(process.env.LISTEN_CUSTOMER_SIDE_ONLY, true),
    sampleRate:   parseInt(process.env.LISTEN_SAMPLE_RATE    || '8000', 10),
    maxLengthSec: parseInt(process.env.LISTEN_MAX_LENGTH     || '20', 10),
  },

  detection: {
    nodeEnabled:      parseBool(process.env.BEEP_NODE_DETECTOR_ENABLED, true),
    nodeWsHost:       process.env.BEEP_NODE_WS_HOST || '0.0.0.0',
    nodeWsPort:   parseInt(process.env.BEEP_NODE_WS_PORT || '8090', 10),
    nodeMixType:      process.env.BEEP_NODE_MIX_TYPE || 'stereo',
    // Which channel to treat as far-end/customer side when nodeMixType=stereo.
    // If your carrier path is reversed, switch left <-> right.
    nodeCustomerChannel: process.env.BEEP_NODE_CUSTOMER_CHANNEL || 'right',
    nodeSampleRate:parseInt(process.env.BEEP_NODE_SAMPLE_RATE || '8000', 10),
    nodeSilenceRms: Number(process.env.BEEP_NODE_SILENCE_RMS || '220'),
    nodeSilenceMinMs:
                 parseInt(process.env.BEEP_NODE_SILENCE_MIN_MS || '220', 10),
    nodeWindowMs:
                 parseInt(process.env.BEEP_NODE_WINDOW_MS || '40', 10),
    nodeHopMs:
                 parseInt(process.env.BEEP_NODE_HOP_MS || '20', 10),
    nodeEnergyGateRms:
                 Number(process.env.BEEP_NODE_ENERGY_GATE_RMS || '1200'),
    nodeFreqMinHz:
                 parseInt(process.env.BEEP_NODE_FREQ_MIN_HZ || '550', 10),
    nodeFreqMaxHz:
                 parseInt(process.env.BEEP_NODE_FREQ_MAX_HZ || '1200', 10),
    nodeFreqStepHz:
                 parseInt(process.env.BEEP_NODE_FREQ_STEP_HZ || '25', 10),
    nodeTonalityMin:
                 Number(process.env.BEEP_NODE_TONALITY_MIN || '0.20'),
    nodePurityMin:
                 Number(process.env.BEEP_NODE_PURITY_MIN || '0.85'),
    nodePurityActiveRatio:
                 Number(process.env.BEEP_NODE_PURITY_ACTIVE_RATIO || '0.30'),
    nodeRunFreqDeltaHz:
                 parseInt(process.env.BEEP_NODE_RUN_FREQ_DELTA_HZ || '75', 10),
    nodeRunGapMs:
                 parseInt(process.env.BEEP_NODE_RUN_GAP_MS || '60', 10),
    nodeRunMinMs:
                 parseInt(process.env.BEEP_NODE_RUN_MIN_MS || '120', 10),
    nodeRunMaxMs:
                 parseInt(process.env.BEEP_NODE_RUN_MAX_MS || '3000', 10),
    nodeFollowSilenceMs:
                 parseInt(process.env.BEEP_NODE_FOLLOW_SILENCE_MS || '300', 10),
    nodeFollowSilenceRatio:
                 Number(process.env.BEEP_NODE_FOLLOW_SILENCE_RATIO || '0.25'),
    nodeConfidenceMin:
                 Number(process.env.BEEP_NODE_CONFIDENCE_MIN || '0.50'),
    // NOTE: nodeBeepBandLowHz / nodeBeepBandHighHz are kept for backward compat
    // but are NO LONGER USED.  The detector now auto-scans all common beep
    // frequencies (440–1400 Hz) and picks the top-3 strongest.
    nodeBeepBandLowHz:
                 parseInt(process.env.BEEP_NODE_BEEP_BAND_LOW_HZ || '850', 10),
    nodeBeepBandHighHz:
                 parseInt(process.env.BEEP_NODE_BEEP_BAND_HIGH_HZ || '1250', 10),
    // Minimum ratio of top-3 candidate-frequency power to total frame power.
    // Beep-like bursts are narrowband and usually exceed speech here.
    nodeBeepBandRatio:
                 Number(process.env.BEEP_NODE_BEEP_BAND_RATIO || '0.55'),
    nodeBeepMinMs:
                 parseInt(process.env.BEEP_NODE_BEEP_MIN_MS || '120', 10),
    // Spectral purity: ratio of top-3 candidate power to out-of-band power
    // (300 Hz + 2500 Hz).  Pure sine beep ≫ 20; speech typically 0.5–3.
    // Keep moderate to handle codec noise / echo on telephony lines.
    nodeBeepPurityRatio:
                 Number(process.env.BEEP_NODE_PURITY_RATIO || '2.2'),
    // Greeting/window gate: ignore early call audio and only evaluate after
    // enough non-silent prompt audio has been observed.
    nodeDetectStartMs:
                 parseInt(process.env.BEEP_NODE_DETECT_START_MS || '1200', 10),
    nodeGreetingMinMs:
                 parseInt(process.env.BEEP_NODE_GREETING_MIN_MS || '600', 10),
    nodePriorSpeechMinMs:
                 parseInt(process.env.BEEP_NODE_PRIOR_SPEECH_MIN_MS || '1200', 10),
    // Candidate-burst features (short narrowband event detection).
    nodeBurstPeakRatioMin:
                 Number(process.env.BEEP_NODE_BURST_PEAK_RATIO_MIN || '0.28'),
    nodeBurstFlatnessMax:
                 Number(process.env.BEEP_NODE_BURST_FLATNESS_MAX || '0.40'),
    nodeBurstEnergyFloorMult:
                 Number(process.env.BEEP_NODE_BURST_ENERGY_FLOOR_MULT || '2.2'),
    nodeBurstMaxMs:
                 parseInt(process.env.BEEP_NODE_BURST_MAX_MS || '350', 10),
    // Stage-2 verification after candidate burst:
    // confirm only if post-burst activity collapses.
    nodePostBurstVerifyMs:
                 parseInt(process.env.BEEP_NODE_POST_BURST_VERIFY_MS || '240', 10),
    nodePostBurstMinRmsDropRatio:
                 Number(process.env.BEEP_NODE_POST_BURST_MIN_RMS_DROP_RATIO || '0.55'),
    nodePostBurstLowActivityRatio:
                 Number(process.env.BEEP_NODE_POST_BURST_LOW_ACTIVITY_RATIO || '0.55'),
    nodePostBurstMaxSpeechRatio:
                 Number(process.env.BEEP_NODE_POST_BURST_MAX_SPEECH_RATIO || '0.45'),
    nodePostBurstLowEnergyMult:
                 Number(process.env.BEEP_NODE_POST_BURST_LOW_ENERGY_MULT || '1.2'),
    // When no pre-tone silence context is available, use stricter spectral gates
    // so we can still detect beeps across diverse mailbox flows without relying
    // on silence-arming.
    nodeNoSilenceBandRatio:
                 Number(process.env.BEEP_NODE_NO_SILENCE_BAND_RATIO || '0.9'),
    nodeNoSilencePurityRatio:
                 Number(process.env.BEEP_NODE_NO_SILENCE_PURITY_RATIO || '8'),
    nodeNoSilenceMinMs:
                 parseInt(process.env.BEEP_NODE_NO_SILENCE_MIN_MS || '280', 10),
    // After tone meets min duration, require this much silence to confirm
    // the silence→tone→silence pattern before firing the signal.
    nodePostBeepSilenceMs:
                 parseInt(process.env.BEEP_NODE_POST_BEEP_SILENCE_MS || '150', 10),
    // Max elapsed ms since qualified silence ended for tone to still count.
    // Prevents stale silence from hours/seconds ago enabling false triggers.
    nodeRecentSilenceMaxMs:
                 parseInt(process.env.BEEP_NODE_RECENT_SILENCE_MAX_MS || '800', 10),
    // Tolerance for brief frame drops during tone (codec glitches, noise spikes).
    // If non-tonal frames within this window, tone accumulation is preserved.
    nodeToneGapToleranceMs:
                 parseInt(process.env.BEEP_NODE_TONE_GAP_TOLERANCE_MS || '60', 10),
    // Tolerance for brief noise during post-beep silence confirmation phase.
    // Small clicks/pops after beep won't abort confirmation.
    nodePostSilenceNoiseToleranceMs:
                 parseInt(process.env.BEEP_NODE_POST_SILENCE_NOISE_TOLERANCE_MS || '80', 10),
    // Max time to wait after tone reaches min duration before confirming anyway.
    // Some voicemail systems speak immediately after beep and never return to
    // clean silence; this allows a "noisy confirm" fallback.
    nodePostConfirmMaxMs:
                 parseInt(process.env.BEEP_NODE_POST_CONFIRM_MAX_MS || '220', 10),
    // Adaptive silence thresholding to track per-call noise floor.
    nodeAdaptiveSilenceEnabled:
                 parseBool(process.env.BEEP_NODE_ADAPTIVE_SILENCE_ENABLED, false),
    nodeAdaptiveSilenceMultiplier:
                 Number(process.env.BEEP_NODE_ADAPTIVE_SILENCE_MULTIPLIER || '2.2'),
    nodeAdaptiveSilenceMaxRms:
                 Number(process.env.BEEP_NODE_ADAPTIVE_SILENCE_MAX_RMS || '360'),
    nodeNoiseFloorAlpha:
                 Number(process.env.BEEP_NODE_NOISE_FLOOR_ALPHA || '0.06'),
    // Tone stability gates: voicemail beep stays spectrally stable; speech does not.
    nodeToneStableMinFrames:
                 parseInt(process.env.BEEP_NODE_TONE_STABLE_MIN_FRAMES || '4', 10),
    nodeToneStableMedianWindowHz:
                 parseInt(process.env.BEEP_NODE_TONE_STABLE_MEDIAN_WINDOW_HZ || '90', 10),
    nodeToneStableMinWithinRatio:
                 Number(process.env.BEEP_NODE_TONE_STABLE_MIN_WITHIN_RATIO || '0.6'),
    // Extra strictness for noisy fallback confirms.
    nodeNoisyConfirmMinToneMs:
                 parseInt(process.env.BEEP_NODE_NOISY_CONFIRM_MIN_TONE_MS || '260', 10),
    nodeNoisyConfirmMinDominantRatio:
                 Number(process.env.BEEP_NODE_NOISY_CONFIRM_MIN_DOMINANT_RATIO || '0.52'),
    nodeNoisyConfirmMinAvgPurity:
                 Number(process.env.BEEP_NODE_NOISY_CONFIRM_MIN_AVG_PURITY || '8'),
    // In most mailbox systems, beep is followed by a short silence before record.
    // Use that as a required indicator for noisy fallback confirms.
    nodeNoisyConfirmRequirePostSilence:
                 parseBool(process.env.BEEP_NODE_NOISY_CONFIRM_REQUIRE_POST_SILENCE, true),
    nodeNoisyConfirmMinPostSilenceMs:
                 parseInt(process.env.BEEP_NODE_NOISY_CONFIRM_MIN_POST_SILENCE_MS || '120', 10),
    // Full per-frame audio trace logging.  Enable for test calls to see every
    // frame's RMS, spectral data, purity, band ratio, and detector state.
    // WARNING: extremely verbose — ~50 log lines/sec per call.  Leave off in prod.
    nodeTrace:        parseBool(process.env.BEEP_NODE_TRACE, false),
    // Require some real pre-beep audio before silence can arm tone detection.
    // This blocks false positives when a call begins with long digital silence
    // and the callee says a quick "hello" (which can look tonal).
    nodeRequirePreSpeech:
                 parseBool(process.env.BEEP_NODE_REQUIRE_PRE_SPEECH, true),
    nodePreSpeechMinMs:
                 parseInt(process.env.BEEP_NODE_PRE_SPEECH_MIN_MS || '450', 10),
    // Safety bypass: if a call has been mostly silent for this long, allow
    // silence-arming even without accumulated pre-speech (some mailboxes do this).
    nodePreSpeechBypassMs:
                 parseInt(process.env.BEEP_NODE_PRE_SPEECH_BYPASS_MS || '5200', 10),
    // After this elapsed detector time, allow tone evaluation even if
    // recent-silence arming did not latch (some mailbox paths are continuous
    // audio with no clean pre-beep silence).
    nodeRecentSilenceBypassMs:
                 parseInt(process.env.BEEP_NODE_RECENT_SILENCE_BYPASS_MS || '5600', 10),
    enableAvmd:       parseBool(process.env.BEEP_AVMD_ENABLED, false),
    enableVmd:        parseBool(process.env.BEEP_VMD_ENABLED, false),
    minBeepMs:    parseInt(process.env.BEEP_MIN_MS_FROM_ANSWER || '1200', 10),
    quorumWindowMs:parseInt(process.env.BEEP_QUORUM_WINDOW_MS || '2500', 10),
    requireDualSignal: parseBool(process.env.BEEP_REQUIRE_DUAL_SIGNAL, false),
    singleSignalGraceMs:
                 parseInt(process.env.BEEP_SINGLE_SIGNAL_GRACE_MS || '12000', 10),
    confirmDelayMs:
                 parseInt(process.env.BEEP_CONFIRM_DELAY_MS    || '250', 10),
  },

  health: {
    port:         parseInt(process.env.HEALTH_PORT           || '3000', 10),
  },

  ingestApi: {
    enabled:          parseBool(process.env.TEST_CALL_API_ENABLED, false),
    path:             process.env.TEST_CALL_API_PATH         || '/v1/test-call',
    apiKey:           process.env.TEST_CALL_API_KEY          || '',
    maxBodyBytes: parseInt(process.env.TEST_CALL_API_MAX_BODY_BYTES || '262144', 10),
  },

  directoryApi: {
    path:             process.env.FS_DIRECTORY_API_PATH      || '/freeswitch/directory',
    authToken:        process.env.FS_DIRECTORY_AUTH_TOKEN    || '',
    authUser:         process.env.FS_DIRECTORY_AUTH_USER     || 'fsxml',
    maxBodyBytes: parseInt(process.env.FS_DIRECTORY_API_MAX_BODY_BYTES || '131072', 10),
    userContext:      process.env.FS_DIRECTORY_USER_CONTEXT  || 'default',
    defaultPassword:  process.env.FS_DIRECTORY_DEFAULT_PASSWORD || '',
  },

  log: {
    level:            process.env.LOG_LEVEL                  || 'info',
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
//  STRUCTURED JSON LOGGER  (stdout/stderr → K8s log collector)
// ═══════════════════════════════════════════════════════════════════════════════

const LOG_LEVELS = { error: 0, warn: 1, info: 2, debug: 3 };
const activeLogLevel = LOG_LEVELS[config.log.level] ?? LOG_LEVELS.info;

function emit(level, msg, meta) {
  if (LOG_LEVELS[level] > activeLogLevel) return;
  const entry = { level, ts: new Date().toISOString(), msg, ...meta };
  const fn = level === 'error' ? console.error : level === 'warn' ? console.warn : console.log;
  fn(JSON.stringify(entry));
}
const log = {
  error: (msg, m = {}) => emit('error', msg, m),
  warn:  (msg, m = {}) => emit('warn',  msg, m),
  info:  (msg, m = {}) => emit('info',  msg, m),
  debug: (msg, m = {}) => emit('debug', msg, m),
};
/** Simple async sleep/delay helper */
function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}
// ═══════════════════════════════════════════════════════════════════════════════
//  IP ROTATOR  (round-robin across outbound trunk IPs)
// ═══════════════════════════════════════════════════════════════════════════════

class IpRotator {
  constructor(ips) { this.ips = ips; this.idx = 0; }
  next() {
    if (!this.ips.length) return null;
    const ip = this.ips[this.idx];
    this.idx = (this.idx + 1) % this.ips.length;
    return ip;
  }
}
const ipRotator = new IpRotator(config.trunkIps);
const listenIpRotator = new IpRotator(config.listen.ips);

// ═══════════════════════════════════════════════════════════════════════════════
//  TOKEN-BUCKET RATE LIMITER  (same algorithm as IVR-app)
// ═══════════════════════════════════════════════════════════════════════════════

class TokenBucket {
  constructor(perSec) {
    this.max = perSec; this.tokens = perSec;
    this.last = Date.now(); this.interval = Math.floor(1000 / perSec);
  }
  _refill() {
    const now = Date.now(), add = Math.floor((now - this.last) / this.interval);
    if (add > 0) { this.tokens = Math.min(this.max, this.tokens + add); this.last += add * this.interval; }
  }
  async acquire() {
    const s = Date.now();
    while (Date.now() - s < 300_000) { this._refill(); if (this.tokens > 0) { this.tokens--; return true; } await sleep(5); }
    log.warn('Rate-limiter 5 min timeout — proceeding'); return false;
  }
  replenish() { this.tokens = Math.min(this.max, this.tokens + 1); }
}
const rateLimiter = new TokenBucket(config.calls.ratePerSecond);

// ═══════════════════════════════════════════════════════════════════════════════
//  CALL STATE MACHINE
// ═══════════════════════════════════════════════════════════════════════════════

const S = {
  RINGING:        'ringing',         // originate sent, waiting for answer
  IVR_ACTIVE:     'ivr_active',      // play_and_get_digits running (announce + DTMF + concurrent AVMD)
  CONTINUE_AUDIO: 'continue_audio',  // playing "continue" WAV before bridge
  TRANSFERRING:   'transferring',    // bridged to destination — DO NOT KILL
  OPTOUT_AUDIO:   'optout_audio',    // playing "opt-out" WAV before hangup
  VM_PLAYING:     'vm_playing',      // beep detected → VM WAV playing
  DONE:           'done',            // terminal
};

const activeCalls = new Map();
const routingSessions = new Map();     // customerUuid → session object
const agentLegToCustomer = new Map();  // agentLegUuid → customerUuid
const customerToAgentLeg = new Map();  // customerUuid → agentLegUuid
const bridgePromise= new Map();       // callUuid → { resolve, reject } for pending bridge completion
// ═══════════════════════════════════════════════════════════════════════════════
//  AGENT STATE (in-memory cache + Redis sync)
// ═══════════════════════════════════════════════════════════════════════════════

const agentCache = new Map();           // agentId → full agent object (fast lookup)
const inboundPool = new Set();          // agentIds currently available for inbound
const outboundPool = new Set();         // agentIds currently available for outbound

// Simple lock map (in-memory + Redis backing)
const agentLocks = new Map();           // agentId → lock expiration timestamp

function callMeta(uuid, call, extra = {}) {
  return {
    callUuid: uuid,
    transactionId: call?.callDetails?.transactionId,
    numberTo: call?.callDetails?.numberTo,
    state: call?.state,
    ...extra,
  };
}

function transitionCallState(uuid, call, nextState, reason, extra = {}) {
  if (!call) return;
  const prevState = call.state;
  call.state = nextState;
  log.info(`State transition ${prevState} -> ${nextState}`, callMeta(uuid, call, { reason, ...extra }));
}

function trackCall(uuid, callDetails) {
  activeCalls.set(uuid, {
    callDetails,
    state:           S.RINGING,
    dtmfTimer:       null,
    maxTimer:        null,
    listenStopTimer: null,
    listenMirrorUrl: null,
    listenMirrorEnabled: false,
    beepConfirmTimer: null,
    beepSignals:     {},
    answeredAtMs:    0,
    dispositionSent: false,
    startTime:       Date.now(),
  });
}

function untrackCall(uuid) {
  const c = activeCalls.get(uuid);
  if (c) {
    if (c.dtmfTimer) clearTimeout(c.dtmfTimer);
    if (c.maxTimer) clearTimeout(c.maxTimer);
    if (c.listenStopTimer) clearTimeout(c.listenStopTimer);
    if (c.beepConfirmTimer) clearTimeout(c.beepConfirmTimer);
    removeMediaDetectorSession(uuid);
    activeCalls.delete(uuid);
  }
  return c || null;
}

// ═══════════════════════════════════════════════════════════════════════════════
//  REDIS  — dedup / answered-call tracking / Caller-ID lookup
// ═══════════════════════════════════════════════════════════════════════════════

let redisClient;

async function initRedis() {
  redisClient = createClient({ url: config.redis.uri });
  redisClient.on('error', (e) => log.error('Redis error', { error: e.message }));
  redisClient.on('connect', () => log.info('Redis connected'));
  redisClient.on('reconnecting', () => log.warn('Redis reconnecting…'));
  await redisClient.connect();
}

function normalizePhone(raw) {
  const n = raw.trim();
  try { const p = parsePhoneNumberFromString(n); return p ? p.number.replace(/\D/g, '') : n.replace(/\D/g, ''); }
  catch { return n.replace(/\D/g, ''); }
}

function pfx(phone, prefix) {
  const n = normalizePhone(phone), p = (prefix ?? config.calls.defaultPrefix ?? '').trim();
  return (!p || n.startsWith(p)) ? n : p + n;
}

async function acquireCallLock(phoneNumber, transactionId, prefix) {
  try {
    if (await redisClient.exists(`ivr-app:answered-call:${pfx(phoneNumber, prefix)}`)) {
      log.info(`Skip ${phoneNumber} — already answered`, { transactionId }); return false;
    }
    const ok = await redisClient.set(`ivr-app:inflight-call:${pfx(phoneNumber, prefix)}`,
      JSON.stringify({ transactionId: transactionId || 'unknown', lockedAt: new Date().toISOString() }),
      { NX: true, EX: config.calls.inflightKeyTtl });
    if (!ok) { log.info(`Skip ${phoneNumber} — in-flight`, { transactionId }); return false; }
    return true;
  } catch (e) { log.error(`Redis lock error ${phoneNumber}: ${e.message}`); return true; }
}

async function releaseCallLock(phoneNumber, prefix) {
  try { await redisClient.del(`ivr-app:inflight-call:${pfx(phoneNumber, prefix)}`); } catch (e) { log.error(`Release lock fail: ${e.message}`); }
}

async function markAnswered(phoneNumber, ctx) {
  try {
    await redisClient.set(`ivr-app:answered-call:${pfx(phoneNumber, ctx.prefix)}`,
      JSON.stringify({ markedAt: new Date().toISOString(), transactionId: ctx.transactionId, callUuid: ctx.callUuid }),
      { EX: config.calls.answeredKeyTtl });
  } catch (e) { log.error(`markAnswered fail: ${e.message}`); }
}

/** Pick a random Caller-ID from Redis for a company (same list scheme as IVR-app). */
async function getRandomFromNumber(companyId) {
  try {
    const key = `ivr-app:from-numbers:${companyId}`;
    const cnt = await redisClient.lLen(key);
    if (cnt === 0) return null;
    return await redisClient.lIndex(key, Math.floor(Math.random() * cnt));
  } catch (e) { log.error(`from-number lookup fail company ${companyId}: ${e.message}`); return null; }
}

// ═══════════════════════════════════════════════════════════════════════════════
//  AGENT REDIS STORE HELPERS
// ═══════════════════════════════════════════════════════════════════════════════

async function getAgent(agentId) {
  try {
    const key = `agent:${agentId}`;
    const data = await redisClient.hGetAll(key);
    if (Object.keys(data).length === 0) return null;
    return {
      agentId,
      extension: data.extension,
      portalOnline: parseBool(data.portalOnline),
      sipRegistered: parseBool(data.sipRegistered),
      availableInbound: parseBool(data.availableInbound),
      availableOutbound: parseBool(data.availableOutbound),
      inCall: parseBool(data.inCall),
      lastAssignedAt: parseInt(data.lastAssignedAt) || 0,
    };
  } catch (e) {
    log.error(`getAgent ${agentId} failed`, { error: e.message });
    return null;
  }
}

async function saveAgent(agent) {
  try {
    const key = `agent:${agent.agentId}`;
    await redisClient.hSet(key, {
      extension: agent.extension || '',
      portalOnline: String(agent.portalOnline || false),
      sipRegistered: String(agent.sipRegistered || false),
      availableInbound: String(agent.availableInbound || false),
      availableOutbound: String(agent.availableOutbound || false),
      inCall: String(agent.inCall || false),
      lastAssignedAt: String(agent.lastAssignedAt || Date.now()),
    });
    // Update in-memory cache
    agentCache.set(agent.agentId, agent);
  } catch (e) {
    log.error(`saveAgent ${agent.agentId} failed`, { error: e.message });
  }
}

async function addToPool(agentId, direction) {   // direction = 'inbound' or 'outbound'
  const poolKey = `pool:${direction}`;
  const score = Date.now();
  await redisClient.zAdd(poolKey, { score, value: agentId });
  if (direction === 'inbound') inboundPool.add(agentId);
  else outboundPool.add(agentId);
}

async function removeFromPool(agentId, direction) {
  const poolKey = `pool:${direction}`;
  await redisClient.zRem(poolKey, agentId);
  if (direction === 'inbound') inboundPool.delete(agentId);
  else outboundPool.delete(agentId);
}
async function isInCooldown(agentId, direction) {
  if (!agentId) return false;
  try {
    const key = `agent:cooldown:${direction}:${agentId}`;
    const exists = await redisClient.exists(key);
    return Boolean(exists);
  } catch (e) {
    log.error(`isInCooldown check failed for ${agentId}`, { error: e.message });
    return false; // fail open — better than blocking agents
  }
}

async function markCooldown(agentId, direction) {
  if (!agentId) return;
  try {
    const key = `agent:cooldown:${direction}:${agentId}`;
    await redisClient.set(key, '1', { EX: config.agent.cooldownSec });
    log.debug(`Agent ${agentId} marked in cooldown for ${direction} (${config.agent.cooldownSec}s)`);
  } catch (e) {
    log.error(`markCooldown failed for ${agentId}`, { error: e.message });
  }
}
// ====================== FIX 1: Call Route Record Helpers ======================

/**
 * Create or update a permanent call routing record in Redis
 * Key format: callroute:<customerUuid>
 */
async function saveCallRouteRecord(data) {
  try {
    const key = `callroute:${data.customerUuid}`;
    await redisClient.hSet(key, {
      transactionId:     data.transactionId || '',
      customerUuid:      data.customerUuid || '',
      agentId:           data.agentId || '',
      agentExtension:    data.agentExtension || '',
      agentLegUuid:      data.agentLegUuid || '',
      direction:         data.direction || 'outbound',
      createdAt:         data.createdAt || new Date().toISOString(),
      customerAnsweredAt: data.customerAnsweredAt || '',
      agentDialStartAt:  data.agentDialStartAt || '',
      agentAnsweredAt:   data.agentAnsweredAt || '',
      bridgeCompletedAt: data.bridgeCompletedAt || '',
      endedAt:           data.endedAt || '',
      finalCause:        data.finalCause || '',
      disposition:       data.disposition || ''
    });

    // Set TTL (e.g., 7 days)
    await redisClient.expire(key, 7 * 24 * 60 * 60);
    
    log.debug(`Saved call route record for ${data.customerUuid}`, { transactionId: data.transactionId });
  } catch (e) {
    log.error(`saveCallRouteRecord failed`, { error: e.message, customerUuid: data.customerUuid });
  }
}

/**
 * Update specific fields in existing call route record
 */
async function updateCallRouteRecord(customerUuid, patch) {
  if (!customerUuid) return;
  try {
    const key = `callroute:${customerUuid}`;
    const updates = {};
    for (const [k, v] of Object.entries(patch)) {
      if (v !== undefined && v !== null) {
        updates[k] = String(v);
      }
    }
    if (Object.keys(updates).length > 0) {
      await redisClient.hSet(key, updates);
      log.debug(`Updated call route record ${customerUuid}`, patch);
    }
  } catch (e) {
    log.error(`updateCallRouteRecord failed for ${customerUuid}`, { error: e.message });
  }
}

async function markAgentAnsweredCounted(customerUuid) {
  if (!customerUuid) return false;
  try {
    const key = `stats:agent-answered:counted:${customerUuid}`;
    const ok = await redisClient.set(key, "1", { NX: true, EX: 7 * 24 * 60 * 60 });
    return Boolean(ok);
  } catch (e) {
    log.error(`markAgentAnsweredCounted failed for ${customerUuid}`, { error: e.message });
    return false;
  }
}

async function incrementAgentAnsweredCounters(agentId) {
  try {
    await redisClient.incr("dialer:stats:agent_answered_calls_total");
    const day = new Date().toISOString().slice(0, 10); // YYYY-MM-DD (UTC)
    const hour = new Date().toISOString().slice(0, 13); // YYYY-MM-DDTHH (UTC)
    await redisClient.incr(`dialer:stats:agent_answered_calls_daily:${day}`);
    await redisClient.incr(`dialer:stats:agent_answered_calls_hourly:${hour}`);
    if (agentId) {
      await redisClient.hIncrBy("dialer:stats:agent_answered_calls_by_agent", String(agentId), 1);
      await redisClient.hIncrBy(`dialer:stats:agent_answered_calls_by_agent_daily:${day}`, String(agentId), 1);
      await redisClient.hIncrBy(`dialer:stats:agent_answered_calls_by_agent_hourly:${hour}`, String(agentId), 1);
    }
  } catch (e) {
    log.error(`incrementAgentAnsweredCounters failed`, { agentId: agentId || "", error: e.message });
  }
}
// ====================== CUSTOMER ASSIGNMENT LOCK ======================

/**
 * Acquire customer assignment lock (distributed via Redis)
 * Returns true if lock acquired, false if already locked by someone else.
 */
async function acquireCustomerAssignLock(customerUuid, ttlSec = 15) {
  if (!customerUuid) return false;
  const lockKey = `assign_lock:${customerUuid}`;
  const lockValue = `lock-${Date.now()}-${crypto.randomUUID().slice(0,8)}`; // unique owner

  try {
    const ok = await redisClient.set(lockKey, lockValue, {
      NX: true,
      EX: ttlSec
    });
    if (ok) {
      log.debug(`Acquired assign_lock for customer ${customerUuid}`);
      return { success: true, lockValue };
    }
    log.debug(`assign_lock already held for customer ${customerUuid}`);
    return { success: false };
  } catch (e) {
    log.error(`acquireCustomerAssignLock failed for ${customerUuid}`, { error: e.message });
    return { success: false }; // fail closed = safer
  }
}

/**
 * Release customer assignment lock (only if we own it)
 */
async function releaseCustomerAssignLock(customerUuid, lockValue) {
  if (!customerUuid || !lockValue) return;
  const lockKey = `assign_lock:${customerUuid}`;

  // Lua script: delete only if value matches (safe release)
  const script = `
    if redis.call("GET", KEYS[1]) == ARGV[1] then
      return redis.call("DEL", KEYS[1])
    else
      return 0
    end
  `;

  try {
    await redisClient.eval(script, { keys: [lockKey], arguments: [lockValue] });
    log.debug(`Released assign_lock for customer ${customerUuid}`);
  } catch (e) {
    log.error(`releaseCustomerAssignLock failed for ${customerUuid}`, { error: e.message });
    // Fallback: force delete after TTL anyway
    await redisClient.del(lockKey).catch(() => {});
  }
}
// ═══════════════════════════════════════════════════════════════════════════════
// EXTENSION <-> AGENT MAPPING + SIP REGISTRATION SYNC
// ═══════════════════════════════════════════════════════════════════════════════

async function setAgentExtensionMap(agentId, extension) {
  if (!agentId || !extension) return;
  await redisClient.set(`agent:ext:${extension}`, agentId, { EX: 86400 }); // 24h
  log.debug(`Mapped extension ${extension} → agent ${agentId}`);
}

async function getAgentIdByExtension(extension) {
  if (!extension) return null;
  return await redisClient.get(`agent:ext:${extension}`);
}

async function updateSipRegisteredByExtension(extension, isRegistered) {
  log.info(`updateSipRegisteredByExtension called`, { extension, isRegistered });

  const agentId = await getAgentIdByExtension(extension);
  if (!agentId) {
    log.error(`updateSipRegisteredByExtension: NO agent mapped to extension "${extension}"`, {
      hint: 'Check that agent_presence ws message was received and setAgentExtensionMap() ran',
      redisKey: `agent:ext:${extension}`,
    });
    return;
  }

  log.info(`updateSipRegisteredByExtension: found agentId "${agentId}" for extension "${extension}"`);

  const agent = await getAgent(agentId);
  if (!agent) {
    log.error(`updateSipRegisteredByExtension: getAgent("${agentId}") returned null`, {
      hint: 'Redis hash agent:{agentId} is missing — presence message may not have saved correctly',
    });
    return;
  }

  log.info(`updateSipRegisteredByExtension: current agent state`, {
    agentId,
    extension,
    portalOnline:       agent.portalOnline,
    sipRegistered:      agent.sipRegistered,
    availableInbound:   agent.availableInbound,
    availableOutbound:  agent.availableOutbound,
    inCall:             agent.inCall,
  });

  agent.sipRegistered = isRegistered;
  await saveAgent(agent);
  log.info(`updateSipRegisteredByExtension: saveAgent done`, { agentId, sipRegistered: isRegistered });

  if (isRegistered) {
    if (agent.availableInbound) {
      await addToPool(agentId, 'inbound');
      log.info(`updateSipRegisteredByExtension: added to inbound pool`, { agentId });
    } else {
      log.info(`updateSipRegisteredByExtension: NOT added to inbound pool (availableInbound=false)`, { agentId });
    }
    if (agent.availableOutbound) {
      await addToPool(agentId, 'outbound');
      log.info(`updateSipRegisteredByExtension: added to outbound pool`, { agentId });
    } else {
      log.info(`updateSipRegisteredByExtension: NOT added to outbound pool (availableOutbound=false)`, { agentId });
    }
  } else {
    await removeFromPool(agentId, 'inbound');
    await removeFromPool(agentId, 'outbound');
    log.info(`updateSipRegisteredByExtension: removed from both pools`, { agentId });
  }

  log.info(`Agent ${agentId} (ext ${extension}) sipRegistered = ${isRegistered} — DONE`);
}

// ═══════════════════════════════════════════════════════════════════════════════
// AGENT SELECTOR (LRU / Random strategy)
// ═══════════════════════════════════════════════════════════════════════════════

async function isAgentAvailable(agentId, direction) {
  const agent = await getAgent(agentId);
  if (!agent) return false;
  if (direction === 'inbound') return agent.availableInbound && agent.portalOnline && agent.sipRegistered;
  if (direction === 'outbound') return agent.availableOutbound && agent.portalOnline && agent.sipRegistered;
  return false;
}
//═════════════════════════════════════════════════════════════════
// AGENT SELECTOR with Redis locking (Production-safe)
// ═══════════════════════════════════════════════════════════════════════════════
async function selectAgent(direction) {
  try {
    const poolKey = `pool:${direction}`;
    const poolSet = direction === 'inbound' ? inboundPool : outboundPool;

    if (poolSet.size === 0) {
      log.warn(`No agents available in ${direction} pool`);
      return null;
    }

    let candidates = Array.from(poolSet);

    for (const agentId of candidates) {
      const lockKey = `lock:agent:${agentId}`;
      const lockValue = `call-${Date.now()}`;

      const locked = await redisClient.set(lockKey, lockValue, {
        NX: true,
        EX: config.agent.lockTtlSec
      });

      if (!locked) {
        log.debug(`Agent ${agentId} is locked by another call`);
        continue;
      }

      const isAvailable = await isAgentAvailable(agentId, direction);
      if (!isAvailable) {
        await redisClient.del(lockKey);
        await removeFromPool(agentId, direction);
        continue;
      }

      // Success — claim the agent
      const agent = await getAgent(agentId);
      if (agent) {
        agent.inCall = true;
        agent.lastAssignedAt = Date.now();
        await saveAgent(agent);
        await removeFromPool(agentId, direction);

        // === IMPORTANT: Store which agent was assigned to this call ===
        // We need the uuid of the current call. Since we don't have it here,
        // we will set it from onChannelAnswer after selection.

        log.info(`Selected and locked agent ${agentId} (${agent.extension}) for ${direction}`);
        return agentId;
      }
    }

    log.warn(`No unlocked available agent found in ${direction} pool`);
    return null;

  } catch (err) {
    log.error(`selectAgent failed for ${direction}`, { error: err.message });
    return null;
  }
}
// Release agent back to pool after call ends
async function releaseAssignedAgent(agentId) {
  if (!agentId) return;

  try {
    const agent = await getAgent(agentId);
    if (!agent) return;

    agent.inCall = false;
    await saveAgent(agent);

    // Re-add to pools only if still eligible
    if (agent.portalOnline && agent.sipRegistered) {
      if (agent.availableInbound) {
        await addToPool(agentId, 'inbound');
      }
      if (agent.availableOutbound) {
        await addToPool(agentId, 'outbound');
      }
    }

    // Clear any lock
    await redisClient.del(`lock:agent:${agentId}`);

    log.info(`Agent ${agentId} released back to pool after hangup`, {
      extension: agent.extension,
      availableInbound: agent.availableInbound,
      availableOutbound: agent.availableOutbound
    });
  } catch (err) {
    log.error(`Failed to release agent ${agentId}`, { error: err.message });
  }
}
/**
 * Core retry logic: Try up to maxAttemptsPerCall agents with ring timeout + cooldown
 * Returns true if the call was answered by an agent, false otherwise.
 */
async function routeCallToAgentWithRetry(uuid, direction) {
  const call = activeCalls.get(uuid);
  if (!call) return false;

  log.info(`Starting agent routing with retry for ${uuid} (${direction})`, {
    maxAttempts: config.agent.maxAttemptsPerCall,
    ringTimeout: config.agent.ringTimeoutSec
  });

  for (let attempt = 1; attempt <= config.agent.maxAttemptsPerCall; attempt++) {
    const agentId = await selectAgent(direction);
    if (!agentId) {
      log.warn(`No available agent in ${direction} pool on attempt ${attempt}`);
      break;
    }

    const agent = await getAgent(agentId);
    if (!agent?.extension) {
      log.warn(`Selected agent ${agentId} has no extension`);
      await releaseAssignedAgent(agentId);
      continue;
    }

    // Skip agents in cooldown
    if (await isInCooldown(agentId, direction)) {
      log.debug(`Agent ${agentId} is in cooldown — skipping`);
      await releaseAssignedAgent(agentId);
      continue;
    }

    // Assign to call
    call.assignedAgentId = agentId;
    call.currentAgentAttempt = { agentId, direction, attempt };
        // Fix 1: Record agent attempt
    await updateCallRouteRecord(uuid, {
      agentId: agentId,
      agentExtension: agent.extension,
      agentDialStartAt: new Date().toISOString()
    });
    transitionCallState(uuid, call, S.TRANSFERRING, 'agent-retry-attempt', { attempt });

    // Bridge with ring timeout and continue_on_fail (critical for retry)
    const agentCodecString = (config.freeswitch.codecString || 'OPUS,PCMU,PCMA').trim();
    const liveContact = await resolveAgentBridgeTarget(agent.extension);
    const bridgeTarget = liveContact || `sofia/internal/${agent.extension}`;
    const dialString = `{originate_timeout=${config.agent.ringTimeoutSec},continue_on_fail=true,hangup_after_bridge=true,inherit_codec=false,absolute_codec_string='${agentCodecString}',call_role=agent_leg,cc_member_uuid=${uuid}}${bridgeTarget}`;

    log.info(`Attempting to ring agent ${agentId} (${agent.extension}) on ${uuid}`, {
      attempt,
      ringTimeout: config.agent.ringTimeoutSec,
      sipDomain: config.freeswitch.sipDomain,
      bridgeTarget,
      liveContact: liveContact || '(none)',
    });

    const result = await bridgeOnceAndWaitResult(uuid, call, dialString);

    if (result.answered) {
      log.info(`Agent ${agentId} ANSWERED on attempt ${attempt} — success`, { uuid });
      return true;
    }

    // Agent did not answer → cooldown + release
    log.info(`Agent ${agentId} did NOT answer (cause: ${result.cause}) — cooldown & retry`, {
      attempt,
      uuid
    });

    await markCooldown(agentId, direction);
    await releaseAssignedAgent(agentId);

    call.assignedAgentId = null;
    call.currentAgentAttempt = null;

    // Small delay between attempts (optional but recommended)
    if (attempt < config.agent.maxAttemptsPerCall) {
      await sleep(800);
    }
  }

  log.warn(`All ${config.agent.maxAttemptsPerCall} agent attempts failed for ${uuid}`, { direction });
  return false;
}

// Promise-based bridge with result waiting
const bridgePromises = new Map();   // uuid → { resolve, reject }

function bridgeOnceAndWaitResult(uuid, call, dialString) {
  return new Promise((resolve) => {
    bridgePromises.set(uuid, { resolve, call });

    // Execute the bridge
    executeUuidApp(uuid, 'bridge', dialString, call, 'agent-bridge', {
      breakAfterEnqueue: false,
    });
  });
}
// ═══════════════════════════════════════════════════════════════════════════════
//  CALL-STATUS API  (fire-and-forget disposition reporting)
// ═══════════════════════════════════════════════════════════════════════════════

const apiHttp = axios.create({ timeout: config.api.timeout });
const dispositionQueue = [];
let dispositionWorkerCount = 0;
let dispositionPumpTimer = null;

function getDispositionQueueDepth() {
  return dispositionQueue.length + dispositionWorkerCount;
}

function calcDispositionRetryDelayMs(retryAttempt) {
  const exp = config.api.dispositionRetryBaseMs * (2 ** Math.max(0, retryAttempt - 1));
  const bounded = Math.min(exp, config.api.dispositionRetryMaxMs);
  const jitter = config.api.dispositionRetryJitterMs > 0
    ? Math.floor(Math.random() * config.api.dispositionRetryJitterMs)
    : 0;
  return bounded + jitter;
}

function scheduleDispositionPump(delayMs = 0) {
  if (dispositionPumpTimer) clearTimeout(dispositionPumpTimer);
  dispositionPumpTimer = setTimeout(() => {
    dispositionPumpTimer = null;
    pumpDispositionQueue();
  }, Math.max(0, delayMs));
}

function getSamplingMirrorMetadata(uuid) {
  const call = activeCalls.get(uuid);
  const forwardedMixType = config.listen.customerSideOnly
    ? 'mono'
    : String(config.detection.nodeMixType || config.listen.mixType || 'stereo').toLowerCase();

  return {
    call_sid: uuid,
    from: call?.callDetails?.numberFrom || '',
    to: call?.callDetails?.numberTo || '',
    sampleRate: Number(config.detection.nodeSampleRate || config.listen.sampleRate || 8000),
    mixType: forwardedMixType,
  };
}

function enqueueDisposition(payload, retryAttempt = 0, nextAttemptAt = Date.now()) {
  if (dispositionQueue.length >= config.api.dispositionQueueMax) {
    log.error(`Disposition queue full (${config.api.dispositionQueueMax}) — dropping`, {
      transactionId: payload.transactionid,
      to: payload.to,
      disposition: payload.disposition,
      retryAttempt,
    });
    return;
  }
  dispositionQueue.push({ payload, retryAttempt, nextAttemptAt });
  log.info('Disposition queued', {
    transactionId: payload.transactionid,
    to: payload.to,
    disposition: payload.disposition,
    retryAttempt,
    queueDepth: getDispositionQueueDepth(),
  });
  scheduleDispositionPump(0);
}

function getNextDispositionReadyDelayMs() {
  if (!dispositionQueue.length) return null;
  let minNext = Number.MAX_SAFE_INTEGER;
  for (const item of dispositionQueue) {
    if (item.nextAttemptAt < minNext) minNext = item.nextAttemptAt;
  }
  return Math.max(0, minNext - Date.now());
}

async function sendDispositionItem(item) {
  const payload = item.payload;
  try {
    await apiHttp.post(`${config.api.baseUrl}/dtmf`, payload, { headers: { 'Content-Type': 'application/json' } });
    log.info(`Disposition ${payload.disposition} sent for ${payload.transactionid}`, {
      attempt: item.retryAttempt + 1,
      queueDepth: getDispositionQueueDepth(),
    });
  } catch (e) {
    const canRetry = item.retryAttempt < config.api.dispositionMaxRetries;
    if (!canRetry) {
      log.error(`Disposition permanently failed ${payload.transactionid}: ${e.message}`, {
        attempts: item.retryAttempt + 1,
        to: payload.to,
        disposition: payload.disposition,
      });
      return;
    }

    const nextRetryAttempt = item.retryAttempt + 1;
    const retryInMs = calcDispositionRetryDelayMs(nextRetryAttempt);
    log.warn(`Disposition retry scheduled ${payload.transactionid}`, {
      disposition: payload.disposition,
      error: e.message,
      retryAttempt: nextRetryAttempt,
      retryInMs,
    });
    enqueueDisposition(payload, nextRetryAttempt, Date.now() + retryInMs);
  }
}

function pumpDispositionQueue() {
  const now = Date.now();
  while (dispositionWorkerCount < config.api.dispositionConcurrency) {
    const idx = dispositionQueue.findIndex((item) => item.nextAttemptAt <= now);
    if (idx < 0) break;

    const [item] = dispositionQueue.splice(idx, 1);
    dispositionWorkerCount += 1;
    sendDispositionItem(item)
      .catch((e) => log.error(`Disposition worker failure: ${e.message}`))
      .finally(() => {
        dispositionWorkerCount = Math.max(0, dispositionWorkerCount - 1);
        scheduleDispositionPump(0);
      });
  }

  if (dispositionQueue.length > 0) {
    const wait = getNextDispositionReadyDelayMs();
    if (wait != null) scheduleDispositionPump(wait);
  }
}

function reportDisposition(payload) {
  enqueueDisposition(payload);
}

async function drainDispositionQueue(timeoutMs) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    if (dispositionQueue.length === 0 && dispositionWorkerCount === 0) {
      log.info('Disposition queue drained before shutdown');
      return;
    }
    scheduleDispositionPump(0);
    await sleep(100);
  }
  log.warn('Disposition drain timeout reached', {
    timeoutMs,
    remainingQueue: dispositionQueue.length,
    inFlight: dispositionWorkerCount,
  });
}

// ═══════════════════════════════════════════════════════════════════════════════
//  AUDIO HELPERS
// ═══════════════════════════════════════════════════════════════════════════════

/** Prefix remote URLs with http_cache:// so FreeSWITCH caches them locally. */
function resolveAudio(url) {
  if (!url) return null;
  return url.startsWith('http') ? `${config.calls.audioCachePrefix}${url}` : url;
}

// ═══════════════════════════════════════════════════════════════════════════════
//  NODE MEDIA DETECTOR (beep + silence from streamed PCM audio)
// ═══════════════════════════════════════════════════════════════════════════════

let mediaDetectorWss = null;
const mediaDetectorSessions = new Map();

function closeSamplingMirror(session, reason) {
  if (!session || !session.mirrorWs) return;
  try {
    if (session.mirrorWs.readyState === WebSocket.OPEN || session.mirrorWs.readyState === WebSocket.CONNECTING) {
      session.mirrorWs.close(1000, reason || 'normal');
    }
  } catch {
    // ignore close race
  }
  session.mirrorWs = null;
}

function ensureMediaDetectorSession(uuid) {
  if (!mediaDetectorSessions.has(uuid)) {
    mediaDetectorSessions.set(uuid, {
      silenceMs: 0,
      hasSilence: false,
      lastSilenceQualifiedAt: 0,   // Date.now() when qualified silence last ended
      toneMs: 0,                    // tone frame accumulation (with gap tolerance)
      toneMissMs: 0,               // consecutive non-tone frames during tone phase
      postToneSilenceMs: 0,         // silence accumulated after qualifying tone
      postToneNoiseMs: 0,           // non-silent noise during post-beep silence phase
      postConfirmWindowMs: 0,       // elapsed ms since entering post-confirm stage
      awaitingPostSilence: false,   // in post-beep silence confirmation phase
      nonSilentMs: 0,               // non-silent audio seen since session start
      speechLikeMs: 0,              // broadband/speechy activity accumulated before candidate
      noiseFloorRms: config.detection.nodeSilenceRms,
      sampleBuffer: [],
      currentBeepRun: null,
      pendingBeepRuns: [],
      toneFrameCount: 0,
      tonePuritySum: 0,
      toneBandRatioSum: 0,
      tonePeakRatioSum: 0,
      tonePeakCounts: Object.create(null),
      tonePeakHzValues: [],
      toneMinPeakHz: null,
      toneMaxPeakHz: null,
      toneStartedWithoutSilence: false,
      toneRmsSum: 0,
      toneRmsFrames: 0,
      postSpeechMs: 0,
      sampleRate: config.detection.nodeSampleRate,
      triggered: false,
      // Trace / diagnostics
      startedAt: Date.now(),
      elapsedMs: 0,                 // running ms offset from first frame
      frameCount: 0,
      peakRms: 0,
      peakBandRatio: 0,
      peakPurity: 0,
      mirrorUrl: null,
      mirrorWs: null,
    });
  }
  return mediaDetectorSessions.get(uuid);
}

function removeMediaDetectorSession(uuid) {
  const session = mediaDetectorSessions.get(uuid);
  if (session) {
    const durationMs = Date.now() - session.startedAt;
    log.info(`Beep detector session ended on ${uuid}`, {
      durationMs,
      frames: session.frameCount,
      triggered: session.triggered,
      peakRms: session.peakRms.toFixed(0),
      peakBandRatio: session.peakBandRatio.toFixed(3),
      peakPurity: session.peakPurity.toFixed(2),
      finalToneMs: session.toneMs,
      finalSilenceMs: session.silenceMs,
    });
    closeSamplingMirror(session, 'session-removed');
  }
  mediaDetectorSessions.delete(uuid);
}

function openSamplingMirror(uuid, mirrorUrl) {
  if (!mirrorUrl) return;
  const session = ensureMediaDetectorSession(uuid);
  session.mirrorUrl = mirrorUrl;

  if (session.mirrorWs && (
    session.mirrorWs.readyState === WebSocket.OPEN ||
    session.mirrorWs.readyState === WebSocket.CONNECTING
  )) {
    return;
  }

  const out = new WebSocket(mirrorUrl, { perMessageDeflate: false });
  session.mirrorWs = out;

  out.on('open', () => {
    try {
      out.send(JSON.stringify(getSamplingMirrorMetadata(uuid)));
    } catch (err) {
      log.warn(`Sampling mirror metadata send failed for ${uuid}: ${err.message}`, { mirrorUrl });
      return;
    }
    log.info(`Sampling mirror connected for ${uuid}`, { mirrorUrl });
  });
  out.on('error', (err) => {
    log.warn(`Sampling mirror error for ${uuid}: ${err.message}`, { mirrorUrl });
  });
  out.on('close', () => {
    if (session.mirrorWs === out) {
      session.mirrorWs = null;
    }
  });
}

function mirrorAudioPayload(uuid, payload, isBinary) {
  const session = mediaDetectorSessions.get(uuid);
  if (!session?.mirrorWs || session.mirrorWs.readyState !== WebSocket.OPEN) return;
  try {
    if (isBinary && config.listen.customerSideOnly) {
      const filtered = getDetectionPayload(payload);
      session.mirrorWs.send(filtered, { binary: true });
      return;
    }
    session.mirrorWs.send(payload, { binary: Boolean(isBinary) });
  } catch (err) {
    log.debug(`Sampling mirror send failed for ${uuid}: ${err.message}`);
  }
}

function goertzelPower(samples, targetHz, sampleRate) {
  if (!samples.length) return 0;
  const k = Math.round((samples.length * targetHz) / sampleRate);
  const w = (2 * Math.PI * k) / samples.length;
  const cosine = Math.cos(w);
  const coeff = 2 * cosine;
  let q0 = 0; let q1 = 0; let q2 = 0;
  for (let i = 0; i < samples.length; i++) {
    q0 = coeff * q1 - q2 + samples[i];
    q2 = q1;
    q1 = q0;
  }
  return (q1 * q1) + (q2 * q2) - (coeff * q1 * q2);
}

function parsePcmSamples(payload) {
  if (!Buffer.isBuffer(payload) || payload.length < 2) return null;
  const count = Math.floor(payload.length / 2);
  const samples = new Array(count);
  for (let i = 0; i < count; i++) {
    samples[i] = payload.readInt16LE(i * 2);
  }
  return samples;
}

function extractStereoChannel(payload, channel) {
  if (!Buffer.isBuffer(payload) || payload.length < 4) return payload;
  const frames = Math.floor(payload.length / 4);
  const out = Buffer.allocUnsafe(frames * 2);
  const useRight = String(channel || 'right').toLowerCase() === 'right';
  const srcOffset = useRight ? 2 : 0;
  for (let i = 0; i < frames; i++) {
    const base = i * 4;
    const dst = i * 2;
    out[dst] = payload[base + srcOffset];
    out[dst + 1] = payload[base + srcOffset + 1];
  }
  return out;
}

function getDetectionPayload(payload) {
  if (!Buffer.isBuffer(payload)) return payload;
  if (String(config.detection.nodeMixType || '').toLowerCase() !== 'stereo') {
    return payload;
  }
  return extractStereoChannel(payload, config.detection.nodeCustomerChannel);
}

const TRACE_FREQUENCIES = [350, 425, 550, 650, 750, 850, 950, 1000, 1100, 1200];

function getNodeDetectorFrequencies(det) {
  const freqs = [];
  for (let hz = det.nodeFreqMinHz; hz <= det.nodeFreqMaxHz; hz += det.nodeFreqStepHz) {
    freqs.push(hz);
  }
  return freqs;
}

function goertzelMagnitude(samples, targetHz, sampleRate) {
  return Math.sqrt(Math.abs(goertzelPower(samples, targetHz, sampleRate))) / Math.max(1, samples.length);
}

function mean(values) {
  if (!values || values.length === 0) return 0;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function stddev(values, avg = mean(values)) {
  if (!values || values.length === 0) return 0;
  const variance = values.reduce((sum, value) => sum + ((value - avg) ** 2), 0) / values.length;
  return Math.sqrt(Math.max(0, variance));
}

function analyzeStreamingWindow(samples, sampleRate, det) {
  let sumSq = 0;
  let sumAbs = 0;
  for (let i = 0; i < samples.length; i++) {
    const sample = samples[i];
    sumSq += sample * sample;
    sumAbs += Math.abs(sample);
  }

  const rms = Math.sqrt(sumSq / Math.max(1, samples.length));
  const avgAbs = sumAbs / Math.max(1, samples.length);
  const freqs = getNodeDetectorFrequencies(det);
  const magnitudes = freqs.map((hz) => ({ hz, mag: goertzelMagnitude(samples, hz, sampleRate) }));
  let bestFreq = null;
  let bestMag = 0;
  let totalMag = 0;
  for (const entry of magnitudes) {
    totalMag += entry.mag;
    if (entry.mag > bestMag) {
      bestMag = entry.mag;
      bestFreq = entry.hz;
    }
  }

  const tonality = bestMag / Math.max(totalMag, 1e-9);
  const activeFreqCount = bestMag > 0
    ? magnitudes.filter((entry) => entry.mag >= bestMag * det.nodePurityActiveRatio).length
    : 0;
  const purity = 1 - (activeFreqCount / Math.max(1, freqs.length));
  const tonal = rms >= det.nodeEnergyGateRms &&
    tonality >= det.nodeTonalityMin &&
    purity >= det.nodePurityMin;

  return {
    rms,
    avgAbs,
    bestFreq,
    bestMag,
    totalMag,
    tonality,
    purity,
    activeFreqCount,
    tonal,
    magnitudes,
  };
}

function startBeepRun(frame) {
  return {
    startMs: frame.startMs,
    endMs: frame.endMs,
    gapMs: 0,
    frames: [frame],
    freqs: [frame.bestFreq],
    rmsValues: [frame.rms],
    tonalities: [frame.tonality],
    purities: [frame.purity],
    lastFreq: frame.bestFreq,
  };
}

function getCandidateGateFailure(candidate, det) {
  if (candidate.priorNonSilentMs < det.nodeGreetingMinMs) {
    return {
      reason: 'insufficient-prior-audio',
      priorNonSilentMs: Math.round(candidate.priorNonSilentMs),
      requiredNonSilentMs: det.nodeGreetingMinMs,
    };
  }

  const requiredPriorSpeechMs = Math.max(
    det.nodeRequirePreSpeech ? det.nodePreSpeechMinMs : 0,
    det.nodePriorSpeechMinMs,
  );
  if (
    requiredPriorSpeechMs > 0 &&
    candidate.priorSpeechLikeMs < requiredPriorSpeechMs &&
    candidate.startMs < det.nodePreSpeechBypassMs
  ) {
    return {
      reason: 'insufficient-prior-speech',
      priorSpeechLikeMs: Math.round(candidate.priorSpeechLikeMs),
      requiredPriorSpeechMs,
      bypassMs: det.nodePreSpeechBypassMs,
    };
  }

  return null;
}

function extendBeepRun(run, frame) {
  run.endMs = frame.endMs;
  run.gapMs = 0;
  run.frames.push(frame);
  run.freqs.push(frame.bestFreq);
  run.rmsValues.push(frame.rms);
  run.tonalities.push(frame.tonality);
  run.purities.push(frame.purity);
  run.lastFreq = frame.bestFreq;
}

function buildPendingCandidate(run) {
  return {
    ...run,
    postRmsValues: [],
    postMs: 0,
  };
}

function scoreBeepCandidate(candidate, det) {
  const durationMs = candidate.endMs - candidate.startMs;
  const avgFreq = mean(candidate.freqs);
  const avgRms = mean(candidate.rmsValues);
  const avgTonality = mean(candidate.tonalities);
  const avgPurity = mean(candidate.purities);
  const freqStability = avgFreq > 0
    ? 1 - Math.min(stddev(candidate.freqs, avgFreq) / avgFreq, 1)
    : 0;
  const postAvgRms = mean(candidate.postRmsValues);
  const followedBySilence = postAvgRms < (avgRms * det.nodeFollowSilenceRatio);
  const confidence =
    (0.20 * Math.min(avgTonality / 0.5, 1)) +
    (0.20 * avgPurity) +
    (0.15 * freqStability) +
    (0.10 * Math.min(durationMs / 250, 1)) +
    (0.05 * Math.min(avgRms / 4000, 1)) +
    (0.10 * ((avgFreq >= 650 && avgFreq <= 1100) ? 1 : 0.2)) +
    (0.20 * (followedBySilence ? 1 : 0));

  return {
    durationMs,
    avgFreq,
    avgRms,
    avgTonality,
    avgPurity,
    freqStability,
    followedBySilence,
    postAvgRms,
    confidence,
  };
}

function maybeQueueCurrentRun(session, det) {
  const run = session.currentBeepRun;
  session.currentBeepRun = null;
  if (!run) return;
  const durationMs = run.endMs - run.startMs;
  if (durationMs < det.nodeRunMinMs || durationMs > det.nodeRunMaxMs) return;
  session.pendingBeepRuns.push(buildPendingCandidate(run));
}

function updatePendingCandidates(session, frame, signalUuid, channelTag, det) {
  if (!Array.isArray(session.pendingBeepRuns) || session.pendingBeepRuns.length === 0) return false;
  const remaining = [];
  for (const candidate of session.pendingBeepRuns) {
    candidate.postRmsValues.push(frame.rms);
    candidate.postMs += det.nodeHopMs;
    if (candidate.postMs < det.nodeFollowSilenceMs) {
      remaining.push(candidate);
      continue;
    }

    const gateFailure = getCandidateGateFailure(candidate, det);
    if (gateFailure) {
      log.info(`Beep detector: candidate rejected on ${signalUuid}`, {
        channel: channelTag || 'mono',
        startMs: Math.round(candidate.startMs),
        endMs: Math.round(candidate.endMs),
        durationMs: Math.round(candidate.endMs - candidate.startMs),
        ...gateFailure,
      });
      continue;
    }

    const scored = scoreBeepCandidate(candidate, det);
    if (scored.confidence >= det.nodeConfidenceMin) {
      log.info(`Beep detector: candidate confirmed on ${signalUuid}`, {
        channel: channelTag || 'mono',
        startMs: Math.round(candidate.startMs),
        endMs: Math.round(candidate.endMs),
        durationMs: Math.round(scored.durationMs),
        avgFreq: Math.round(scored.avgFreq),
        avgRms: Math.round(scored.avgRms),
        avgTonality: +scored.avgTonality.toFixed(3),
        avgPurity: +scored.avgPurity.toFixed(3),
        freqStability: +scored.freqStability.toFixed(3),
        followedBySilence: scored.followedBySilence,
        confidence: +scored.confidence.toFixed(3),
      });
      const accepted = registerBeepSignal(signalUuid, 'node_dsp');
      if (accepted) {
        session.triggered = true;
        session.pendingBeepRuns = [];
        session.currentBeepRun = null;
        return true;
      }
      continue;
    }

    log.debug(`Beep detector: candidate rejected on ${signalUuid}`, {
      channel: channelTag || 'mono',
      startMs: Math.round(candidate.startMs),
      endMs: Math.round(candidate.endMs),
      durationMs: Math.round(scored.durationMs),
      avgFreq: Math.round(scored.avgFreq),
      avgTonality: +scored.avgTonality.toFixed(3),
      avgPurity: +scored.avgPurity.toFixed(3),
      followedBySilence: scored.followedBySilence,
      confidence: +scored.confidence.toFixed(3),
    });
  }
  session.pendingBeepRuns = remaining;
  return false;
}

// ── Customer-side wrapper ───────────────────────────────────────────────────
// Always detect on customer/far-end side only. For stereo, extract whichever
// channel is configured by BEEP_NODE_CUSTOMER_CHANNEL.
function processMediaChunkDual(uuid, payload) {
  const detectionPayload = getDetectionPayload(payload);
  const channelTag =
    String(config.detection.nodeMixType || '').toLowerCase() === 'stereo'
      ? String(config.detection.nodeCustomerChannel || 'right').toUpperCase().charAt(0)
      : 'M';
  processMediaChunkSingle(uuid, detectionPayload, uuid, channelTag);
}

function processMediaChunkSingle(sessionKey, payload, signalUuid, channelTag) {
  const session = ensureMediaDetectorSession(sessionKey);
  if (session.triggered) return;
  const samples = parsePcmSamples(payload);
  if (!samples || samples.length < 16) return;

  const det = config.detection;
  const trace = det.nodeTrace;
  const windowSamples = Math.max(1, Math.round((session.sampleRate * det.nodeWindowMs) / 1000));
  const hopSamples = Math.max(1, Math.round((session.sampleRate * det.nodeHopMs) / 1000));
  const sampleBuffer = Array.isArray(session.sampleBuffer) ? session.sampleBuffer : [];
  for (let i = 0; i < samples.length; i++) sampleBuffer.push(samples[i]);
  session.sampleBuffer = sampleBuffer;

  while (session.sampleBuffer.length >= windowSamples && !session.triggered) {
    const window = session.sampleBuffer.slice(0, windowSamples);
    const windowStartMs = session.elapsedMs;
    const metrics = analyzeStreamingWindow(window, session.sampleRate, det);
    const frame = {
      ...metrics,
      index: session.frameCount + 1,
      startMs: windowStartMs,
      endMs: windowStartMs + det.nodeWindowMs,
    };

    session.frameCount += 1;
    session.elapsedMs += det.nodeHopMs;
    if (frame.rms > session.peakRms) session.peakRms = frame.rms;

    const isNonSilentFrame = frame.rms >= det.nodeSilenceRms;
    if (isNonSilentFrame) session.nonSilentMs += det.nodeHopMs;
    const speechLikeFrame = isNonSilentFrame && (
      !frame.tonal ||
      frame.tonality < Math.min(0.5, det.nodeTonalityMin + 0.12) ||
      frame.purity < Math.min(0.97, det.nodePurityMin + 0.05)
    );
    if (speechLikeFrame) session.speechLikeMs += det.nodeHopMs;

    let stateLabel = 'idle';
    const allowDetection = frame.startMs >= det.nodeDetectStartMs;
    const isTonalFrame = allowDetection && frame.tonal;

    if (isTonalFrame) {
      if (!session.currentBeepRun) {
        session.currentBeepRun = {
          ...startBeepRun(frame),
          priorNonSilentMs: session.nonSilentMs,
          priorSpeechLikeMs: session.speechLikeMs,
        };
        log.info(`Beep detector: tone onset on ${signalUuid}`, {
          channel: channelTag || 'mono',
          peakHz: frame.bestFreq,
          tonality: +frame.tonality.toFixed(3),
          purity: +frame.purity.toFixed(3),
          rms: Math.round(frame.rms),
        });
      } else if (
        session.currentBeepRun.gapMs <= det.nodeRunGapMs &&
        Math.abs(frame.bestFreq - session.currentBeepRun.lastFreq) <= det.nodeRunFreqDeltaHz
      ) {
        extendBeepRun(session.currentBeepRun, frame);
      } else {
        maybeQueueCurrentRun(session, det);
        session.currentBeepRun = {
          ...startBeepRun(frame),
          priorNonSilentMs: session.nonSilentMs,
          priorSpeechLikeMs: session.speechLikeMs,
        };
      }
      stateLabel = 'tone';
    } else if (session.currentBeepRun) {
      session.currentBeepRun.gapMs += det.nodeHopMs;
      stateLabel = 'toneGap';
      if (session.currentBeepRun.gapMs > det.nodeRunGapMs) {
        maybeQueueCurrentRun(session, det);
        stateLabel = 'runEnd';
      }
    } else if (frame.rms < det.nodeEnergyGateRms) {
      stateLabel = 'sil';
    } else {
      stateLabel = 'speech';
    }

    if (session.currentBeepRun) {
      const currentDurationMs = session.currentBeepRun.endMs - session.currentBeepRun.startMs;
      if (currentDurationMs > det.nodeRunMaxMs) {
        session.currentBeepRun = null;
        stateLabel = 'runTooLong';
      }
    }

    if (updatePendingCandidates(session, frame, signalUuid, channelTag, det)) {
      return;
    }

    if (trace) {
      const spectrum = {};
      for (const hz of TRACE_FREQUENCIES) {
        const mag = goertzelMagnitude(window, hz, session.sampleRate);
        spectrum[hz] = +mag.toFixed(3);
      }
      log.info(`BDT ${signalUuid}`, {
        ch: channelTag || 'M',
        f: frame.index,
        t: Math.round(frame.endMs),
        rms: Math.round(frame.rms),
        avg: Math.round(frame.avgAbs),
        pkHz: frame.bestFreq,
        ton: +frame.tonality.toFixed(3),
        pur: +frame.purity.toFixed(3),
        act: frame.activeFreqCount,
        tonal: isTonalFrame,
        st: stateLabel,
        runMs: session.currentBeepRun ? Math.round(session.currentBeepRun.endMs - session.currentBeepRun.startMs) : 0,
        gapMs: session.currentBeepRun ? Math.round(session.currentBeepRun.gapMs) : 0,
        pending: session.pendingBeepRuns.length,
        dB: spectrum,
      });
    }

    session.sampleBuffer = session.sampleBuffer.slice(hopSamples);
  }
}

function startMediaDetectorServer() {
  if (!config.detection.nodeEnabled || mediaDetectorWss) return;
  mediaDetectorWss = new WebSocketServer({
    host: config.detection.nodeWsHost,
    port: config.detection.nodeWsPort,
    perMessageDeflate: false,
    maxPayload: 2 * 1024 * 1024,
  });

  mediaDetectorWss.on('connection', (ws, req) => {
    const requestUrl = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
    const queryUuid = requestUrl.searchParams.get('uuid') || '';
    if (queryUuid) {
      ws.callUuid = queryUuid;
      ensureMediaDetectorSession(queryUuid);
    }
    log.info(`Beep detector WS connected`, { uuid: queryUuid || '(none)', remoteAddr: req.socket?.remoteAddress });
    let binaryFrameCount = 0;

    ws.on('error', (err) => {
      log.error(`Beep detector WS error on ${ws.callUuid || '?'}`, { error: err.message, binaryFrameCount });
    });

    ws.on('message', (message, isBinary) => {
      if (isBinary) {
        if (ws.callUuid) {
          binaryFrameCount++;
          try {
            processMediaChunkDual(ws.callUuid, message);
          } catch (err) {
            // Log the first few errors then go quiet to avoid log flood
            if (binaryFrameCount <= 5) {
              log.error(`processMediaChunkDual threw on ${ws.callUuid} (frame ${binaryFrameCount})`, {
                error: err.message,
                stack: err.stack,
                payloadLen: message?.length,
              });
            }
          }
          mirrorAudioPayload(ws.callUuid, message, true);
        }
        return;
      }

      const text = String(message || '');
      if (ws.callUuid) {
        mirrorAudioPayload(ws.callUuid, text, false);
      }
      let parsed;
      try { parsed = JSON.parse(text); } catch { return; }

      const maybeUuid = parsed?.uuid || parsed?.call_uuid || parsed?.channel_uuid || parsed?.callUuid;
      if (maybeUuid && !ws.callUuid) {
        ws.callUuid = String(maybeUuid);
        ensureMediaDetectorSession(ws.callUuid);
        log.info(`Beep detector WS uuid assigned via JSON`, { uuid: ws.callUuid });
        const call = activeCalls.get(ws.callUuid);
        if (call?.listenMirrorEnabled && call.listenMirrorUrl) {
          openSamplingMirror(ws.callUuid, call.listenMirrorUrl);
        }
      }

      const b64 = parsed?.audio || parsed?.payload || parsed?.data;
      if (ws.callUuid && typeof b64 === 'string' && b64.length > 0) {
        try { processMediaChunkDual(ws.callUuid, Buffer.from(b64, 'base64')); } catch { /* ignore malformed payload */ }
      }
    });

    ws.on('close', (code, reason) => {
      log.info(`Beep detector WS closed`, { uuid: ws.callUuid || '(none)', code, reason: String(reason || ''), binaryFrameCount });
      if (ws.callUuid) {
        removeMediaDetectorSession(ws.callUuid);
      }
    });
  });

  mediaDetectorWss.on('listening', () => {
    log.info('Node media detector server listening', {
      host: config.detection.nodeWsHost,
      port: config.detection.nodeWsPort,
    });
  });
}

function stopMediaDetectorServer() {
  if (!mediaDetectorWss) return;
  mediaDetectorWss.close();
  mediaDetectorWss = null;
  for (const session of mediaDetectorSessions.values()) {
    closeSamplingMirror(session, 'server-stopped');
  }
  mediaDetectorSessions.clear();
}

function startNodeAudioFork(uuid) {
  if (!config.detection.nodeEnabled) return;
  const wsTarget = `ws://127.0.0.1:${config.detection.nodeWsPort}/detect?uuid=${uuid}`;
  const args = `${uuid} start ${wsTarget} ${config.detection.nodeMixType} ${config.detection.nodeSampleRate}`;
  fsApi('uuid_audio_fork', args, (res) => {
    const body = (res.getBody() || '').trim();
    if (body.startsWith('-ERR')) {
      log.error(`uuid_audio_fork FAILED on ${uuid}`, { response: body, wsTarget });
    } else {
      log.info(`uuid_audio_fork started on ${uuid}`, { response: body, wsTarget });
    }
  });
}

function stopNodeAudioFork(uuid) {
  if (!config.detection.nodeEnabled) return;
  fsApi('uuid_audio_fork', `${uuid} stop`, () => {});
}

// ═══════════════════════════════════════════════════════════════════════════════
//  FREESWITCH ESL
// ═══════════════════════════════════════════════════════════════════════════════
// ═══════════════════════════════════════════════════════════════════════════════
//  PORTAL WEB SOCKET SERVER (Agent login, presence, availability updates)
// ═══════════════════════════════════════════════════════════════════════════════

let portalWss = null;
const portalOfflineTimers = new Map(); // agentId -> timeout

function startPortalWebSocket() {
  if (portalWss) return;
  portalWss = new WebSocketServer({
    host: '0.0.0.0',
    port: config.agent.portalWsPort,
    perMessageDeflate: false,
  });

  portalWss.on('connection', (ws, req) => {
    log.info(`Portal agent WS connected`, { remote: req.socket.remoteAddress });

    ws.on('message', async (message) => {
      try {
        const msg = JSON.parse(message);
        if (msg.type === 'agent_presence') {
          if (msg.agentId && portalOfflineTimers.has(msg.agentId)) {
            clearTimeout(portalOfflineTimers.get(msg.agentId));
            portalOfflineTimers.delete(msg.agentId);
          }

          const agent = {
            agentId: msg.agentId,
            extension: msg.extension,
            portalOnline: true,
            sipRegistered: msg.sipRegistered || false,
            availableInbound: msg.availableInbound || false,
            availableOutbound: msg.availableOutbound || false,
            inCall: false,
            lastAssignedAt: Date.now(),
          };

          await saveAgent(agent);
          await setAgentExtensionMap(agent.agentId, agent.extension);   // ← Important mapping

          if (agent.availableInbound) await addToPool(agent.agentId, 'inbound');
          if (agent.availableOutbound) await addToPool(agent.agentId, 'outbound');

          ws.agentId = agent.agentId;   // Store on socket for cleanup on disconnect

          log.info(`Agent ${msg.agentId} (${msg.extension}) presence updated`, agent);
        }
      } catch (e) {
        log.error(`Portal WS message error`, { error: e.message });
      }
    });

    ws.on('close', async () => {
      log.info('Portal agent WS closed');

      // If we stored agentId on the socket during presence message
      if (ws.agentId) {
        if (portalOfflineTimers.has(ws.agentId)) {
          clearTimeout(portalOfflineTimers.get(ws.agentId));
        }

        const timer = setTimeout(async () => {
          try {
            const agent = await getAgent(ws.agentId);
            if (!agent) return;
            agent.portalOnline = false;
            await saveAgent(agent);
            await removeFromPool(ws.agentId, 'inbound');
            await removeFromPool(ws.agentId, 'outbound');
            log.info(`Agent ${ws.agentId} marked offline after WS grace timeout`);
          } catch (err) {
            log.error(`Failed marking ${ws.agentId} offline after WS close`, { error: err.message });
          } finally {
            portalOfflineTimers.delete(ws.agentId);
          }
        }, 15000);

        portalOfflineTimers.set(ws.agentId, timer);
      }
    });
  });

  log.info(`Portal Agent WS listening on :${config.agent.portalWsPort}`);
}

let eslConn = null, eslConnected = false, eslReconnTimer = null;
const pendingJobs = new Map();

const EVENTS = process.env.ESL_EVENTS || 'all';

function connectESL() {
  return new Promise((resolve, reject) => {
    let settled = false;
    log.info(`Connecting ESL → ${config.freeswitch.host}:${config.freeswitch.port}`);

    const conn = new esl.Connection(
      config.freeswitch.host,
      config.freeswitch.port,
      config.freeswitch.password,
      () => {
        eslConn = conn;
        eslConnected = true;

        log.info('FreeSWITCH ESL connected');

        conn.events('json', EVENTS, () =>
          log.info('Subscribed to FS events', { events: EVENTS })
        );

        // Existing handlers
        conn.on('esl::event::BACKGROUND_JOB::*',          onBackgroundJob);
        conn.on('esl::event::CHANNEL_ANSWER::*',           onChannelAnswer);
        conn.on('esl::event::CHANNEL_HANGUP_COMPLETE::*',  onChannelHangup);
        conn.on('esl::event::CHANNEL_EXECUTE_COMPLETE::*', onExecuteComplete);
        conn.on('esl::event::CUSTOM::*',                   onCustomEvent);

        // ✅ Sofia registration events
        conn.on('esl::event::sofia::register::*',   onSofiaRegister);
        conn.on('esl::event::sofia::unregister::*', onSofiaUnregister);

        // ✅ Verify sofia status
        conn.api('sofia', 'status', (res) => {
          log.info('Sofia status at startup', {
            response: (res.getBody() || '').trim()
          });
        });

        // ✅ Explicit subscription
        conn.api('event', 'json CUSTOM sofia::register sofia::unregister', (res) => {
          log.info('Explicit sofia event subscription result', {
            response: (res.getBody() || '').trim()
          });
        });

        // ✅ Inbound routing handlers (KEEP INSIDE CALLBACK)
        conn.on('esl::event::CHANNEL_CREATE::*', onChannelCreate);
        conn.on('esl::event::CHANNEL_PARK::*',   onChannelPark);

        settled = true;
        resolve();
      }
    );

    conn.on('error', (e) => {
      eslConnected = false;
      log.error(`ESL error: ${e.message}`);
      if (!settled) {
        settled = true;
        reject(e);
        return;
      }
      scheduleESLReconnect();
    });

    conn.on('esl::end', () => {
      eslConnected = false;
      log.warn('ESL ended');
      scheduleESLReconnect();
    });
  });
}

async function connectESLWithRetry() {
  while (!isShuttingDown) {
    try {
      await connectESL();
      return;
    } catch (e) {
      log.warn('Initial ESL connect failed, retrying', {
        error: e?.message || 'unknown',
        retryInMs: config.freeswitch.reconnectMs,
      });
      await sleep(config.freeswitch.reconnectMs);
    }
  }
  throw new Error('shutdown-in-progress');
}

function scheduleESLReconnect() {
  if (eslReconnTimer) return;
  eslReconnTimer = setTimeout(() => { eslReconnTimer = null; connectESL().catch(() => scheduleESLReconnect()); }, config.freeswitch.reconnectMs);
}

// ─── Helper: send FS API command ─────────────────────────────────────────────
function fsApi(cmd, args, cb) { eslConn.api(cmd, args, cb || (() => {})); }

function fsApiBody(res) {
  return (res?.getBody?.() || '').trim();
}

function fsApiAsync(cmd, args) {
  return new Promise((resolve) => {
    try {
      fsApi(cmd, args, (res) => resolve(fsApiBody(res)));
    } catch (err) {
      resolve(`-ERR ${err?.message || 'api-failed'}`);
    }
  });
}

async function resolveAgentBridgeTarget(extension) {
  const ext = String(extension || '').trim();
  if (!ext) return null;

  // Try explicit profile/domain first, then wildcard profile, then bare extension.
  const candidates = [
    `internal/${ext}@${config.freeswitch.sipDomain}`,
    `*/${ext}`,
    ext,
  ];

  for (const candidate of candidates) {
    const response = String(await fsApiAsync('sofia_contact', candidate) || '').trim();
    if (!response) continue;
    const low = response.toLowerCase();
    if (low.startsWith('error/') || low.startsWith('-err')) continue;
    return response;
  }
  return null;
}

function executeUuidApp(uuid, app, appArgs, call, label, options = {}) {
  const metaBase = callMeta(uuid, call, { app, label });
  const breakAfterEnqueue = options.breakAfterEnqueue !== false;
  const breakMode = breakAfterEnqueue ? (options.breakMode || 'callback') : 'none';
  const requiredState = options.requiredState || null;
  if (requiredState) {
    const current = activeCalls.get(uuid);
    if (!current || current.state !== requiredState) {
      log.info(`Skipping ${label} on ${uuid} due to state mismatch`, {
        expectedState: requiredState,
        actualState: current?.state || '(missing)',
      });
      return;
    }
  }

  // The correct approach for inbound ESL is eslConn.execute() which sends
  // sendmsg/execute to FreeSWITCH. It puts execute-app-name and execute-app-arg
  // in separate ESL headers so FS never space-splits the args — no truncation.
  //
  // Why NOT uuid_broadcast: FS parses its args as switch_separate_string(..., 4)
  // so argv[1] = only the FIRST space-delimited token (e.g. "play_and_get_digits::1").
  // All remaining args are silently discarded → no audio, no error logged.
  //
  // Why NOT uuid_transfer inline: FS space-splits first too, so the quoted
  // destination gets broken → "Dialplan [1] not found" → NO_ROUTE_DESTINATION.
  //
  // Sequence: queue the execute FIRST so there is always a next app ready, THEN
  // break park() — avoids any race where the channel has nothing to run.
  // modesl is callback-based here, not Promise-based.
  eslConn.execute(app, String(appArgs), uuid, (reply) => {
    const body = (reply?.getBody?.() || '').trim();
    log.info(`execute callback for ${label} on ${uuid}`, { ...metaBase, response: body });

    if (body.startsWith('-ERR')) {
      log.error(`execute ${label} failed on ${uuid}`, { ...metaBase, response: body });
      return;
    }

    if (breakMode !== 'callback') {
      return;
    }

    fsApi('uuid_break', `${uuid} all`, (breakRes) => {
      log.info(`uuid_break after execute callback on ${uuid}`, {
        ...metaBase,
        response: fsApiBody(breakRes),
      });
    });
  });

  if (breakMode !== 'dispatch') return;

  // Break immediately after dispatching execute; waiting for execute callback can
  // delay interruption until app completion on some FS/modesl combinations.
  fsApi('uuid_break', `${uuid} all`, (breakRes) => {
    log.info(`uuid_break after execute dispatch on ${uuid}`, {
      ...metaBase,
      response: fsApiBody(breakRes),
    });
  });
}

// ─── Start the IVR gather (announcement + DTMF) via play_and_get_digits ─────
function startIvrGather(uuid, call) {
  call.state = S.IVR_ACTIVE;
  const cd  = call.callDetails;
  const url = resolveAudio(cd.wavUrlAnnounce);
  const t   = config.calls.dtmfGatherTimeout;
  // play_and_get_digits <min> <max> <tries> <timeout_after_play_ms> <terminators> <file> <invalid_file> <var_name> <regexp> <digit_timeout>
  // timeout starts AFTER the audio finishes → matches Jambonz gather behaviour
  const terms = (config.calls.dtmfTerminators || '*').trim() || '*';
  const args = `1 1 1 ${t * 1000} ${terms} ${url} silence_stream://10 dtmf_result \\d ${t * 1000}`;
  executeUuidApp(uuid, 'play_and_get_digits', args, call, 'play_and_get_digits', {
    breakAfterEnqueue: false,
    requiredState: S.IVR_ACTIVE,
  });
  log.info(`IVR gather started on ${uuid}`, { transactionId: cd.transactionId, announceUrl: url });
}

function verifyDetectorFlow(uuid, attempt = 1) {
  const call = activeCalls.get(uuid);
  if (!call || call.state !== S.IVR_ACTIVE) return;
  if (!config.detection.nodeEnabled) return;

  const session = mediaDetectorSessions.get(uuid);
  const frames = session?.frameCount || 0;
  if (frames > 0) return;

  if (attempt > 3) {
    log.error(`No detector media frames on ${uuid} after retries`, callMeta(uuid, call, {
      attempts: attempt - 1,
      hasSession: Boolean(session),
      frames,
    }));
    return;
  }

  log.warn(`No detector frames yet on ${uuid}; restarting audio fork`, callMeta(uuid, call, {
    attempt,
    hasSession: Boolean(session),
    frames,
  }));
  stopNodeAudioFork(uuid);
  startNodeAudioFork(uuid);

  setTimeout(() => verifyDetectorFlow(uuid, attempt + 1), 1200 * attempt);
}

function maybeStartListenStreaming(uuid, call) {
  if (!config.listen.enabled) return;
  if (Math.random() >= config.listen.probability) return;

  const ip = listenIpRotator.next();
  if (!ip) return;

  const wsUrl = `ws://${ip}:${config.listen.websocketPort}`;
  call.listenMirrorEnabled = true;
  call.listenMirrorUrl = wsUrl;

  if (config.detection.nodeEnabled) {
    openSamplingMirror(uuid, wsUrl);
    log.info(`Sampling mirror armed on ${uuid}`, {
      transactionId: call.callDetails.transactionId,
      wsUrl,
    });
  } else {
    const args = `${uuid} start ${wsUrl} ${config.listen.mixType} ${config.listen.sampleRate}`;
    fsApi('uuid_audio_fork', args, (res) => {
      log.info(`Listen streaming started on ${uuid}`, {
        transactionId: call.callDetails.transactionId,
        wsUrl,
        response: (res.getBody() || '').trim(),
      });
    });
  }

  call.listenStopTimer = setTimeout(() => {
    stopListenStreaming(uuid, call, 'max-length-reached');
  }, config.listen.maxLengthSec * 1000);
}

function stopListenStreaming(uuid, call, reason) {
  if (!call) return;
  if (call.listenStopTimer) {
    clearTimeout(call.listenStopTimer);
    call.listenStopTimer = null;
  }
  call.listenMirrorEnabled = false;
  if (config.detection.nodeEnabled) {
    const session = mediaDetectorSessions.get(uuid);
    if (session) closeSamplingMirror(session, reason || 'stopped');
    return;
  }
  fsApi('uuid_audio_fork', `${uuid} stop`, () => {
    log.debug(`Listen streaming stopped on ${uuid}`, {
      transactionId: call.callDetails.transactionId,
      reason,
    });
  });
}

function startDetectors(uuid) {
  log.info(`Starting detectors on ${uuid}`, {
    nodeDsp: config.detection.nodeEnabled,
    avmd: config.detection.enableAvmd,
    vmd: config.detection.enableVmd,
  });
  if (config.detection.enableAvmd) {
    fsApi('avmd', `${uuid} start`, (r) => {
      log.info(`AVMD started ${uuid}: ${(r.getBody() || '').trim()}`);
    });
  }
  if (config.detection.enableVmd) {
    // VMD as a second signal source; API form avoids blocking IVR gather.
    fsApi('vmd', `${uuid} start`, (r) => {
      log.debug(`VMD started ${uuid}: ${(r.getBody() || '').trim()}`);
    });
  }
}

function stopDetectors(uuid) {
  log.debug(`Stopping detectors on ${uuid}`);
  stopNodeAudioFork(uuid);
  if (config.detection.enableAvmd) fsApi('avmd', `${uuid} stop`);
  if (config.detection.enableVmd) fsApi('vmd', `${uuid} stop`);
}

function waitForRemoteSilenceThen(uuid, call, done) {
  if (!config.detection.nodeEnabled) {
    done();
    return;
  }

  const requiredMs = Math.max(0, config.calls.vmRemoteSilenceRequiredMs || 0);
  const maxWaitMs = Math.max(0, config.calls.vmRemoteSilenceMaxWaitMs || 0);
  const startedAt = Date.now();

  const tick = () => {
    const active = activeCalls.get(uuid);
    if (!active || active.state !== S.VM_PLAYING) return;

    const session = mediaDetectorSessions.get(uuid);
    const silenceMs = session?.silenceMs || 0;
    const waitedMs = Date.now() - startedAt;

    if (silenceMs >= requiredMs) {
      log.info(`Remote silence reached before VM playback on ${uuid}`, {
        transactionId: call.callDetails.transactionId,
        silenceMs,
        requiredMs,
        waitedMs,
      });
      done();
      return;
    }

    if (waitedMs >= maxWaitMs) {
      log.info(`VM silence wait timed out on ${uuid}; proceeding`, {
        transactionId: call.callDetails.transactionId,
        silenceMs,
        requiredMs,
        maxWaitMs,
      });
      done();
      return;
    }

    setTimeout(tick, 100);
  };

  tick();
}

function hasBeepQuorum(call, nowMs) {
  const recentSources = Object.entries(call.beepSignals)
    .filter(([, ts]) => nowMs - ts <= config.detection.quorumWindowMs)
    .map(([source]) => source);
  return new Set(recentSources).size >= 2;
}

function triggerBeepTransition(uuid, call, sourceSummary) {
  if (!call) return;
  if (call.state === S.TRANSFERRING || call.state === S.DONE || call.state === S.VM_PLAYING) return;

  transitionCallState(uuid, call, S.VM_PLAYING, 'beep-confirmed', { sourceSummary });
  if (call.beepConfirmTimer) {
    clearTimeout(call.beepConfirmTimer);
    call.beepConfirmTimer = null;
  }

  log.info(`BEEP confirmed on ${uuid} — switching to VM`, {
    transactionId: call.callDetails.transactionId,
    to: call.callDetails.numberTo,
    signalSource: sourceSummary,
  });

  // Stop detectors first so no further IVR/beep side-effects race with VM handoff.
  stopDetectors(uuid);
  stopListenStreaming(uuid, call, 'beep-detected');

  const vmUrl = resolveAudio(call.callDetails.wavUrlVM);
  if (!vmUrl) {
    log.warn(`No wavUrlVM for ${uuid} — hanging up`);
    fsApi('uuid_kill', uuid);
  } else {
    // For VM handoff, break the active gather first so playback does not sit
    // behind a still-running play_and_get_digits app on the channel.
    fsApi('uuid_break', `${uuid} all`, (breakRes) => {
      log.info(`uuid_break before VM playback on ${uuid}`, {
        transactionId: call.callDetails.transactionId,
        response: fsApiBody(breakRes),
      });

      setTimeout(() => {
        executeUuidApp(uuid, 'playback', vmUrl, call, 'vm-playback', {
          breakAfterEnqueue: false,
          requiredState: S.VM_PLAYING,
        });
      }, config.calls.vmPlaybackStartDelayMs);
    });
  }

  if (!call.dispositionSent) {
    call.dispositionSent = true;
    reportDisposition({
      transactionid: call.callDetails.transactionId,
      from: call.callDetails.numberFrom,
      to: call.callDetails.numberTo,
      disposition: 'VM',
    });
  }
}

function registerBeepSignal(uuid, source) {
  try {
    const call = activeCalls.get(uuid);
    if (!call) return false;
    if (call.state === S.TRANSFERRING || call.state === S.DONE || call.state === S.VM_PLAYING) return false;

    const nowMs = Date.now();
    const elapsedFromAnswer = call.answeredAtMs > 0 ? nowMs - call.answeredAtMs : 0;
    log.info(`Beep signal received on ${uuid}`, callMeta(uuid, call, { source, elapsedFromAnswer }));
    if (elapsedFromAnswer < config.detection.minBeepMs) {
      log.info(`Ignoring early ${source} signal on ${uuid}`, {
        elapsedFromAnswer,
        minRequired: config.detection.minBeepMs,
      });
      return false;
    }

    // Defensive: keep this always usable even if call state object is malformed.
    if (!call.beepSignals || typeof call.beepSignals !== 'object') {
      call.beepSignals = {};
    }
    call.beepSignals[source] = nowMs;
    const sourceList = Object.keys(call.beepSignals);

    if (config.detection.requireDualSignal && !hasBeepQuorum(call, nowMs)) {
      if (elapsedFromAnswer >= config.detection.singleSignalGraceMs) {
        log.info(`Beep grace timeout reached on ${uuid}; accepting single source`, callMeta(uuid, call, { source }));
        triggerBeepTransition(uuid, call, `single-${source}-after-grace`);
        return true;
      }
      log.debug(`Waiting for second beep signal on ${uuid}`, { source, sourceList });
      return false;
    }

    if (call.beepConfirmTimer) clearTimeout(call.beepConfirmTimer);
    log.info(`Arming beep confirm timer on ${uuid}`, callMeta(uuid, call, {
      confirmDelayMs: config.detection.confirmDelayMs,
      sources: sourceList,
    }));
    call.beepConfirmTimer = setTimeout(() => {
      const current = activeCalls.get(uuid);
      if (!current) return;
      triggerBeepTransition(uuid, current, sourceList.join('+'));
    }, config.detection.confirmDelayMs);
    return true;
  } catch (err) {
    log.error(`registerBeepSignal failed on ${uuid}`, {
      source,
      error: err?.message || String(err),
      stack: err?.stack,
    });
    return false;
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
//  EVENT HANDLERS
// ═══════════════════════════════════════════════════════════════════════════════
// ═══════════════════════════════════════════════════════════════════════════════
// SOFIA REGISTRATION HANDLERS (for browser agents)
// ═══════════════════════════════════════════════════════════════════════════════



function onSofiaRegister(evt) {
  // Dump ALL headers so we can see exactly what FS sends
  const allHeaders = {};
  try {
    const raw = evt.serialize ? evt.serialize() : '{}';
    // modesl exposes getHeader() per key; let's probe the known candidates
    const candidates = [
      'User', 'from_user', 'sip_auth_username', 'sip_from_user',
      'variable_sip_auth_username', 'Auth-User', 'To-User',
      'Caller-Username', 'sip_contact_user',
    ];
    for (const h of candidates) {
      const v = evt.getHeader(h);
      if (v) allHeaders[h] = v;
    }
  } catch (e) {
    log.error('onSofiaRegister: header dump failed', { error: e.message });
  }

  log.info('onSofiaRegister fired', {
    headers: allHeaders,
    subclass: evt.getHeader('Event-Subclass') || '',
    profile: evt.getHeader('profile-name') || evt.getHeader('Sofia-Profile') || '',
    network_ip: evt.getHeader('network-ip') || '',
    expires: evt.getHeader('expires') || '',
  });

  const extension =
    evt.getHeader('from_user') ||
    evt.getHeader('sip_auth_username') ||
    evt.getHeader('User') ||
    evt.getHeader('sip_from_user') ||
    evt.getHeader('variable_sip_auth_username') ||
    '';

  if (!extension) {
    log.error('onSofiaRegister: could not extract extension from any known header', { headers: allHeaders });
    return;
  }

  log.info(`onSofiaRegister: resolved extension = "${extension}"`);
  updateSipRegisteredByExtension(extension, true);
}

function onSofiaUnregister(evt) {
  const extension =
    evt.getHeader('from_user') ||
    evt.getHeader('sip_auth_username') ||
    evt.getHeader('User') ||
    evt.getHeader('sip_from_user') ||
    '';

  log.info('onSofiaUnregister fired', {
    extension: extension || '(none)',
    profile: evt.getHeader('profile-name') || '',
  });

  if (!extension) {
    log.warn('onSofiaUnregister: no extension found — skipping');
    return;
  }

  updateSipRegisteredByExtension(extension, false);
}
// ═══════════════════════════════════════════════════════════════════════════════
// INBOUND AGENT ROUTING (using CHANNEL_CREATE + CHANNEL_PARK)
// ═══════════════════════════════════════════════════════════════════════════════

function isInboundRoutedCall(evt) {
  const dir = String(evt.getHeader('Call-Direction') || '').toLowerCase();
  const callType = String(evt.getHeader('variable_call_type') || '').toLowerCase();
  const routeGroup = String(evt.getHeader('variable_route_group') || '').toLowerCase();

  return dir === 'inbound' && 
         (callType === 'inbound' || routeGroup === 'inbound');
}
// ─── Channel role classifier ──────────────────────────────────────────────────
// Returns one of: 'inbound_customer' | 'outbound_customer' | 'agent_leg' | 'unknown'
// Single source of truth consumed by onChannelCreate, onChannelAnswer, onChannelHangup.
function classifyChannel(evt) {
  const role      = String(evt.getHeader('variable_call_role') || evt.getHeader('call_role') || '').toLowerCase();
  const direction = String(evt.getHeader('Call-Direction')     || '').toLowerCase();
  const callType  = String(evt.getHeader('variable_call_type') || evt.getHeader('call_type') || '').toLowerCase();
  const routeGrp  = String(evt.getHeader('variable_route_group') || evt.getHeader('route_group') || '').toLowerCase();
  const inboundDid = String(evt.getHeader('variable_inbound_did') || evt.getHeader('inbound_did') || '').trim();
  const inboundFrom = String(evt.getHeader('variable_inbound_from') || evt.getHeader('inbound_from') || '').trim();

  if (role === 'agent_leg')         return 'agent_leg';
  if (role === 'outbound_customer') return 'outbound_customer';
  if (role === 'inbound_customer')  return 'inbound_customer';

  // Fallback for channels that predate the explicit call_role var
  if (direction === 'inbound' && (callType === 'inbound' || routeGrp === 'inbound'))
    return 'inbound_customer';
  if (inboundDid || inboundFrom)
    return 'inbound_customer';
  if (direction === 'outbound')
    return 'outbound_customer';

  return 'unknown';
}
function onChannelCreate(evt) {
  const uuid = evt.getHeader('Unique-ID');
  const role = classifyChannel(evt);

  if (role === 'agent_leg') {
    const customerUuid = evt.getHeader('variable_cc_member_uuid') || '';
    if (customerUuid) {
      agentLegToCustomer.set(uuid, customerUuid);
      customerToAgentLeg.set(customerUuid, uuid);
      log.info(`Agent leg created ${uuid} → customer ${customerUuid}`, { role });
    } else {
      log.warn(`Agent leg ${uuid} created with no cc_member_uuid`, { role });
    }
    return;
  }

  if (role === 'inbound_customer') {
    const did  = evt.getHeader('variable_inbound_did')  || evt.getHeader('Caller-Destination-Number');
    const from = evt.getHeader('variable_inbound_from') || evt.getHeader('Caller-Caller-ID-Number');
    log.info(`Inbound customer channel created ${uuid}`, { role, did, from });
    return;
  }

  if (role === 'outbound_customer') {
    log.debug(`Outbound customer channel created ${uuid}`, { role });
    return;
  }

  log.debug(`Channel created ${uuid} with unclassified role`, { role });
}

async function onChannelPark(evt) {
  const role = classifyChannel(evt);
  const uuid = evt.getHeader('Unique-ID');
  const did = evt.getHeader('variable_inbound_did') || evt.getHeader('inbound_did') || evt.getHeader('Caller-Destination-Number');
  const from = evt.getHeader('variable_inbound_from') || evt.getHeader('inbound_from') || evt.getHeader('Caller-Caller-ID-Number');
  const callType = String(evt.getHeader('variable_call_type') || evt.getHeader('call_type') || '').toLowerCase();
  const routeGrp = String(evt.getHeader('variable_route_group') || evt.getHeader('route_group') || '').toLowerCase();
  const direction = String(evt.getHeader('Call-Direction') || '').toLowerCase();
  const roleVar = String(evt.getHeader('variable_call_role') || evt.getHeader('call_role') || '').toLowerCase();

  log.info(`CHANNEL_PARK observed ${uuid}`, {
    direction,
    roleVar,
    roleClassified: role,
    callType,
    routeGrp,
    did,
    from,
  });

  const inboundTagged =
    direction === 'inbound' &&
    (role === 'inbound_customer' || callType === 'inbound' || routeGrp === 'inbound' || Boolean(did || from));
  if (!inboundTagged) return;

  log.info(`Inbound call parked ${uuid} — starting queue retry logic`, { role, did, from });

  // Track the call if not already tracked
  if (!activeCalls.has(uuid)) {
    activeCalls.set(uuid, {
      callDetails: {
        transactionId: `inbound-${Date.now()}`,
        numberTo: did,
        numberFrom: from,
        isAgentCall: true,
        callType: 'inbound',
      },
      state: S.RINGING,
      assignedAgentId: null,
      dispositionSent: false,
      startTime: Date.now(),
      queueStartAt: Date.now(),
      queueAttempts: 0,
    });

    // Persist inbound call route baseline so later updates have full context.
    await saveCallRouteRecord({
      transactionId: activeCalls.get(uuid)?.callDetails?.transactionId,
      customerUuid: uuid,
      direction: 'inbound',
      createdAt: new Date().toISOString(),
    });
  }

  const call = activeCalls.get(uuid);
  const deadline = Date.now() + (config.agent.inboundMaxWaitSec * 1000);

  // Ensure inbound customer leg is fully answered before bridging to WebRTC agent.
  // Keeping the leg in early-media state can cause post-answer bridge teardown.
  executeUuidApp(uuid, 'answer', '', call, 'inbound-answer', {
    breakAfterEnqueue: false,
  });

  // Optional: Start MOH or hold prompt
  if (config.agent.inboundHoldPrompt) {
    executeUuidApp(uuid, 'playback', resolveAudio(config.agent.inboundHoldPrompt), call, 'inbound-hold', {
      breakAfterEnqueue: false,
    });
  } else {
    // Default Music on Hold if no custom prompt
    executeUuidApp(uuid, 'playback', 'local_stream://moh', call, 'moh', {
      breakAfterEnqueue: false,
    });
  }

  // Main queue retry loop
  while (Date.now() < deadline) {
    // Check if caller already hung up
    if (!activeCalls.has(uuid)) {
      log.info(`Inbound caller hung up while waiting in queue ${uuid}`);
      return;
    }

        call.queueAttempts = (call.queueAttempts || 0) + 1;

    // === ADD CUSTOMER ASSIGNMENT LOCK HERE ===
    const lockResult = await acquireCustomerAssignLock(uuid, 20); // 20s TTL
    if (!lockResult.success) {
      log.info(`Customer ${uuid} is already being assigned by another process — skipping this attempt`);
      if (Date.now() + config.agent.inboundRetryGapMs < deadline) {
        await sleep(config.agent.inboundRetryGapMs);
      }
      continue;
    }
    const lockValue = lockResult.lockValue || '';
    let ok = false;
    try {
      ok = await routeCallToAgentWithRetry(uuid, 'inbound');
    } finally {
      await releaseCustomerAssignLock(uuid, lockValue);
    }

    if (ok) {
      log.info(`Inbound call ${uuid} successfully routed to agent after ${call.queueAttempts} attempts`);
      return;
    }

    // No agent answered → wait before next retry
    if (Date.now() + config.agent.inboundRetryGapMs < deadline) {
      log.debug(`No agent available, retrying in ${config.agent.inboundRetryGapMs}ms`, { 
        uuid, 
        attempts: call.queueAttempts 
      });
      await sleep(config.agent.inboundRetryGapMs);
    } else {
      break;
    }
  }

  // === Fallback after timeout ===
  log.warn(`Inbound queue timeout for ${uuid} after ${call.queueAttempts} attempts`, { did, from });

  await updateCallRouteRecord(uuid, {
    queueEndAt: new Date().toISOString(),
    finalCause: 'NO_AGENT_TIMEOUT',
    disposition: 'QUEUE_TIMEOUT'
  });

  // Fallback 1: Play short unavailable prompt
  executeUuidApp(uuid, 'playback', 'ivr/agent-unavailable.wav', call, 'unavailable-prompt', {
    breakAfterEnqueue: false,
  });

  // Fallback 2: Transfer to voicemail (if configured) or just hangup
  // Example: fsApi('uuid_transfer', `${uuid} 1000 XML default`); // change 1000 to your VM extension

  // For now, just hangup
  setTimeout(() => {
    fsApi('uuid_kill', uuid);
  }, 3000); // give 3 seconds for prompt to play
}
// ── bgapi originate result ───────────────────────────────────────────────────
function onBackgroundJob(evt) {
  const jobId = evt.getHeader('Job-UUID'), callId = pendingJobs.get(jobId);
  if (!callId) return;
  pendingJobs.delete(jobId);
  const body = (evt.getBody() || '').trim();
  if (body.startsWith('-ERR')) {
    log.error(`Originate failed ${callId}: ${body}`);
    const c = untrackCall(callId);
    if (c) {
      rateLimiter.replenish();
      releaseCallLock(c.callDetails.numberTo, c.callDetails.prefix);
      if (!c.dispositionSent) { c.dispositionSent = true;
        reportDisposition({ transactionid: c.callDetails.transactionId, from: c.callDetails.numberFrom,
          to: c.callDetails.numberTo, disposition: body.includes('USER_BUSY') ? 'USER_BUSY' : 'NO_ANSWER' });
      }
    }
  } else { log.debug(`Originate OK ${callId}`); }
}

// ── Call answered → start AVMD + IVR gather concurrently ─────────────────────
// ── Call answered → start AVMD + IVR gather concurrently ─────────────────────
async function onChannelAnswer(evt) {
  const uuid = evt.getHeader('Unique-ID');
  const role = classifyChannel(evt);

  // ==================== AGENT LEG HANDLING ====================
  if (role === 'agent_leg') {
    const customerUuid = agentLegToCustomer.get(uuid) ||
                         evt.getHeader('variable_cc_member_uuid') ||
                         '';

    log.info(`Agent leg answered ${uuid}`, { role, customerUuid });

    if (!customerUuid) {
      log.warn(`Agent leg answered but no customerUuid found`, { agentLegUuid: uuid });
      fsApi('uuid_kill', uuid);
      return;
    }

    // === CRITICAL CHECK: Agent answered AFTER customer already hung up? ===
    if (!activeCalls.has(customerUuid)) {
      log.warn(`Agent answered but customer ${customerUuid} already hung up — killing agent leg`, {
        agentLegUuid: uuid,
        customerUuid
      });

      fsApi('uuid_kill', uuid);

      // Try to release the agent
      const routeData = await redisClient.hGetAll(`callroute:${customerUuid}`).catch(() => ({}));
      const agentId = routeData?.agentId || null;
      if (agentId) {
        await releaseAssignedAgent(agentId);
      }

      agentLegToCustomer.delete(uuid);
      customerToAgentLeg.delete(customerUuid);

      await updateCallRouteRecord(customerUuid, {
        finalCause: 'AGENT_ANSWERED_AFTER_CUSTOMER_HANGUP',
        endedAt: new Date().toISOString()
      });
      return;
    }

    // Normal flow: customer is still alive
    await updateCallRouteRecord(customerUuid, {
      agentLegUuid: uuid,
      agentAnsweredAt: new Date().toISOString(),
    });

    // Count each customer call only once when an agent actually answers.
    const counted = await markAgentAnsweredCounted(customerUuid);
    if (counted) {
      const customerCall = activeCalls.get(customerUuid);
      const agentId = customerCall?.assignedAgentId || "";
      await incrementAgentAnsweredCounters(agentId);
    }

    // Bridge success should be resolved only when actual agent_leg answers.
    const pending = bridgePromises.get(customerUuid);
    if (pending) {
      pending.resolve({ answered: true, cause: 'SUCCESS' });
      bridgePromises.delete(customerUuid);
      log.info(`Resolved bridge promise from real agent answer`, {
        customerUuid,
        agentLegUuid: uuid,
      });
    }

    log.info(`Agent ${uuid} successfully bridged to live customer ${customerUuid}`);
    return;
  }

  // ==================== CUSTOMER LEG HANDLING ====================
  const call = activeCalls.get(uuid);
  if (!call) return;

  log.info(`Answered ${uuid}`, {
    role,
    transactionId: call.callDetails?.transactionId,
    to: call.callDetails?.numberTo,
  });

  call.answeredAtMs = Date.now();

  markAnswered(call.callDetails.numberTo, {
    transactionId: call.callDetails.transactionId,
    callUuid: uuid,
    prefix: call.callDetails.prefix
  });

  await updateCallRouteRecord(uuid, {
    customerAnsweredAt: new Date().toISOString()
  });

  maybeStartListenStreaming(uuid, call);
  startNodeAudioFork(uuid);
  startDetectors(uuid);
  setTimeout(() => verifyDetectorFlow(uuid, 1), 1500);

  // 🔥 AGENT ROUTING LOGIC
  if (call.callDetails?.isAgentCall === true) {

    if (call.callDetails.callType === 'inbound') {
      log.debug(`Skipping outbound agent selection for inbound call ${uuid}`);
    } 
    else {
      // === OUTBOUND: Add customer assignment lock here ===
      let assignLockValue = '';
      try {
        log.info(`Outbound agent call detected — starting retry routing`, {
          transactionId: call.callDetails?.transactionId
        });

        const lockResult = await acquireCustomerAssignLock(uuid, 20);
        if (!lockResult.success) {
          log.info(`Customer ${uuid} is already being assigned — falling back to IVR`);
          startIvrGather(uuid, call);
          return;
        }
        assignLockValue = lockResult.lockValue || '';

        const ok = await routeCallToAgentWithRetry(uuid, 'outbound');

        if (ok) {
          log.info(`Outbound call successfully routed to an agent`, {
            transactionId: call.callDetails?.transactionId
          });
          return;
        }

        log.warn(`No outbound agent answered after retries — falling back to IVR`, {
          transactionId: call.callDetails?.transactionId
        });
        startIvrGather(uuid, call);

      } catch (err) {
        log.error(`Outbound agent routing failed`, {
          error: err.message,
          transactionId: call.callDetails?.transactionId
        });
        startIvrGather(uuid, call);
      } finally {
        if (assignLockValue) {
          await releaseCustomerAssignLock(uuid, assignLockValue);
        }
      }
    }
  }

  // Normal IVR flow for regular calls
  if (call.callDetails?.callType !== 'inbound' && 
      call.callDetails?.isAgentCall !== true) {
    startIvrGather(uuid, call);
  }

  // Safety max duration timer
  call.maxTimer = setTimeout(() => {
    const c = activeCalls.get(uuid);
    if (c && c.state !== S.TRANSFERRING && c.state !== S.DONE) {
      log.warn(`Max duration reached on ${uuid} — forcing hangup`);
      fsApi('uuid_kill', uuid);
    }
  }, config.calls.maxCallDurationMs);
}
// ── AVMD beep detected → interrupt IVR, play VM, report ─────────────────────
function onCustomEvent(evt) {
  const subclass = (evt.getHeader('Event-Subclass') || '').toLowerCase();
  const uuid = evt.getHeader('Unique-ID');
  if (!uuid) return;
  if (subclass.includes('avmd') || subclass.includes('vmd')) {
    log.debug(`Detector custom event on ${uuid}`, { subclass });
  }

  if (subclass === 'avmd::beep') {
    registerBeepSignal(uuid, 'avmd');
    return;
  }
 
  if (subclass.includes('vmd')) {
    const vmdStatus = String(evt.getHeader('VMD-Status') || evt.getHeader('vmd_status') || '').toLowerCase();
    const body = String(evt.getBody() || '').toLowerCase();
    if (subclass.includes('beep') || vmdStatus.includes('beep') || body.includes('beep')) {
      registerBeepSignal(uuid, 'vmd');
    }
    return;
  }
}


// ── Application execution completed ──────────────────────────────────────────
async function onExecuteComplete(evt) {
  const uuid = evt.getHeader('Unique-ID'), app = evt.getHeader('Application');
  const call = activeCalls.get(uuid);
  if (!call) return;
  const appResponse = evt.getHeader('Application-Response') || '';
  log.info(`Execute complete ${app} on ${uuid}`, callMeta(uuid, call, { appResponse }));

  // ── play_and_get_digits finished (IVR announcement + DTMF collection) ──
  if (app === 'play_and_get_digits') {
    if (call.state !== S.IVR_ACTIVE) return; // beep/transfer already took over → ignore
    // Read the digit FreeSWITCH stored in the channel variable
    fsApi('uuid_getvar', `${uuid} dtmf_result`, (res) => {
      const c = activeCalls.get(uuid);
      if (!c || c.state !== S.IVR_ACTIVE) return; // state changed while we waited
      const digit = (res.getBody() || '').trim();
      log.info(`Gather completed on ${uuid}`, callMeta(uuid, c, {
        dtmfResultRaw: digit || '_undef_',
      }));
      if (!digit || digit === '_undef_') {
        handleDtmfTimeout(uuid, c);
      } else {
        handleDtmfDigit(uuid, c, digit);
      }
    });
    return;
  }

  // ── playback finished (continue audio / optout audio / VM audio) ───────
  if (app === 'playback') {
    switch (call.state) {
      case S.CONTINUE_AUDIO:
        log.info(`Continue audio finished; starting bridge on ${uuid}`, callMeta(uuid, call));
        bridgeToDestination(uuid, call);
        break;
      case S.OPTOUT_AUDIO:
      case S.VM_PLAYING:
        transitionCallState(uuid, call, S.DONE, 'playback-finished');
        fsApi('uuid_kill', uuid);
        break;
      // If we get a stale playback complete (e.g. from a broken gather), ignore
      default: break;
    }
    return;
  }

  // ── bridge finished → the transferred call ended naturally ─────────────
if (app === 'bridge') {
  const pending = bridgePromises.get(uuid);
  const hangupCause = evt.getHeader('Hangup-Cause') ||
                     evt.getHeader('variable_last_bridge_hangup_cause') || 'UNKNOWN';

  // Fix 2: Capture Agent Leg UUID (B-leg)
  const agentLegUuid = 
    evt.getHeader('Other-Leg-Unique-ID') ||
    evt.getHeader('Bridge-B-Unique-ID') ||
    evt.getHeader('variable_bridge_uuid') || '';

  const normalizedCause = String(hangupCause || '').toUpperCase();
  const isAnswered = normalizedCause === 'SUCCESS';

  if (pending) {
    pending.resolve({
      answered: isAnswered,
      cause: hangupCause
    });
    bridgePromises.delete(uuid);
  }

  // Fix 1 + Fix 2: Update call route record when bridge completes
  if (call?.currentAgentAttempt) {
    await updateCallRouteRecord(uuid, {
      agentLegUuid: agentLegUuid,
      agentAnsweredAt: isAnswered ? new Date().toISOString() : '',
      bridgeCompletedAt: new Date().toISOString(),
      finalCause: hangupCause
    });

    // Also store in in-memory call object
    call.agentLegUuid = agentLegUuid;
  }

  // Only treat as final DONE for non-agent (normal IVR continue) bridges
  if (!call?.currentAgentAttempt) {
    transitionCallState(uuid, call, S.DONE, 'bridge-complete');
  }

  return;
}

}

// ── DTMF handling ────────────────────────────────────────────────────────────

function handleDtmfDigit(uuid, call, digit) {
  const cd = call.callDetails;
  log.info(`DTMF received on ${uuid}`, callMeta(uuid, call, { digit }));

  if (digit === cd.digitContinue) {
    // ── Continue: play continue audio → bridge to destination ──
    transitionCallState(uuid, call, S.CONTINUE_AUDIO, 'dtmf-continue', { digit });
    stopDetectors(uuid);
    stopListenStreaming(uuid, call, 'dtmf-continue');
    const url = resolveAudio(cd.wavUrlContinue);
    log.info(`Playing continue prompt on ${uuid}`, callMeta(uuid, call, { url }));
    executeUuidApp(uuid, 'playback', url, call, 'continue-playback');
    if (!call.dispositionSent) { call.dispositionSent = true;
      reportDisposition({ transactionid: cd.transactionId, from: cd.numberFrom, to: cd.numberTo, disposition: 'CONTINUE' });
    }

  } else if (digit === cd.digitOptOut) {
    // ── Opt-out: play opt-out audio → hangup ──
    transitionCallState(uuid, call, S.OPTOUT_AUDIO, 'dtmf-optout', { digit });
    stopDetectors(uuid);
    stopListenStreaming(uuid, call, 'dtmf-optout');
    const url = resolveAudio(cd.wavUrlOptOut);
    log.info(`Playing opt-out prompt on ${uuid}`, callMeta(uuid, call, { url }));
    executeUuidApp(uuid, 'playback', url, call, 'optout-playback');
    if (!call.dispositionSent) { call.dispositionSent = true;
      reportDisposition({ transactionid: cd.transactionId, from: cd.numberFrom, to: cd.numberTo, disposition: 'OPTOUT' });
    }

  } else {
    // ── Invalid digit → replay announcement ──
    log.info(`Invalid digit "${digit}" on ${uuid} — replaying`, { transactionId: cd.transactionId });
    startIvrGather(uuid, call);
  }
}

function handleDtmfTimeout(uuid, call) {
  const cd = call.callDetails;
  log.info(`DTMF timeout on ${uuid}`, { transactionId: cd.transactionId });
  transitionCallState(uuid, call, S.DONE, 'dtmf-timeout');
  stopDetectors(uuid);
  stopListenStreaming(uuid, call, 'dtmf-timeout');
  fsApi('uuid_kill', uuid);
  if (!call.dispositionSent) { call.dispositionSent = true;
    reportDisposition({ transactionid: cd.transactionId, from: cd.numberFrom, to: cd.numberTo, disposition: 'NOVMNOINPUT' });
  }
}

// ── Bridge to destination (continue flow) ────────────────────────────────────

function bridgeToDestination(uuid, call) {
  transitionCallState(uuid, call, S.TRANSFERRING, 'bridge-start');
  stopDetectors(uuid);
  stopListenStreaming(uuid, call, 'bridging');
  // Clear the safety-net timer — transferred calls stay up as long as needed
  if (call.maxTimer) { clearTimeout(call.maxTimer); call.maxTimer = null; }

  const cd = call.callDetails;

  // Resolve caller ID (same logic as IVR-app)
  let callerId = cd.numberFrom;
  try { const p = parsePhoneNumberFromString(cd.numberTo); if (p) callerId = p.number; }
  catch { /* fallback */ }
  try { if (callerId === cd.numberFrom) { const p = parsePhoneNumberFromString(cd.numberFrom); if (p) callerId = p.number; } }
  catch { /* fallback */ }

  // Format bridge destination
  const dest = formatBridgeDest(cd);

  log.info(`Bridging ${uuid} → ${cd.destinationAddress}`, { transactionId: cd.transactionId, bridgeDest: dest, callerId });

  fsApi('uuid_setvar', `${uuid} hangup_after_bridge true`);
  fsApi('uuid_setvar', `${uuid} effective_caller_id_number ${callerId}`);
  fsApi('uuid_setvar', `${uuid} effective_caller_id_name ${callerId}`);
  executeUuidApp(uuid, 'bridge', dest, call, 'bridge');
}

function formatBridgeDest(cd) {
  const rawDest = String(cd.destinationAddress || '').trim();
  const hasGateway = Boolean(config.freeswitch.gateway);
  const hasTrunkIps = Array.isArray(config.trunkIps) && config.trunkIps.length > 0;

  // When SIP_GATEWAY is not set, SIP_TRUNK_IPS fully controls routing and overrides
  // any host found in payload destinationAddress/carrierAddress style values.
  const forceTrunkOverride = !hasGateway && hasTrunkIps;
  const [userPart = '', hostPart = ''] = rawDest.split('@');

  // Keep raw SIP URI destinations only when explicit gateway mode is enabled.
  if (!forceTrunkOverride && rawDest && hostPart) {
    return `sofia/${config.freeswitch.profile}/${rawDest}`;
  }

  // PSTN number (or user part of sip:user@host when forceTrunkOverride=true)
  const candidate = forceTrunkOverride && userPart ? userPart : rawDest;
  let num = candidate.replace(/^\+/, '');
  try { const p = parsePhoneNumberFromString(candidate); if (p) num = p.number.replace(/^\+/, ''); } catch {}
  const prefix = (cd.prefix || config.calls.defaultPrefix || '').trim();
  if (prefix && !num.startsWith(prefix)) num = '#' + prefix + num; // '#' prefix same as IVR-app usePlusSign=true

  if (hasGateway) return `sofia/gateway/${config.freeswitch.gateway}/${num}`;
  const trunk = ipRotator.next();
  if (!trunk) {
    log.error('No SIP_TRUNK_IPS available for bridge', {
      transactionId: cd.transactionId,
      destinationAddress: rawDest,
    });
    return `sofia/${config.freeswitch.profile}/${num}`;
  }
  return `sofia/${config.freeswitch.profile}/${num}@${trunk}`;
}

// ── Channel hangup → final cleanup ──────────────────────────────────────────
async function onChannelHangup(evt) {
  const uuid  = evt.getHeader('Unique-ID');
  const cause = evt.getHeader('Hangup-Cause') || 'UNKNOWN';
  const role  = classifyChannel(evt);
  const sipCode = evt.getHeader('variable_sip_term_status') ||
                  evt.getHeader('variable_last_bridge_proto_specific_hangup_cause');

  // ── Agent leg hung up — clean up cross-reference maps ────────────────────
  if (role === 'agent_leg') {
    const customerUuid = agentLegToCustomer.get(uuid) || '';
    log.info(`Agent leg hangup ${uuid}`, { role, cause, customerUuid });
    agentLegToCustomer.delete(uuid);
    if (customerUuid) customerToAgentLeg.delete(customerUuid);
    return;
  }

  // ── Customer leg (inbound or outbound) — existing cleanup ────────────────
  log.debug(`Customer leg hangup ${uuid}`, { role, cause });

  // === Resolve any pending bridge promise to prevent memory leaks ===
  const pending = bridgePromises.get(uuid);
  if (pending) {
    pending.resolve({ answered: false, cause: cause || 'HANGUP' });
    bridgePromises.delete(uuid);
    log.debug(`Resolved pending bridge promise on hangup for ${uuid}`, { cause });
  }

  const call = activeCalls.get(uuid);

  if (call?.assignedAgentId) {
    await releaseAssignedAgent(call.assignedAgentId);
  }

  const callAfterUntrack = untrackCall(uuid);
  if (!callAfterUntrack && !call) return;

  const logCall = call || callAfterUntrack;

  stopListenStreaming(uuid, logCall, 'hangup');

  log.info(`Hangup ${uuid}: ${cause}`, {
    role,
    transactionId: logCall?.callDetails?.transactionId,
    to:            logCall?.callDetails?.numberTo,
    state:         logCall?.state,
    sipCode,
    assignedAgent: call?.assignedAgentId || 'none',
    agentLegUuid:  call?.agentLegUuid    || 'none',
  });

  await updateCallRouteRecord(uuid, {
    endedAt:     new Date().toISOString(),
    finalCause:  cause,
    disposition: logCall?.dispositionSent ? 'SENT' : 'FALLBACK',
  });

  if (logCall && !logCall.dispositionSent) {
    let d;
    if (cause === 'USER_BUSY')                                          d = 'USER_BUSY';
    else if (logCall.state === S.RINGING || logCall.state === S.DONE)  d = 'NO_ANSWER';
    else                                                                d = 'NOVMNOINPUT';

    log.info(`Sending fallback disposition for ${uuid}`, { disposition: d, cause });
    reportDisposition({
      transactionid: logCall.callDetails?.transactionId,
      from:          logCall.callDetails?.numberFrom,
      to:            logCall.callDetails?.numberTo,
      disposition:   d,
    });
  }
}


// ═══════════════════════════════════════════════════════════════════════════════
//  CALL ORIGINATION  (with IP rotation)
// ═══════════════════════════════════════════════════════════════════════════════

function formatDestNumber(cd) {
  let num = cd.numberTo;
  try { const p = parsePhoneNumberFromString(num); if (p) num = p.number; } catch {}
  num = num.replace(/^\+/, '');
  const prefix = (cd.prefix || config.calls.defaultPrefix || '').trim();
  if (prefix && !num.startsWith(prefix)) num = prefix + num;
  return num;
}

async function originateCall(callDetails) {
  const uuid = crypto.randomUUID();
  const dest = formatDestNumber(callDetails);
  const from = callDetails.numberFrom;

  const vars = [
    `origination_uuid=${uuid}`,
    `origination_caller_id_number=${from}`,
    `origination_caller_id_name=${from}`,
    `originate_timeout=${config.freeswitch.originateTimeout}`,
    `ignore_early_media=true`,
    `call_role=outbound_customer`,
  ];
  if (config.freeswitch.codecString) vars.push(`absolute_codec_string='${config.freeswitch.codecString}'`);

  // Endpoint: named gateway OR rotate through trunk IPs
  let endpoint, trunkUsed;
  if (config.freeswitch.gateway) {
    endpoint = `sofia/gateway/${config.freeswitch.gateway}/${dest}`;
    trunkUsed = config.freeswitch.gateway;
  } else {
    const trunk = ipRotator.next();
    if (!trunk) { log.error(`No trunk for ${dest}`); rateLimiter.replenish(); releaseCallLock(callDetails.numberTo, callDetails.prefix); return null; }
    endpoint = `sofia/${config.freeswitch.profile}/${dest}@${trunk}`;
    trunkUsed = trunk;
  }
  trackCall(uuid, callDetails);

  // Fix 1: Create initial call route record in Redis
  await saveCallRouteRecord({
    transactionId: callDetails.transactionId,
    customerUuid: uuid,
    direction: callDetails.callType === 'inbound' ? 'inbound' : 'outbound',
    createdAt: new Date().toISOString()
  });

  log.info(`Originate ${uuid} → ${dest} via ${trunkUsed}`, { 
    transactionId: callDetails.transactionId, 
    from 
  });
  eslConn.bgapi('originate', `{${vars.join(',')}}${endpoint} &park()`, (evt) => {
    const reply = (evt.getHeader('Reply-Text') || '').trim();
    if (reply.includes('Job-UUID')) {
      const jid = reply.split('Job-UUID:')[1]?.trim();
      if (jid) pendingJobs.set(jid, uuid);
    } else if (reply.startsWith('-ERR')) {
      log.error(`Originate instant fail ${uuid}: ${reply}`);
      const c = untrackCall(uuid);
      if (c) { rateLimiter.replenish(); releaseCallLock(callDetails.numberTo, callDetails.prefix); }
    }
  });
  return uuid;
}

// ═══════════════════════════════════════════════════════════════════════════════
//  CALL PROCESSOR  (dedup → caller-ID → rate-limit → originate)
// ═══════════════════════════════════════════════════════════════════════════════

async function processCallMessage(cd) {
  // 1. Resolve Caller-ID from Redis per company
  cd.isAgentCall = cd.isAgentCall === true || cd.routeToAgent === true || false;
  if (cd.companyId) {
    const resolved = await getRandomFromNumber(cd.companyId);
    if (resolved) { log.info(`CID ${resolved} for company ${cd.companyId}`, { transactionId: cd.transactionId }); cd.numberFrom = resolved; }
  }

  // 2. Dedup
  if (!(await acquireCallLock(cd.numberTo, cd.transactionId, cd.prefix))) return;

  // 3. Rate-limit
  if (!(await rateLimiter.acquire())) {
    log.warn(`Rate timeout ${cd.numberTo}`, { transactionId: cd.transactionId });
    await releaseCallLock(cd.numberTo, cd.prefix); return;
  }

  // 4. ESL must be up
  if (!eslConnected) {
    log.error('ESL down — skip originate'); rateLimiter.replenish();
    await releaseCallLock(cd.numberTo, cd.prefix); return;
  }

  // 5. Originate
  originateCall(cd);
}

// ═══════════════════════════════════════════════════════════════════════════════
//  RABBITMQ CONSUMER
// ═══════════════════════════════════════════════════════════════════════════════

let mqConn = null, mqChannel = null;
let isShuttingDown = false;
let isShutdownInProgress = false;

function safeAck(msg, meta = {}) {
  if (!msg || !mqChannel || isShuttingDown) return;
  try {
    mqChannel.ack(msg);
  } catch (e) {
    log.warn(`Skip ack: ${e.message}`, meta);
  }
}

function safeNack(msg, requeue, meta = {}) {
  if (!msg || !mqChannel || isShuttingDown) return;
  try {
    mqChannel.nack(msg, false, requeue);
  } catch (e) {
    log.warn(`Skip nack: ${e.message}`, { requeue, ...meta });
  }
}

async function initRabbitMQ() {
  mqConn = await amqplib.connect(config.rabbitmq.uri, { heartbeat: 60 });
  mqConn.on('error', (e) => log.error(`RabbitMQ error: ${e.message}`));
  mqConn.on('close', () => {
    mqChannel = null;
    if (isShuttingDown) {
      log.info('RabbitMQ connection closed during shutdown');
      return;
    }
    log.warn('RabbitMQ closed — reconnecting');
    setTimeout(() => initRabbitMQ().catch(e => log.error(`RMQ reconnect fail: ${e.message}`)), config.rabbitmq.reconnectMs);
  });

  mqChannel = await mqConn.createChannel();
  await mqChannel.prefetch(config.rabbitmq.prefetch);
  const qo = config.rabbitmq.queueType === 'quorum' ? { durable: true, arguments: { 'x-queue-type': 'quorum' } } : { durable: true };
  await mqChannel.assertQueue(config.rabbitmq.queue, qo);
  log.info(`Consuming "${config.rabbitmq.queue}" (prefetch ${config.rabbitmq.prefetch}, ${config.calls.ratePerSecond} CPS)`);

  mqChannel.consume(config.rabbitmq.queue, async (msg) => {
    if (!msg) return;
    if (isShuttingDown) return;
    let cd;
    try {
      cd = JSON.parse(msg.content.toString());
    } catch (e) {
      log.error(`Parse fail: ${e.message}`);
      safeAck(msg, { reason: 'invalid-json' });
      return;
    }
    if (!cd.numberTo || !cd.numberFrom || !cd.transactionId) {
      log.error('Invalid msg — missing fields');
      safeAck(msg, { reason: 'missing-fields' });
      return;
    }
    log.info(`Received → ${cd.numberTo}`, { transactionId: cd.transactionId, from: cd.numberFrom });
    try {
      await processCallMessage(cd);
      safeAck(msg, { transactionId: cd.transactionId });
    } catch (e) {
      log.error(`Process fail ${cd.transactionId}: ${e.message}`);
      safeNack(msg, true, { transactionId: cd.transactionId });
    }
  });
}

async function initRabbitMQWithRetry() {
  while (!isShuttingDown) {
    try {
      await initRabbitMQ();
      return;
    } catch (e) {
      log.warn('RabbitMQ init failed — retrying in background', {
        error: e.message,
        retryInMs: config.rabbitmq.reconnectMs,
      });
      await sleep(config.rabbitmq.reconnectMs);
    }
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
//  HEALTH / READINESS  (K8s probes)
// ═══════════════════════════════════════════════════════════════════════════════

const healthServer = http.createServer(async (req, res) => {
  if (await handleFreeswitchDirectoryApi(req, res)) return;
  if (await handleTestCallApi(req, res)) return;

  if (req.url === '/healthz' || req.url === '/health') {
    const ok = eslConnected && (redisClient?.isReady ?? false);
    res.writeHead(ok ? 200 : 503, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: ok ? 'ok' : 'degraded', esl: eslConnected,
      redis: redisClient?.isReady ?? false, activeCalls: activeCalls.size, uptime: process.uptime() }));
    return;
  }
  if (req.url === '/metrics') {
    const m = process.memoryUsage();
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end(`# HELP active_calls Current in-flight calls\n# TYPE active_calls gauge\nactive_calls ${activeCalls.size}\n` +
      `# HELP heap_used_bytes Heap used\n# TYPE heap_used_bytes gauge\nheap_used_bytes ${m.heapUsed}\n` +
      `# HELP rss_bytes RSS\n# TYPE rss_bytes gauge\nrss_bytes ${m.rss}\n`);
    return;
  }
  res.writeHead(404); res.end();
});

// ═══════════════════════════════════════════════════════════════════════════════
//  MEMORY MONITOR
// ═══════════════════════════════════════════════════════════════════════════════

const memMonitor = setInterval(() => {
  const m = process.memoryUsage(), h = Math.round(m.heapUsed/1024/1024), r = Math.round(m.rss/1024/1024);
  log.info(`Memory: heap=${h}MB rss=${r}MB active=${activeCalls.size} pending=${pendingJobs.size}`);
  if (r > 1024 && global.gc) { log.warn(`High RSS ${r}MB — GC`); global.gc(); }
}, 300_000);

// ═══════════════════════════════════════════════════════════════════════════════
//  MAIN / SHUTDOWN
// ═══════════════════════════════════════════════════════════════════════════════

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function readRequestBody(req, maxBytes) {
  return new Promise((resolve, reject) => {
    let total = 0;
    const chunks = [];
    req.on('data', (chunk) => {
      total += chunk.length;
      if (total > maxBytes) {
        reject(new Error('payload-too-large'));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => {
      const raw = Buffer.concat(chunks).toString('utf8');
      resolve(raw);
    });
    req.on('error', (err) => reject(err));
  });
}

async function readRequestJson(req, maxBytes) {
  const raw = await readRequestBody(req, maxBytes);
  try {
    return raw ? JSON.parse(raw) : {};
  } catch {
    throw new Error('invalid-json');
  }
}

function validateTestCallPayload(body) {
  const required = [
    'numberTo',
    'numberFrom',
    'transactionId',
    'wavUrlAnnounce',
    'wavUrlVM',
    'wavUrlContinue',
    'wavUrlOptOut',
    'digitContinue',
    'digitOptOut',
    'destinationAddress',
  ];
  const missing = required.filter((k) => {
    const v = body?.[k];
    return v == null || String(v).trim() === '';
  });
  return { ok: missing.length === 0, missing };
}

function xmlEscape(value) {
  return String(value == null ? '' : value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;');
}

function buildEmptyDirectoryXml(domain = '') {
  const safeDomain = xmlEscape(domain || 'default');
  return `<?xml version="1.0" encoding="UTF-8"?>
<document type="freeswitch/xml">
  <section name="directory">
    <domain name="${safeDomain}"/>
  </section>
</document>`;
}

function buildDirectoryUserXml({ domain, extension, authName, authValue, userContext, callerIdName, callerIdNumber }) {
  const safeDomain = xmlEscape(domain || 'default');
  const safeExtension = xmlEscape(extension);
  const safeContext = xmlEscape(userContext || config.directoryApi.userContext);
  const safeCallerIdName = xmlEscape(callerIdName || extension);
  const safeCallerIdNumber = xmlEscape(callerIdNumber || extension);
  const safeAuthName = xmlEscape(authName);
  const safeAuthValue = xmlEscape(authValue);

  return `<?xml version="1.0" encoding="UTF-8"?>
<document type="freeswitch/xml">
  <section name="directory">
    <domain name="${safeDomain}">
      <user id="${safeExtension}">
        <params>
          <param name="${safeAuthName}" value="${safeAuthValue}"/>
        </params>
        <variables>
          <variable name="user_context" value="${safeContext}"/>
          <variable name="effective_caller_id_name" value="${safeCallerIdName}"/>
          <variable name="effective_caller_id_number" value="${safeCallerIdNumber}"/>
          <variable name="dial-string" value="{sip_invite_domain=${safeDomain}}\${sofia_contact(\${dialed_user}@\${dialed_domain})}"/>
        </variables>
      </user>
    </domain>
  </section>
</document>`;
}

function sendXmlResponse(res, statusCode, xmlBody) {
  res.writeHead(statusCode, { 'Content-Type': 'text/xml; charset=utf-8' });
  res.end(xmlBody);
}

function parseBasicAuth(req) {
  const header = String(req.headers.authorization || '');
  if (!header.toLowerCase().startsWith('basic ')) return null;
  const encoded = header.slice(6).trim();
  if (!encoded) return null;
  try {
    const decoded = Buffer.from(encoded, 'base64').toString('utf8');
    const idx = decoded.indexOf(':');
    if (idx < 0) return null;
    return { user: decoded.slice(0, idx), pass: decoded.slice(idx + 1) };
  } catch {
    return null;
  }
}

function normalizeDirectoryParams(raw, requestUrl) {
  const params = { ...raw };
  for (const [k, v] of requestUrl.searchParams.entries()) {
    if (params[k] == null || params[k] === '') params[k] = v;
  }

  const section = String(params.section || '').toLowerCase();
  const tagName = String(params.tag_name || '').toLowerCase();
  const keyName = String(params.key_name || '').toLowerCase();
  const keyValue = String(params.key_value || '').trim();

  let domain = String(
    params.domain ||
    params.domain_name ||
    params.sip_auth_realm ||
    params.variable_domain_name ||
    ''
  ).trim();

  if (!domain && tagName === 'domain' && keyName === 'name') {
    domain = keyValue;
  }

  let extension = String(
    params.user ||
    params.userid ||
    params.sip_auth_username ||
    params.variable_sip_auth_username ||
    ''
  ).trim();

  if (!extension && tagName === 'user' && ['id', 'user', 'name'].includes(keyName)) {
    extension = keyValue;
  }
  if (!extension && keyName === 'user') {
    extension = keyValue;
  }

  if (extension.includes('@')) {
    const [userPart, domainPart] = extension.split('@');
    extension = userPart;
    if (!domain) domain = domainPart;
  }

  return { params, section, tagName, keyName, keyValue, domain, extension };
}

function isDirectoryRequestAuthorized(req, requestUrl) {
  if (!config.directoryApi.authToken) return true;

  const tokenFromHeader = String(req.headers['x-fs-auth'] || req.headers['x-directory-token'] || '');
  if (tokenFromHeader && tokenFromHeader === config.directoryApi.authToken) return true;

  const tokenFromQuery = String(requestUrl.searchParams.get('token') || '');
  if (tokenFromQuery && tokenFromQuery === config.directoryApi.authToken) return true;

  const basic = parseBasicAuth(req);
  if (basic && basic.user === config.directoryApi.authUser && basic.pass === config.directoryApi.authToken) {
    return true;
  }
  return false;
}

async function lookupDirectoryUser(extension) {
  if (!extension) return null;

  const ext = String(extension).trim();
  if (!/^[a-zA-Z0-9_.-]{1,64}$/.test(ext)) return null;

  let agentId = await getAgentIdByExtension(ext);
  let agentHash = {};
  if (agentId) {
    agentHash = await redisClient.hGetAll(`agent:${agentId}`);
  }

  // Optional external provisioning keys for admin-created SIP users.
  const fsDirHash = await redisClient.hGetAll(`fsdir:user:${ext}`).catch(() => ({}));
  const directoryHash = await redisClient.hGetAll(`directory:user:${ext}`).catch(() => ({}));

  const merged = { ...directoryHash, ...fsDirHash, ...agentHash };
  if (!agentId && merged.agentId) agentId = String(merged.agentId);

  const a1Hash =
    merged.a1Hash ||
    merged.a1_hash ||
    '';

  const password =
    merged.sipPassword ||
    merged.sip_password ||
    merged.password ||
    config.directoryApi.defaultPassword ||
    '';

  if (!a1Hash && !password) {
  log.error(`lookupDirectoryUser: extension "${ext}" found in Redis but has NO password/a1Hash`, {
    redisKeys: {
      fsdir: Object.keys(fsDirHash),
      directory: Object.keys(directoryHash),
      agent: Object.keys(agentHash)
    },
    hint: 'Set FS_DIRECTORY_DEFAULT_PASSWORD env var or store sipPassword in agent Redis hash',
  });
  return null;
}

  return {
    extension: ext,
    agentId: agentId || '',
    authName: a1Hash ? 'a1-hash' : 'password',
    authValue: a1Hash || password,
    userContext: merged.userContext || merged.user_context || config.directoryApi.userContext,
    callerIdName: merged.callerIdName || merged.caller_id_name || ext,
    callerIdNumber: merged.callerIdNumber || merged.caller_id_number || ext,
  };
}

async function handleFreeswitchDirectoryApi(req, res) {
  const requestUrl = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
  const method = String(req.method || 'GET').toUpperCase();
  if (!['GET', 'POST'].includes(method) || requestUrl.pathname !== config.directoryApi.path) {
    return false;
  }

  let rawBody = '';
  if (method === 'POST') {
    try {
      rawBody = await readRequestBody(req, config.directoryApi.maxBodyBytes);
    } catch (e) {
      const xml = buildEmptyDirectoryXml('default');
      sendXmlResponse(res, e.message === 'payload-too-large' ? 413 : 400, xml);
      return true;
    }
  }

  const contentType = String(req.headers['content-type'] || '').toLowerCase();
  let bodyParams = {};
  if (contentType.includes('application/json')) {
    try {
      bodyParams = rawBody ? JSON.parse(rawBody) : {};
    } catch {
      sendXmlResponse(res, 400, buildEmptyDirectoryXml('default'));
      return true;
    }
  } else {
    const parsed = new URLSearchParams(rawBody || '');
    for (const [k, v] of parsed.entries()) bodyParams[k] = v;
  }

  const normalized = normalizeDirectoryParams(bodyParams, requestUrl);
  // ✅ ADD THIS HERE
log.debug('Directory API request received', {
  method,
  section: normalized.section,
  tagName:  normalized.tagName,
  keyName:  normalized.keyName,
  keyValue: normalized.keyValue,
  domain:   normalized.domain,
  extension: normalized.extension,
  remote:   req.socket?.remoteAddress || '',
});

  if (!isDirectoryRequestAuthorized(req, requestUrl)) {
    log.warn('Directory API unauthorized request', {
      section: normalized.section,
      tagName: normalized.tagName,
      keyName: normalized.keyName,
      keyValue: normalized.keyValue,
      remote: req.socket?.remoteAddress || '',
    });
    sendXmlResponse(res, 403, buildEmptyDirectoryXml(normalized.domain));
    return true;
  }

  if (normalized.section !== 'directory') {
    sendXmlResponse(res, 200, buildEmptyDirectoryXml(normalized.domain));
    return true;
  }

  if (!normalized.extension) {
    sendXmlResponse(res, 200, buildEmptyDirectoryXml(normalized.domain));
    return true;
  }

  const user = await lookupDirectoryUser(normalized.extension);
  if (!user) {
    log.info(`Directory lookup miss for extension ${normalized.extension}`, { domain: normalized.domain || 'default' });
    sendXmlResponse(res, 200, buildEmptyDirectoryXml(normalized.domain));
    return true;
  }

  const xml = buildDirectoryUserXml({
    domain: normalized.domain || 'default',
    extension: user.extension,
    authName: user.authName,
    authValue: user.authValue,
    userContext: user.userContext,
    callerIdName: user.callerIdName,
    callerIdNumber: user.callerIdNumber,
  });

  log.info(`Directory lookup hit for extension ${user.extension}`, {
    domain: normalized.domain || 'default',
    agentId: user.agentId || '',
    auth: user.authName,
  });
  sendXmlResponse(res, 200, xml);
  return true;
}

async function handleTestCallApi(req, res) {
  if (!config.ingestApi.enabled || req.method !== 'POST' || req.url !== config.ingestApi.path) {
    return false;
  }

  if (config.ingestApi.apiKey) {
    const provided = req.headers['x-api-key'];
    if (!provided || String(provided) !== config.ingestApi.apiKey) {
      res.writeHead(401, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'unauthorized' }));
      return true;
    }
  }

  let payload;
  try {
    payload = await readRequestJson(req, config.ingestApi.maxBodyBytes);
  } catch (e) {
    const code = e.message === 'payload-too-large' ? 413 : 400;
    res.writeHead(code, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: e.message }));
    return true;
  }

  const check = validateTestCallPayload(payload);
  if (!check.ok) {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'missing-required-fields', missing: check.missing }));
    return true;
  }

  log.info('Test-call API received request', {
    transactionId: payload.transactionId,
    numberTo: payload.numberTo,
  });

  try {
    await processCallMessage(payload);
    res.writeHead(202, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'accepted',
      message: 'Call queued for processing',
      transactionId: payload.transactionId,
      numberTo: payload.numberTo,
    }));
  } catch (e) {
    log.error(`Test-call API processing failed: ${e.message}`, {
      transactionId: payload.transactionId,
    });
    res.writeHead(500, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'internal-error' }));
  }
  return true;
}

async function main() {
  log.info('═══ FreeSWITCH IVR Dialer starting ═══', {
    cps: config.calls.ratePerSecond, 
    fsHost: config.freeswitch.host,
    trunkIps: config.trunkIps, 
    queue: config.rabbitmq.queue,
    gateway: config.freeswitch.gateway || '(IP rotation)',
    beepDetection: {
      avmd: config.detection.enableAvmd,
      vmd: config.detection.enableVmd,
      requireDualSignal: config.detection.requireDualSignal,
    },
    listenStreaming: {
      enabled: config.listen.enabled,
      probability: config.listen.probability,
      ips: config.listen.ips,
    },
    ingestApi: {
      enabled: config.ingestApi.enabled,
      path: config.ingestApi.path,
      auth: config.ingestApi.apiKey ? 'x-api-key' : 'none',
    },
    directoryApi: {
      path: config.directoryApi.path,
      auth: config.directoryApi.authToken ? `basic/x-fs-auth (${config.directoryApi.authUser})` : 'none',
      userContext: config.directoryApi.userContext,
      defaultPassword: config.directoryApi.defaultPassword ? 'set' : 'unset',
    },
    dispositionRetry: {
      maxRetries: config.api.dispositionMaxRetries,
      baseMs: config.api.dispositionRetryBaseMs,
      maxMs: config.api.dispositionRetryMaxMs,
      concurrency: config.api.dispositionConcurrency,
      queueMax: config.api.dispositionQueueMax,
    },
    // === NEW: Agent Pool Settings (for browser agents) ===
    agent: {
      portalWsPort: config.agent.portalWsPort,
      poolStrategy: config.agent.poolStrategy,
    }
  });

  // === Existing startup sequence ===
  startMediaDetectorServer();
  await initRedis();

  // Start HTTP endpoints early so FreeSWITCH directory auth is available
  // even if ESL connectivity is delayed or temporarily unavailable.
  healthServer.listen(config.health.port, '0.0.0.0', () =>
    log.info(`Health server listening on :${config.health.port}`)
  );

  // Start portal WS immediately after Redis is ready so browser WS clients
  // can connect without waiting for ESL/FreeSWITCH readiness.
  startPortalWebSocket();

  await connectESLWithRetry();

  // Keep SIP/portal WS available immediately even if RabbitMQ is still starting.
  initRabbitMQWithRetry().catch((e) => {
    log.error(`RabbitMQ background init crashed: ${e.message}`, { stack: e.stack });
  });
  // startSipRegistrationSync(); // Uncomment later when you create this module

  log.info(`═══ Running — ${config.calls.ratePerSecond} calls/sec, rotating ${config.trunkIps.length} trunk IPs ═══`);
  log.info(`Agent Portal WS ready on port ${config.agent.portalWsPort}`);
}

async function shutdown(sig) {
  if (isShutdownInProgress) return;
  isShutdownInProgress = true;
  isShuttingDown = true;
  log.info(`${sig} — shutting down (${activeCalls.size} active)`);
  clearInterval(memMonitor);
  try { if (mqChannel) await mqChannel.close(); } catch {}
  try { if (mqConn) await mqConn.close(); } catch {}
  for (const [uuid, call] of activeCalls) {
    if (call.state !== S.TRANSFERRING) { try { eslConn?.api('uuid_kill', uuid, ()=>{}); } catch {} }
  }
  await sleep(1000);
  await drainDispositionQueue(config.api.dispositionDrainTimeoutMs);
  if (dispositionPumpTimer) { clearTimeout(dispositionPumpTimer); dispositionPumpTimer = null; }
  try { if (eslConn) eslConn.disconnect(); } catch {}
  try { if (redisClient) await redisClient.quit(); } catch {}
  try { stopMediaDetectorServer(); } catch {}
  try { healthServer.close(); } catch {}
  log.info('Shutdown complete'); process.exit(0);
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

main().catch(e => { log.error(`Fatal: ${e.message}`, { stack: e.stack }); process.exit(1); });

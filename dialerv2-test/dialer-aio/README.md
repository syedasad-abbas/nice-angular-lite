# FreeSWITCH Dialer

Node.js outbound dialer that replaces `jambonz-ivr-app` with direct FreeSWITCH ESL control.

It supports:
- RabbitMQ-driven outbound call creation
- Redis dedupe + Redis caller ID rotation
- 30 CPS token-bucket rate limiting (configurable)
- Concurrent IVR gather + voicemail beep detection
- Node-side DSP beep detection (with silence gating)
- Random audio sampling stream mirror
- VM drop / continue transfer / opt-out / no-input dispositions
- Kubernetes-friendly health + metrics + graceful shutdown

---

## 1) How it works (brief)

1. Consume `CallDetails` jobs from RabbitMQ.
2. Deduplicate with Redis (`answered-call` and `inflight-call` keys).
3. Apply CPS token bucket.
4. Originate via FreeSWITCH ESL (`bgapi originate ... &park()`).
5. On answer:
   - Start `play_and_get_digits` (announcement + DTMF gather).
   - Start node DSP detection via `uuid_audio_fork` to local WS server.
   - Optionally arm random sampling mirror to an external WS listener.
6. If beep is detected:
   - Break current gather/play.
   - Play `wavUrlVM`.
   - Hangup and report `VM`.
7. If DTMF received:
   - Continue digit -> play continue WAV -> bridge transfer.
   - Opt-out digit -> play opt-out WAV -> hangup.
   - Invalid digit -> replay gather.
   - Timeout -> hangup with `NOVMNOINPUT`.

Transferred calls are not force-killed by non-transfer safety logic.

---

## 2) FreeSWITCH module requirements

### Required modules

- `mod_event_socket`
  - Required for ESL (`modesl`) control.
- `mod_sofia`
  - SIP signaling for originate and bridge.
- `mod_commands`
  - Needed for API/bgapi commands used by the script.
- `mod_dptools`
  - Needed for `playback`, `bridge`, `play_and_get_digits`, etc.
- `mod_audio_fork`
  - Required for node DSP audio feed (`uuid_audio_fork`).
- `mod_http_cache`
  - Required to use `http_cache://` prompt URLs efficiently.

### Optional/fallback detection modules

- `mod_avmd`
  - Optional fallback beep signal source.
- `mod_vmd`
  - Optional fallback beep signal source.

By default, this dialer uses node-side DSP as primary and keeps AVMD/VMD disabled unless enabled by env.

### Verify modules

Use `fs_cli`:

```bash
fs_cli -x "show modules" | egrep "mod_event_socket|mod_sofia|mod_commands|mod_dptools|mod_audio_fork|mod_http_cache|mod_avmd|mod_vmd"
```

---

## 3) Install and run

From `freeswitch-dialer/`:

```bash
npm install
npm start
```

Or dev mode:

```bash
npm run start:dev
```

---

## 4) Environment variables

### Core

- `FREESWITCH_HOST` (default `127.0.0.1`)
- `FREESWITCH_PORT` (default `8021`)
- `FREESWITCH_PASSWORD` (default `ClueCon`)
- `RABBITMQ_URI` (default `amqp://guest:guest@localhost:5672`)
- `RABBITMQ_CALLS_QUEUE` (default `calls_queue`)
- `REDIS_URI` (default `redis://localhost:6379`)
- `CALL_STATUS_API_BASE_URL` (default `https://portal.dialytica.com/api`)
- `CALL_STATUS_API_TIMEOUT` (default `5000`)
- `DISPOSITION_MAX_RETRIES` (default `8`)
- `DISPOSITION_RETRY_BASE_MS` (default `1000`)
- `DISPOSITION_RETRY_MAX_MS` (default `60000`)
- `DISPOSITION_RETRY_JITTER_MS` (default `500`)
- `DISPOSITION_QUEUE_MAX` (default `50000`)
- `DISPOSITION_CONCURRENCY` (default `16`)
- `DISPOSITION_DRAIN_TIMEOUT_MS` (default `15000`)
- `CALLS_PER_SECOND` (default `30`)

### Routing / trunking

- `SIP_GATEWAY` (if set, preferred route)
- `SIP_PROFILE` (default `external`)
- `SIP_TRUNK_IPS` (default `169.197.85.202,169.197.85.203,169.197.85.204`)
- `DEFAULT_NUMBER_PREFIX`

### IVR / prompts

- `DTMF_GATHER_TIMEOUT` (default `15`)
- `AUDIO_CACHE_PREFIX` (default `http_cache://`)

### Redis dedupe TTLs

- `ANSWERED_CALL_KEY_TTL` (default `14760`)
- `INFLIGHT_KEY_TTL` (default `120`)

### Node DSP beep detection (primary)

- `BEEP_NODE_DETECTOR_ENABLED` (default `true`)
- `BEEP_NODE_WS_HOST` (default `0.0.0.0`)
- `BEEP_NODE_WS_PORT` (default `8090`)
- `BEEP_NODE_SAMPLE_RATE` (default `8000`)
- `BEEP_NODE_WINDOW_MS` (default `40`)
- `BEEP_NODE_HOP_MS` (default `20`)
- `BEEP_NODE_ENERGY_GATE_RMS` (default `1500`)
- `BEEP_NODE_FREQ_MIN_HZ` (default `550`)
- `BEEP_NODE_FREQ_MAX_HZ` (default `1200`)
- `BEEP_NODE_FREQ_STEP_HZ` (default `25`)
- `BEEP_NODE_TONALITY_MIN` (default `0.25`)
- `BEEP_NODE_PURITY_MIN` (default `0.90`)
- `BEEP_NODE_PURITY_ACTIVE_RATIO` (default `0.30`)
- `BEEP_NODE_DETECT_START_MS` (default `1200`)
- `BEEP_NODE_RUN_FREQ_DELTA_HZ` (default `75`)
- `BEEP_NODE_RUN_GAP_MS` (default `60`)
- `BEEP_NODE_RUN_MIN_MS` (default `120`)
- `BEEP_NODE_RUN_MAX_MS` (default `3000`)
- `BEEP_NODE_FOLLOW_SILENCE_MS` (default `300`)
- `BEEP_NODE_FOLLOW_SILENCE_RATIO` (default `0.25`)
- `BEEP_NODE_CONFIDENCE_MIN` (default `0.50`)

### AVMD/VMD fallback (optional)

- `BEEP_AVMD_ENABLED` (default `false`)
- `BEEP_VMD_ENABLED` (default `false`)
- `BEEP_REQUIRE_DUAL_SIGNAL` (default `false`)
- `BEEP_MIN_MS_FROM_ANSWER` (default `3000`)
- `BEEP_QUORUM_WINDOW_MS` (default `2500`)
- `BEEP_SINGLE_SIGNAL_GRACE_MS` (default `12000`)
- `BEEP_CONFIRM_DELAY_MS` (default `250`)

### Random sampling stream (mirrored by Node detector)

- `LISTEN_ENABLED` (default `true`)
- `LISTEN_PROBABILITY` (default `0.2`)
- `LISTEN_IPS` (default IVR-app IP list):
  - `178.156.199.82,178.156.181.17,178.156.146.63,178.156.196.224,178.156.194.53,178.156.181.152,178.156.196.227,178.156.197.141`
- `LISTEN_WEBSOCKET_PORT` (default `8080`)
- `LISTEN_MIX_TYPE` (default `stereo`)
- `LISTEN_SAMPLE_RATE` (default `8000`)
- `LISTEN_MAX_LENGTH` (default `20`)

### Service / runtime

- `HEALTH_PORT` (default `3000`)
- `MAX_CALL_DURATION_MS` (default `180000`)
- `LOG_LEVEL` (default `info`)
- `TEST_CALL_API_ENABLED` (default `false`)
- `TEST_CALL_API_PATH` (default `/v1/test-call`)
- `TEST_CALL_API_KEY` (optional; if set, require `x-api-key` header)
- `TEST_CALL_API_MAX_BODY_BYTES` (default `262144`)

---

## 5) Kubernetes notes

- `GET /healthz` (or `/health`) for liveness/readiness.
- `GET /metrics` exposes simple Prometheus-style gauges.
- Run as non-root where possible.
- Keep `FREESWITCH_HOST` reachable from pod network.
- Set resource limits; DSP work scales with concurrent active calls.

---

## 6) Operational checks

- Verify ESL auth works:
  - `fs_cli -x "status"`
- Verify queue consumption logs appear.
- Verify one test call end-to-end:
  - answer -> gather starts
  - beep -> VM playback/hangup
  - disposition posted to call-status API
- Verify transfer path:
  - continue digit -> bridge -> call not prematurely killed

---

## 7) Test-call API (optional) + curl example

If you want to trigger test calls without publishing to RabbitMQ, enable the built-in ingest API:

```bash
export TEST_CALL_API_ENABLED=true
export TEST_CALL_API_KEY='replace-with-strong-key'
```

Then send a call job:

```bash
curl -X POST "http://127.0.0.1:3000/v1/test-call" \
  -H "Content-Type: application/json" \
  -H "x-api-key: replace-with-strong-key" \
  -d '{
    "transactionId": "98462165798465498",
    "companyId": "2",
    "numberFrom": "+12098574125",
    "numberTo": "+15555555555",
    "prefix": "305195",
    "digitContinue": "2",
    "digitOptOut": "9",
    "destinationAddress": "+12096554105",
    "wavUrlAnnounce": "https://rvm.nyc3.digitaloceanspaces.com/RVM/1729263781.wav",
    "wavUrlContinue": "https://rvm.nyc3.digitaloceanspaces.com/RVM/1718305362.wav",
    "wavUrlOptOut": "https://rvm.nyc3.digitaloceanspaces.com/RVM/1715365467.wav",
    "wavUrlVM": "https://rvm.nyc3.digitaloceanspaces.com/RVM/1733352752.wav"
  }'
```

Notes:
- If `TEST_CALL_API_KEY` is empty, auth header is not required.
- If `SIP_GATEWAY` is unset, outbound routing still uses `SIP_TRUNK_IPS` override logic.
- This API reuses the same dedupe, caller-id rotation, CPS, and originate flow as RabbitMQ jobs.

---

## 8) Notes on reliability and CPU

- Node DSP beep detection is primary to reduce dependence on FS AVMD/VMD behavior.
- AVMD/VMD can be enabled as supplemental sources if needed.
- If CPU pressure appears:
  - keep AVMD/VMD disabled
  - tune `LISTEN_PROBABILITY`
  - tune detector thresholds and sample rate
  - scale horizontally

# ADR-001: Dialer Architecture Boundaries

- Status: Accepted
- Date: 2026-04-21
- Scope: `webportal-ui`, `dialer-aio`, RabbitMQ, FreeSWITCH

## Context

The platform currently has campaign and calling capabilities across `webportal-ui` and `dialer-aio`.
To avoid overlap and unstable behavior, we need clear ownership boundaries for:

1. Control-plane responsibilities (users, campaigns, lead state, auth, UI).
2. Execution-plane responsibilities (origination, bridge, agent routing, call-time decisions).
3. Transport between control and execution planes.
4. Separation of manual click-to-call vs campaign dialing.

## Decision

### 1) Control Plane Ownership (`webportal-ui`)

`webportal-ui` is the control plane and owns:

1. Users, roles, permissions, auth.
2. Campaign and lead lifecycle state (DB source of truth).
3. Campaign mode selection and start/stop actions from UI.
4. Reporting views and operator workflows.

`webportal-ui` does not own in-call routing decisions.

### 2) Execution Plane Ownership (`dialer-aio`)

`dialer-aio` is the execution plane and owns:

1. Outbound call origination execution.
2. Answer-time bridge logic.
3. Inbound call routing to agents.
4. Outbound answered-call routing to logged-in agents.
5. Agent selection, lock, retry, cooldown, and queue-wait behavior.
6. Live routing eligibility based on Redis + SIP registration + portal presence.

`dialer-aio` does not own campaign business state.

### 3) Campaign Transport Ownership (RabbitMQ)

RabbitMQ is the required transport for campaign dialing jobs between control and execution planes.

1. `webportal-ui` publishes campaign call jobs.
2. `dialer-aio` consumes campaign call jobs.
3. Retries and buffering are queue-based; UI request lifecycle is decoupled from call execution lifecycle.

### 4) Manual vs Campaign Separation

Manual click-to-call and campaign dialing are separate flows:

1. Manual click-to-call is operator-triggered and remains independent of campaign queue flow.
2. Campaign dialing uses RabbitMQ job transport and execution by `dialer-aio`.

No direct UI-driven campaign originate path is allowed after full migration.

## Interfaces (boundary contracts)

1. `webportal-ui` -> RabbitMQ: campaign job payload contract.
2. `webportal-ui` -> `dialer-aio` WS: agent presence contract (`agent_presence`).
3. `dialer-aio` -> `webportal-ui`: call status/event callback contract.

Contracts are versioned and must be backward-compatible during rollout.

## Source of Truth

1. Campaign/lead truth: `webportal-ui` database.
2. Live routing eligibility truth: Redis data maintained by `dialer-aio`.
3. Telephony signaling/media truth: FreeSWITCH runtime state.

## Non-Goals

1. No campaign business workflow logic inside `dialer-aio`.
2. No direct campaign origination from UI request handlers.
3. No dual ownership of routing decisions.

## Rollout Control

Introduce an explicit mode toggle for campaign execution:

- `CAMPAIGN_MODE=individual|collective_dialerv2`

Intended usage:

1. `individual`: existing per-agent/manual-compatible campaign path.
2. `collective_dialerv2`: queue-driven campaign execution via RabbitMQ + `dialer-aio`.

## Consequences

### Positive

1. Clear ownership and reduced cross-service coupling.
2. Scalable, resilient campaign execution via queue buffering.
3. Safer inbound/outbound routing under one execution engine.

### Trade-offs

1. Requires stable message/event contracts.
2. Requires callback/event reconciliation for campaign lead finalization.
3. Adds operational dependency on RabbitMQ for campaign mode.

## Step 1 Completion Criteria

Step 1 is complete when all conditions are true:

1. Team sign-off confirms ownership boundaries in this ADR.
2. No conflicting flow remains in implementation plan documents.
3. Manual and campaign call paths are explicitly separated in design notes.
4. Queue-based campaign transport is approved as mandatory for `collective_dialerv2` mode.

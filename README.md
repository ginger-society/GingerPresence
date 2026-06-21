# Presence Service Design Notes

## Core Insight

The WAMP broker is a **NAT traversal layer**, not a pub/sub system. It gives devices behind NAT a stable authenticated address. Think of it as a secure private mailbox per client, not a broadcast channel.

## Communication Directions

- **Device → Service**: HTTP (registration, data push). Services are reachable by FQDN.
- **Service → Device**: WAMP. Services spin up a persistent connection per pod (unique `prefix-<uuid>_<token-sub>` channel) and call device RPCs directly.
- **Heartbeat**: WAMP. Device publishes over its already-open connection — no extra connection, no HTTP header overhead, just a small JSON payload over an existing WebSocket.

## Presence Service

A single dedicated Rust/Rocket instance. At startup it computes its channel name once:

```
presence-<uuid>_<token-sub>
```

and holds it in memory. That's the only state it owns.

**Registration endpoint** (`GET /presence/`): returns the channel name. Pure memory read, essentially free. Horizontally scaled Rocket instances can serve this — it's just a string in `Arc<State>`.

**Heartbeat handler** (`handle_heartbeat` RPC): device calls RPC on the presence channel with its own channel name and capabilities list. Presence service writes to Redis:

- `SET {device_channel}_capability <json array>` — permanent, updated on each heartbeat
- `SET {device_channel}_last_seen <timestamp> EX 15` — TTL key, drives expiry
- `SADD available_devices {device_channel}` — membership set for queries

Replies with `{"status": "ok"}`.

**Expiry watcher**: background task subscribes to Redis keyspace events (`__keyevent@0__:expired`). When a `_last_seen` key expires, acquires a `SET NX` lock (prevents duplicate cleanup across instances) and removes the device from `available_devices` and deletes its `_capability` key.

**Query endpoints**:

- `GET /presence/available-devices` — returns all online devices with their full capability lists
- `GET /presence/available-devices/by-capability?capability=<cap>` — returns channel names of devices matching a single capability, used by orchestrators to find a suitable runner before dispatching an RPC job

## Horizontal Scalability

Each presence instance gets a **unique channel** (`presence-<uuid>_<token-sub>`). Nginx load balances devices across instances at registration time — each device gets pinned to one instance's channel. This means:

- No write contention — each instance handles heartbeats only for its own devices
- No fanout — heartbeat RPCs go to exactly one instance
- Redis is the shared source of truth — all instances read from the same `available_devices` set and capability keys
- The only coordination needed is the `SET NX` lock in the expiry watcher, so only one instance cleans up a given expired device

On presence instance restart, devices get no ack → re-fetch channel via `GET /presence/` → get new instance's channel → resume. The device's re-fetch logic retries after 3 consecutive failures before calling the HTTP endpoint again, and skips heartbeat calls while the HTTP endpoint is also unreachable.

## Device State Machine

```
loop {
    channel = GET /presence/

    loop {
        call handle_heartbeat RPC on channel
        wait for ack
        if no ack → consecutive_failures++
        if consecutive_failures >= 3 → break   // channel is dead, re-fetch
        else → reset consecutive_failures
    }

    // skip heartbeat calls while presence HTTP is also down
    // retry GET /presence/ next tick
}
```

## Failure & Recovery

- **Normal pod restart**: Kubernetes `RollingUpdate` keeps liveness check — new pod up before old pod terminates. Channel never goes dark. Devices never notice.
- **Unexpected crash**: Devices get no ack → 3 consecutive failures → `GET /presence/` → new channel → resume. Registration is HTTP so it's reachable even if WAMP broker is flaky.
- **Presence HTTP also down**: Device skips heartbeat entirely, keeps retrying `GET /presence/` each tick. No thundering herd on the dead channel.
- **Redis TTL**: Devices that go offline naturally expire (`_last_seen` TTL = 15s). Expiry watcher cleans up `available_devices` and `_capability` keys automatically.
- **Token rotation**: New token → new channel name → devices get no ack → re-register → get new channel. Deliberate operation, controllable rollout.

## Security

Authentication is handled entirely at the broker/transport layer. If a message arrives on the presence channel, it came from an authenticated client — **no JWT parsing, no signature verification needed in application code**. The presence service just trusts the payload.

## What Each Layer Does

| Layer | Responsibility |
|---|---|
| WAMP broker | Authentication, secure delivery, NAT traversal |
| Nginx | Load balancing devices across presence instances |
| Kubernetes | Availability, zero-downtime rollout |
| Redis TTL | Presence state, automatic expiry |
| Redis SET NX | Cleanup deduplication across presence instances |
| Device retry logic | Self-healing, re-fetch on channel death |
| Presence service | Thin adapter — WAMP heartbeat → Redis write |

Nothing fighting anything else. Each tool doing exactly what it was designed for.
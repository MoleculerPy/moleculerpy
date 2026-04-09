/**
 * Direct-client Channels Interop harness for MoleculerPy cross-language
 * verification.
 *
 * Why this exists
 * ---------------
 * `@moleculer/channels` 0.2.0 has a regression with current nats.js 2.29.x:
 * `manager.streams.add()` returns `did_create: true` in debug logs but the
 * streams never actually land on the server, and the subsequent subscribe
 * call silently registers zero consumers. As a result, the high-level
 * `@moleculer/channels` stub does not work as a counterpart for a
 * cross-language test.
 *
 * moleculerpy-channels' own NATS adapter is verified working end-to-end
 * against the same NATS server (demo_channels passes 7/7 against
 * moleculerpy-nats:4223), so the problem is specifically on the Node.js
 * adapter side, not the wire protocol.
 *
 * This harness bypasses `@moleculer/channels` entirely and uses the raw
 * `nats` client (which IS working — verified with a probe script). It
 * subscribes to and publishes on the same JetStream subjects that a
 * MoleculerPy channels service would use, with the same JSON envelope,
 * so the demo can prove that the *wire format* is cross-language
 * compatible, independently of whichever high-level middleware either
 * side happens to run.
 *
 * Pattern: we're testing the NATS JetStream subject contract, not the
 * library. If moleculerpy-channels and @moleculer/channels both agree on
 * that contract (same subject names, same stream naming, same JSON
 * payload), they interoperate.
 *
 * Protocol notes
 * --------------
 * - Subject naming: channel name (e.g. "payments.completed") used as-is
 *   on the wire. JetStream stream name is the channel name with dots
 *   replaced by underscores ("payments_completed"). Both sides agree.
 * - Envelope: JSON-encoded object, no extra wrapper, no headers required.
 * - Delivery: JetStream pull consumer with explicit `ack()` on success.
 *
 * Environment
 * -----------
 *   NATS_URL               — transport endpoint (default nats://localhost:4222)
 *   CROSSLANG_CH_LOG       — path the harness will write received messages
 *                            to (JSONL, one entry per received message)
 *   CROSSLANG_CH_READY     — path a marker file is created at once the
 *                            consumer is registered and ready
 *
 * The Python demo driver writes/inspects those files to verify that Node
 * actually received Python's publishes.
 */

const { connect, AckPolicy, DeliverPolicy } = require("nats");
const fs = require("fs");

const NATS_URL = process.env.NATS_URL || "nats://localhost:4222";
const LOG_PATH = process.env.CROSSLANG_CH_LOG || "/tmp/crosslang_channels_node.log";
const READY_PATH = process.env.CROSSLANG_CH_READY || "/tmp/crosslang_channels_ready.marker";

// --- Helpers ---------------------------------------------------------------

function streamNameFor(channelName) {
    // Same convention as moleculerpy-channels and @moleculer/channels:
    // dots -> underscores (and anything else JetStream forbids).
    return channelName.replace(/[.>*]/g, "_");
}

async function ensureStream(jsm, channelName) {
    const name = streamNameFor(channelName);
    try {
        await jsm.streams.info(name);
        // Already exists — don't touch it, avoid stomping on another
        // side's config.
    } catch (e) {
        if (/not found|stream not found/i.test(e.message || "")) {
            await jsm.streams.add({ name, subjects: [channelName] });
            console.log(`[direct] created stream ${name} for subject ${channelName}`);
        } else {
            throw e;
        }
    }
}

async function ensureConsumer(jsm, channelName, durable) {
    const stream = streamNameFor(channelName);
    try {
        await jsm.consumers.info(stream, durable);
    } catch (e) {
        if (/not found|consumer not found/i.test(e.message || "")) {
            await jsm.consumers.add(stream, {
                durable_name: durable,
                ack_policy: AckPolicy.Explicit,
                deliver_policy: DeliverPolicy.All,
                filter_subject: channelName,
            });
            console.log(`[direct] created consumer ${durable} on stream ${stream}`);
        } else {
            throw e;
        }
    }
}

function writeLogLine(entry) {
    fs.appendFileSync(LOG_PATH, JSON.stringify(entry) + "\n");
}

// --- Main ------------------------------------------------------------------

(async () => {
    // Start with a clean log file so each demo run sees only its own data.
    try {
        fs.unlinkSync(LOG_PATH);
    } catch (_) {
        /* nothing to clean */
    }
    try {
        fs.unlinkSync(READY_PATH);
    } catch (_) {
        /* nothing to clean */
    }

    const nc = await connect({ servers: NATS_URL });
    console.log(`[direct] connected to ${nc.getServer()}`);
    const js = nc.jetstream();
    const jsm = await js.jetstreamManager();

    // Provision both streams (publisher subject + consumer subject).
    // The Python side will also try to provision its own stream on
    // "orders.created" — that's fine, whoever comes first wins the
    // create and the other gets "already exists".
    await ensureStream(jsm, "payments.completed");
    await ensureStream(jsm, "orders.created");

    // Register our pull consumer on payments.completed (this is what the
    // Python side will publish to; we consume and log).
    await ensureConsumer(jsm, "payments.completed", "node_payments_consumer");

    // Background loop: pull, deserialize JSON, log, ack.
    const consumer = await js.consumers.get(
        streamNameFor("payments.completed"),
        "node_payments_consumer"
    );

    // Signal readiness AFTER the consumer handle is acquired — only then
    // is the pull loop guaranteed to see any new publishes.
    fs.writeFileSync(READY_PATH, String(Date.now()));

    let pulling = true;
    (async () => {
        // nats.js `consume()` uses a long-lived subscription that delivers
        // messages via an async iterator — no need for explicit polling
        // with `expires`. The iterator simply yields when messages arrive
        // and suspends otherwise. Cleaner and avoids the "expires must be
        // >= 1000ms" constraint of the pull-request API.
        try {
            const messages = await consumer.consume();
            for await (const m of messages) {
                if (!pulling) break;
                let parsed;
                try {
                    parsed = JSON.parse(new TextDecoder().decode(m.data));
                } catch (err) {
                    parsed = { _raw: new TextDecoder().decode(m.data), _parseErr: String(err) };
                }
                console.log("[direct] received on payments.completed:", parsed);
                writeLogLine({
                    channel: "payments.completed",
                    payload: parsed,
                    received_at: new Date().toISOString(),
                });
                m.ack();
            }
        } catch (err) {
            // Swallow shutdown-time errors; any real failure would have
            // surfaced before `pulling` was flipped to false.
            if (pulling) {
                console.error("[direct] consume error:", err.message);
            }
        }
    })();

    // Simple action: listen on a core NATS request subject so the Python
    // driver can ask us to publish an "orders.created" message. This
    // avoids the need for a full Moleculer service registration.
    const sub = nc.subscribe("crosslang.directnode.publishOrder");
    (async () => {
        for await (const m of sub) {
            let req;
            try {
                req = JSON.parse(new TextDecoder().decode(m.data));
            } catch (err) {
                req = {};
            }
            const order = {
                order_id: `direct-node-ord-${Date.now()}`,
                product: req.product || "widget",
                quantity: req.quantity || 1,
                source: "node.js",
                marker: req.marker || null,
            };
            try {
                const ack = await js.publish(
                    "orders.created",
                    new TextEncoder().encode(JSON.stringify(order))
                );
                console.log(`[direct] published orders.created seq=${ack.seq}`);
                m.respond(new TextEncoder().encode(JSON.stringify(order)));
            } catch (err) {
                console.error("[direct] publish error:", err.message);
                m.respond(new TextEncoder().encode(JSON.stringify({ error: err.message })));
            }
        }
    })();

    console.log("[direct] ready");

    // Run until SIGINT / SIGTERM from the driver.
    process.on("SIGINT", async () => {
        pulling = false;
        await nc.drain();
        process.exit(0);
    });
    process.on("SIGTERM", async () => {
        pulling = false;
        await nc.drain();
        process.exit(0);
    });
})().catch((err) => {
    console.error("[direct] fatal:", err.message);
    console.error(err.stack);
    process.exit(1);
});

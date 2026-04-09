// Cross-language verification service.
// Writes markers to files in /tmp so the Python demo can assert
// Node.js actually received/processed cross-lang traffic.
const fs = require("fs");

const T3_LOG = process.env.CROSSLANG_T3_LOG || "/tmp/crosslang_test_T3.log";
const T4_LOG = process.env.CROSSLANG_T4_LOG || "/tmp/crosslang_test_T4.log";
const T5_LOG = process.env.CROSSLANG_T5_LOG || "/tmp/crosslang_test_T5.log";

function appendLine(path, line) {
    try {
        fs.appendFileSync(path, line + "\n");
    } catch (e) {
        console.error(`[crosslang_test] failed to write ${path}: ${e.message}`);
    }
}

module.exports = {
    name: "crosslang_test",

    actions: {
        // Called by Python broker. Node.js in turn calls python-greeter.hello
        // and records the result. This proves Node → Python RPC works.
        async verify_python_call(ctx) {
            const name = (ctx.params && ctx.params.name) || "Cross";
            try {
                const result = await this.broker.call("python-greeter.hello", { name });
                appendLine(T3_LOG, `OK ${result}`);
                return { ok: true, received: result };
            } catch (err) {
                appendLine(T3_LOG, `ERR ${err.message}`);
                throw err;
            }
        },
    },

    events: {
        "cross.lang.ping": {
            handler(ctx) {
                // Object-form event handler: ctx.params holds payload in Moleculer.js v0.14+.
                const data = ctx && ctx.params ? ctx.params : {};
                appendLine(T4_LOG, `PING ${JSON.stringify(data)}`);
            },
        },
    },

    started() {
        // Watch the registry for Python node disappearance / empty services.
        // When Python sends INFO(services=[]) as graceful drain, Node.js
        // registry removes py-crosslang endpoints.
        const bus = this.broker.localBus;
        const onDisconnect = (payload) => {
            appendLine(
                T5_LOG,
                `DISCONNECT ${JSON.stringify({ nodeID: payload && payload.node && payload.node.id })}`,
            );
        };
        const onInfo = (payload) => {
            // Fires when a remote node sends INFO packet.
            // payload.node.services is the advertised service list.
            const node = payload && payload.node;
            if (!node || node.local) return;
            const services = (node.services || []).map((s) => s.name);
            appendLine(
                T5_LOG,
                `INFO ${JSON.stringify({ nodeID: node.id, services })}`,
            );
        };

        bus.on("$node.disconnected", onDisconnect);
        bus.on("$node.updated", onInfo);
        bus.on("$node.connected", onInfo);
        this._crosslangHandlers = { onDisconnect, onInfo };
    },

    stopped() {
        const bus = this.broker.localBus;
        const h = this._crosslangHandlers || {};
        if (h.onDisconnect) bus.off("$node.disconnected", h.onDisconnect);
        if (h.onInfo) {
            bus.off("$node.updated", h.onInfo);
            bus.off("$node.connected", h.onInfo);
        }
    },
};

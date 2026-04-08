const { ServiceBroker } = require("moleculer");

// Transporter is sourced from NATS_URL env var so the Python demo_crosslang
// driver (which sets it to match its own broker) and the top-level
// docker-compose.yml (NATS on 4223) stay in sync without hand-editing this
// file. Default keeps the historical 4222 for standalone Node experiments.
const transporter = process.env.NATS_URL || "nats://localhost:4222";

// Create broker
const broker = new ServiceBroker({
    nodeID: "node-integration-test",
    transporter: transporter,
    logger: {
        type: "Console",
        options: {
            level: "info",
            colors: true,
            moduleColors: true,
            formatter: "full",
            autoPadding: true
        }
    },
    requestTimeout: 10 * 1000,
    retryPolicy: {
        enabled: false
    }
});

// Load services
broker.loadService(__dirname + "/math.service.js");
broker.loadService(__dirname + "/greeter.service.js");
broker.loadService(__dirname + "/crosslang_test.service.js");

// Start broker
broker.start()
    .then(() => {
        console.log("[Node] Broker started successfully");
        console.log(`[Node] NodeID: ${broker.nodeID}`);
        console.log("[Node] Services loaded:");
        broker.services.forEach(service => {
            console.log(`  - ${service.name} v${service.version || '1.0.0'}`);
        });

        // Test Python service availability after a delay
        setTimeout(async () => {
            try {
                console.log("\n[Node] Testing Python service availability...");
                const result = await broker.call("py-math.add", { a: 2, b: 3 });
                console.log(`[Node] ✓ Successfully called Python service: 2 + 3 = ${result}`);
            } catch (err) {
                console.log(`[Node] Python service not available yet: ${err.message}`);
            }
        }, 5000);

        // Keep process alive
        process.on("SIGINT", async () => {
            console.log("\n[Node] Shutting down...");
            await broker.stop();
            process.exit(0);
        });
    })
    .catch(err => {
        console.error(`[Node] Error starting broker: ${err.message}`);
        console.error(err);
        process.exit(1);
    });

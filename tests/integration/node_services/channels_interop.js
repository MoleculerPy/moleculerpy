/**
 * Bidirectional Channels Interop Test
 * Node.js (Moleculer.js + @moleculer/channels) <-> Python (MoleculerPy + moleculerpy-channels)
 *
 * Node.js publishes to channels that Python consumes, and vice versa.
 */
const { ServiceBroker } = require("moleculer");
const { Middleware: ChannelsMiddleware } = require("@moleculer/channels");

const received = {
    orders: [],
    payments: [],
};

const broker = new ServiceBroker({
    nodeID: "node-channels",
    transporter: "nats://localhost:4222",
    logger: {
        type: "Console",
        options: { level: "info", colors: true, formatter: "full" },
    },
    middlewares: [
        ChannelsMiddleware({
            adapter: "NATS",
            schemaProperty: "channels",
        }),
    ],
});

// Service that PUBLISHES events to channels
broker.createService({
    name: "js-publisher",
    actions: {
        async publishOrder(ctx) {
            const order = {
                order_id: `js-ord-${Date.now()}`,
                product: ctx.params.product || "widget",
                quantity: ctx.params.quantity || 1,
                source: "node.js",
            };
            await broker.sendToChannel("orders.created", order);
            console.log(`[JS] Published to orders.created: ${order.order_id}`);
            return order;
        },
    },
});

// Service that CONSUMES channels (reads what Python publishes)
broker.createService({
    name: "js-consumer",
    channels: {
        "payments.completed": {
            group: "js-consumer",
            async handler(msg) {
                console.log(`[JS] Received on payments.completed:`, msg);
                received.payments.push(msg);
            },
        },
        "orders.created": {
            group: "js-analytics",
            async handler(msg) {
                console.log(`[JS] Received on orders.created:`, msg);
                received.orders.push(msg);
            },
        },
    },
    actions: {
        getReceived(ctx) {
            return received;
        },
    },
});

broker
    .start()
    .then(() => {
        console.log("[JS] Broker with channels started");
        console.log("[JS] Waiting for Python to connect...");

        process.on("SIGINT", async () => {
            await broker.stop();
            process.exit(0);
        });
    })
    .catch((err) => {
        console.error("[JS] Start error:", err.message);
        process.exit(1);
    });

const mqtt = require('mqtt');
const client = mqtt.connect('mqtt://127.0.0.1:1883');
client.on('connect', () => {
  client.subscribe('test_swarm/console/registry/test-console');
  client.once('message', (topic, payload) => {
    console.log(`Message on ${topic}: ${payload.toString()}`);
    process.exit(0);
  });
});
setTimeout(() => { console.error('Timeout waiting for registry message'); process.exit(1); }, 5000);

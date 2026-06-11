#!/usr/bin/env bash
set -e

# 1. Start Mosquitto if not already running (assuming it is as per board)
# If it's not running, we might need to start it, but the board says it's RUNNING.

# 2. Start the console in the background
# We'll use a different namespace to avoid conflicts
export PI_SWARM_NS="test_swarm"
export PI_SWARM_CONSOLE_VERBOSE=0
node console.ts --ns test_swarm --broker mqtt://127.0.0.1:1883 --name test-console &
CONSOLE_PID=$!

# Give it a moment to connect
sleep 2

# 3. Verify registry entry exists via mqtt sub (using a simple python script or mosquitto_sub if available)
# Since I don't know if mosquitto_sub is installed, I'll use a small node script to check.
cat <<'INNER_EOF' > check_registry.js
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
INNER_EOF

echo "Checking registry..."
node check_registry.js

# 4. Test Spawning an agent (we'll use 'pi' itself if available, or just a dummy process)
# Since we are in a coding environment, 'pi' should be available.
cat <<'INNER_EOF' > spawn_agent.js
const mqtt = require('mqtt');
const client = mqtt.connect('mqtt://127.0.0.1:1883');
client.on('connect', () => {
  const spawnCmd = {
    action: 'spawn',
    name: 'test-agent',
    includeSwarmExtension: false // simplify for test
  };
  client.publish('test_swarm/console/in', JSON.stringify(spawnCmd), { qos: 1 });
  console.log('Sent spawn command');
});
setTimeout(() => { console.error('Timeout waiting for spawn response'); process.exit(1); }, 5000);
INNER_EOF

# Actually, we want to see if it receives the response.
# Let's use a more robust approach.
cat <<'INNER_EOF' > test_orchestrator.js
const mqtt = require('mqtt');
const client = mqtt.connect('mqtt://127.0.0.1:1883');
const NS = 'test_swarm';
const CONSOLE_ID = 'test-console';

client.on('connect', () => {
  console.log('Connected to MQTT');
  client.subscribe(`${NS}/console/out`);
  client.subscribe(`${NS}/console/registry/${CONSOLE_ID}`);

  // 1. Wait for registry
  client.once('message', (topic, payload) => {
    if (topic.includes('registry')) {
      console.log('Registry received');
      
      // 2. Spawn agent
      const spawnReq = { action: 'spawn', name: 'test-agent', includeSwarmExtension: false, reqId: '123' };
      client.publish(`${NS}/console/in`, JSON.stringify(spawnReq), { qos: 1 });
      console.log('Spawn request sent');
    }
  });

  client.on('message', (topic, payload) => {
    const msg = JSON.parse(payload.toString());
    if (topic.includes('/console/out')) {
      console.log('Received from console:', JSON.stringify(msg, null, 2));
      if (msg.type === 'spawn_result' && msg.ok) {
        console.log('SUCCESS: Agent spawned');
        process.exit(0);
      }
    }
  });
});

setTimeout(() => {
  console.error('Test timed out');
  process.exit(1);
}, 15000);
INNER_EOF

echo "Running test orchestrator..."
node test_orchestrator.js

# 5. Cleanup
kill $CONSOLE_PID
rm check_registry.js spawn_agent.js test_orchestrator.js test_console.sh

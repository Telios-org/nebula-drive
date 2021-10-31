const DatEncoding = require('dat-encoding')
const hypercore = require('hypercore');
const Hyperswarm = require('hyperswarm');
const pump = require('pump');

module.exports = (nameOrKey, opts) => {
  const swarm = new Hyperswarm();

  const { key } = resolveNameOrKey(nameOrKey);

  const storage = opts && opts.storage ? opts.storage : nameOrKey;

  const core = new hypercore(storage, key, opts);

  core.on('ready', async () => {
    if (opts.client || opts.server) {
      await initSwarm(core, swarm, opts);
    }
  });

  core.once('closed', async () => {
    await swarm.leave(core.discoveryKey);
  });

  core.destroy = async () => {
    await swarm.leave(core.discoveryKey);
    await swarm.destroy();
    await core.close();
  }

  return core;
}

async function initSwarm(core, swarm, opts) {
  swarm.on('connection', async (socket, info) => {
    try {
      console.log('CONNECTED', info.client)
      let stream = await core.replicate(info.client, { live: true });

      pump(socket, stream, socket);
    } catch (err) {
      console.log(err)
    }
  });

  console.log(`JOIN SWARM: ${core.discoveryKey.toString('hex')} | server:${opts.server} client:${opts.client}`)

  const discovery = swarm.join(core.discoveryKey, { server: opts.server, client: opts.client });

  if (opts.server) return await discovery.flushed()

  return await swarm.flush();
}

function resolveNameOrKey(nameOrKey) {
  let key, name, id;
  try {
    key = DatEncoding.decode(nameOrKey);
    id = key.toString('hex');
    // Normalize keys to be hex strings of the key instead of dat URLs
  } catch (e) {
    name = nameOrKey;
    id = name;
  }
  return { key, name, id };
}
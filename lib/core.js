const DatEncoding = require('dat-encoding')
const makeHypercorePromise = require('@geut/hypercore-promise');
const hypercore = require('hypercore');
const Hyperswarm = require('hyperswarm');
const pump = require('pump');

module.exports = (nameOrKey, opts) => {
  const swarm = new Hyperswarm();

  const { key } = resolveNameOrKey(nameOrKey);

  const storage = opts && opts.storage ? opts.storage : nameOrKey;

  const core = hypercore(storage, key, opts);

  const wrappedCore = makeHypercorePromise(core);

  core.ready(async() => {
    if(opts.client || opts.server) {
      await initSwarm(wrappedCore, swarm, opts);
    }
  });

  core.once('closed', async () => {
    await swarm.leave(wrappedCore.discoveryKey);
  });

  core.destroy = async () => {
    await swarm.leave(wrappedCore.discoveryKey);
    await swarm.destroy();
    await wrappedCore.close();
  }

  return wrappedCore;
}

async function initSwarm(core, swarm, opts) {
  swarm.on('connection', async (socket, info) => {
    try {
      let stream = await core.replicate(info.client, { live: true });

      pump(socket, stream, socket);
    } catch(err) {
      console.log(err)
    }
  });

  const discovery = swarm.join(core.discoveryKey,  { server: opts.server, client: opts.client });
  await discovery.flushed();
}

function resolveNameOrKey (nameOrKey) {
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
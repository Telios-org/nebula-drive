const Autobase = require('autobase')
const Hyperbee = require('hyperbee')
const Hypercore = require('hypercore')
const EventEmitter = require('events')
const Autobee = require('./autobee')
const Hyperswarm = require('hyperswarm')
const pump = require('pump')

class Database extends EventEmitter {
  constructor(storage, opts = {}) {
    super()

    const key = opts.peerPubKey ? opts.peerPubKey : null;
    
    this.peerPubKey = key;
    this.encryptionKey = opts.encryptionKey
    this.storage = typeof storage === 'string' ? `${storage}/local` : storage
    this.localWriter = null
    this.bee = null

    // Initialize meta core
    this.metaCore = new Hypercore(
      `${this.storage}/meta`,
      key
    )
    this.metadb = new Hyperbee(this.metaCore, {
      keyEncoding: 'utf-8',
      valueEncoding: 'json',
      extension: false
    })

    // Initialize local writer
    // this.localWriter = new Hypercore(
    //   `${this.storage}/writer`,
    //   key,
    //   {
    //     encryptionKey: this.encryptionKey
    //   }
    // )
    // this.bee = new Hyperbee(this.localWriter, {
    //   keyEncoding: 'utf-8',
    //   valueEncoding: 'json',
    //   extension: false
    // })

    // this.autobaseIndex = new Hypercore(
    //   `${this.storage}/index`,
    //   null,
    //   {
    //     // encryptionKey: this.encryptionKey
    //   }
    // )
    // this.peers = [...opts.peers, this.localWriter]
    // this.base = new Autobase(this.peers, {
    //   input: this.localWriter,
    //   indexes: this.autobaseIndex
    // })

    // this.bee = new Autobee(this.base, {
    //   keyEncoding: 'utf-8',
    //   valueEncoding: 'json'
    // })


    this.collections = {}

    // if(!this.encryptionKey) {
    //   const stream = this.metadb.createHistoryStream({ live: true })

    //   stream.on('data', data => {
    //     this.emit('sync', data)
    //   })
    // }
  }

  async ready() {
    await joinSwarm(this.metaCore, { server: !this.peerPubKey, client: !!this.peerPubKey })

    const encryptedCore = await this.metadb.get('__encryptedCore')
    
    if(!encryptedCore) {
      this.localWriter = new Hypercore(
        `${this.storage}/writer`,
        null,
        {
          encryptionKey: this.encryptionKey
        }
      )

      await this.localWriter.ready()

      await this.metadb.put('__encryptedCore', { 
        key: this.localWriter.key.toString('hex')
      })
    } else {
      this.localWriter = new Hypercore(
        `${this.storage}/writer`,
        encryptedCore.value.key,
        {
          encryptionKey: this.encryptionKey
        }
      )

      await this.localWriter.ready()
    }

    await joinSwarm(this.localWriter, { server: !this.peerPubKey, client: !!this.peerPubKey })

    this.bee = new Hyperbee(this.localWriter, {
      keyEncoding: 'utf-8',
      valueEncoding: 'json',
      extension: false
    })
  }

  async collection(name) {
    await this.bee.sub(name)
    const collection = new Collection(name, this.bee)
    this.collections[name] = collection
    return collection
  }

  async addInput(key) {
    const feed = new Hypercore(
      `${this.storage}/${key.toString('hex')}`,
      key,
      {
        encryptionKey: this.encryptionKey
      }
    )

    await this.base.addInput(feed)

    await joinSwarm(feed, { server: false, client: true })
  }

  async removeInput(key) {
    // const peers = this.peers.filter(p => p.key.toString('hex') !== key);
    // this.base = new Autobase(peers, { input: this.feed })
    // this.db = new Autobee(this.base)
    // await this.db.update();
  }
}

// Collections/subs are broken in autobase
// class Collection {
//   constructor(sub, db) {
//     this.sub = sub
//     this.db = db
//   }

//   async put(key, value) {
//     let val = { ...value, __sub: this.sub }
//     await this.db.put(key, val)
//   }

//   async get(key) {
//     return this.db.get(key, { sub: this.sub })
//   }
// }

class Collection {
  constructor(sub, db) {
    this.sub = `__${sub}__`
    this.db = db
  }

  async put(key, value) {
    await this.db.put(this.sub + key, value)
  }

  async get(key) {
    const result = await this.db.get(this.sub + key)

    result.key = result.key.replace(this.sub, '')

    return result
  }
}

async function joinSwarm(core, { server, client }) {
  const swarm = new Hyperswarm()

  await core.ready()

  swarm.on('connection', async (socket, info) => {
    // let stream = core.replicate(info.client, { live: true })

    // pump(socket, stream, socket)
    socket.pipe(core.replicate(info.client)).pipe(socket)
  })

  const topic = core.discoveryKey;
  // console.log(`JOIN SWARM: ${topic.toString('hex')} | server:${server} client:${client}`)
  const discovery = swarm.join(topic, { server, client })

  if (server) return await discovery.flushed()

  return await swarm.flush()
}

module.exports = Database

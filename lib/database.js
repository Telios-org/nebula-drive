const Autobase = require('autobase')
const Hypercore = require('hypercore')
const EventEmitter = require('events')
const Autobee = require('./autobee')
const Hyperswarm = require('hyperswarm')

class Database extends EventEmitter {
  constructor(storage, opts = {}) {
    super()

    const key = opts.peerPubKey ? opts.peerPubKey : null;
    
    this.keyPair = opts.keyPair
    this.acl = opts.acl
    this.peerPubKey = key
    this.encryptionKey = opts.encryptionKey
    this.storage = typeof storage === 'string' ? `${storage}/Database` : storage
    this.storageIsString = typeof storage === 'string' ? true : false
    
    // Init local Autobase
    this.localInput = new Hypercore(
      this.storageIsString ? `${this.storage}/local-writer` : this.storage, 
      null, 
      { encryptionKey: this.encryptionKey }
    )
    
    this.localOutput = new Hypercore(
      this.storageIsString ? `${this.storage}/local-index` : this.storage, 
      null, 
      { encryptionKey: this.encryptionKey }
    )

    this.base = new Autobase([this.localInput], {
      input: this.localInput,
      outputs: this.localOutput
    })
    this.bee = new Autobee(this.base, {
      keyEncoding: 'utf-8',
      valueEncoding: 'json'
    })

    // Init Meta Autobase
    this.localMetaCore = new Hypercore(this.storageIsString ? `${this.storage}/meta-local` : this.storage)
    this.remoteMetaCore = new Hypercore(this.storageIsString ? `${this.storage}/meta-remote` : this.storage, this.peerPubKey)
    this.metaIndex = new Hypercore(this.storageIsString ? `${this.storage}/meta-index` : this.storage, null)
    this.metaBase = new Autobase([this.localMetaCore], {
      input: this.localMetaCore,
      outputs: this.metaIndex
    })
    this.metadb = new Autobee(this.metaBase, {
      keyEncoding: 'utf-8',
      valueEncoding: 'json'
    })

    this.collections = {}
    this.cores = [
      this.localInput,
      this.localOutput,
      this.localMetaCore,
      this.remoteMetaCore,
      this.metaIndex
    ]
    this.connections = []
  }

  async ready() {
    await this._joinSwarm(this.localInput, { server: true, client: false })
    await this._joinSwarm(this.localMetaCore, { server: true, client: false })

    let remotePeers

    if(this.peerPubKey) {
      await this._joinSwarm(this.remoteMetaCore, { server: false, client: true })
      await this.metaBase.addInput(this.remoteMetaCore)
      
      remotePeers = await this.metadb.get('__peers')
    } else {
      await this._joinSwarm(this.remoteMetaCore, { server: true, client: false })
    }
    
    if(!remotePeers) {
      await this.metadb.put('__peers', {
        [this.keyPair.publicKey.toString('hex')]: {
          writer: this.localInput.key.toString('hex'),
          meta: this.localMetaCore.key.toString('hex')
        }
      })
    } else {
      for (const key in remotePeers.value) {
        const peer = remotePeers.value[key]

        const peerWriter = new Hypercore(
          this.storageIsString ? `${this.storage}/peers/${peer.writer}` : this.storage, 
          peer.writer, 
          { encryptionKey: this.encryptionKey }
        )

        this.cores.push(peerWriter)
        
        await this._joinSwarm(peerWriter, { server: false, client: true })
        await this.base.addInput(peerWriter)

        if(peer.meta !== this.remoteMetaCore.key.toString('hex')) {
          const peerMeta = new Hypercore(this.storageIsString ? `${this.storage}/peers/${peer.meta}` : this.storage, peer.meta)
          this.cores.push(peerMeta)
          await this._joinSwarm(peerMeta, { server: false, client: true })
          await this.metaBase.addInput(peerMeta)
        }
      }
    }
  }

  async collection(name) {
    await this.bee.sub(name)
    const collection = new Collection(name, this.bee)
    this.collections[name] = collection
    return collection
  }

  async addInput(key) {
    const core = new Hypercore(
      this.storageIsString ? `${this.storage}/${key.toString('hex')}` : this.storage,
      key,
      { encryptionKey: this.encryptionKey }
    )

    this.cores.push(core)

    await this.base.addInput(core)

    await this._joinSwarm(core, { server: false, client: true })
  }

  async removeInput(key) {
    // TODO
  }

  async close() {
    for await(const core of this.cores) {
      await core.close()
    }

    for await(const conn of this.connections) {
      await conn.destroy()
    }
  }

  // TODO: Figure out how to multiplex these cnonections
  async _joinSwarm(core, { server, client }) {
    const swarm = new Hyperswarm()
  
    await core.ready()
  
    swarm.on('connection', async (socket, info) => {
      socket.pipe(core.replicate(info.client)).pipe(socket)
    })
  
    const topic = core.discoveryKey;
    const discovery = swarm.join(topic, { server, client })
  
    this.connections.push(discovery)
  
    if (server) return await discovery.flushed()
  
    return await swarm.flush()
  }
}

class Collection {
  constructor(sub, db) {
    this.sub = sub
    this.db = db
  }

  async put(key, value) {
    let val = { ...value, __sub: this.sub }
    await this.db.put(key, val)
  }

  async get(key) {
    return this.db.get(key, { sub: this.sub })
  }

  async del(key) {
    return this.db.del(key, { sub: this.sub })
  }
}

module.exports = Database

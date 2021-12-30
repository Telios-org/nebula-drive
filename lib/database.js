const Autobase = require('autobase')
const Hypercore = require('./core')
const EventEmitter = require('events')
const Autobee = require('./autobee')
const Hyperswarm = require('hyperswarm')

class Database extends EventEmitter {
  constructor(storage, opts = {}) {
    super()

    const key = opts.peerPubKey ? opts.peerPubKey : null;
    
    this.drivePath = opts.drivePath
    this.keyPair = opts.keyPair
    this.acl = opts.acl
    this.peerPubKey = key
    this.encryptionKey = opts.encryptionKey
    this.storage = typeof storage === 'string' ? `${storage}/Database` : storage
    this.storageIsString = typeof storage === 'string' ? true : false
    
    // Init local Autobase
    this.localWriter = new Hypercore(this.storageIsString ? `${this.storage}/local-writer` : this.storage, 
      { 
        encryptionKey: this.encryptionKey, 
        storageNamespace: `${this.drivePath}:local-writer`
      }
    )
    
    this.autobaseIndex = new Hypercore(this.storageIsString ? `${this.storage}/local-index` : this.storage, 
      { 
        encryptionKey: this.encryptionKey, 
        storageNamespace: `${this.drivePath}:local-index`
      }
    )

    this.base = new Autobase([this.localWriter], {input: this.localWriter, indexes: this.autobaseIndex })
    this.bee = new Autobee(this.base, { keyEncoding: 'utf-8', valueEncoding: 'json' })

    // Init Meta Autobase
    this.localMetaCore = new Hypercore(this.storageIsString ? `${this.storage}/meta-local` : this.storage, { storageNamespace: `${this.drivePath}:meta-local` })
    this.remoteMetaCore = new Hypercore(this.storageIsString ? `${this.storage}/meta-remote` : this.storage, this.peerPubKey, { storageNamespace: `${this.drivePath}:meta-remote` })
    this.metaIndex = new Hypercore(this.storageIsString ? `${this.storage}/meta-index` : this.storage, null, { storageNamespace: `${this.drivePath}:meta-index` })
    this.metaBase = new Autobase([this.localMetaCore], { input: this.localMetaCore, indexes: this.metaIndex })
    this.metadb = new Autobee(this.metaBase, { keyEncoding: 'utf-8', valueEncoding: 'json' })

    this.collections = {}
    this.cores = [
      this.localWriter,
      this.autobaseIndex,
      this.localMetaCore,
      this.remoteMetaCore,
      this.metaIndex
    ]
    this.connections = []
  }

  async ready() {
    await this._joinSwarm(this.localWriter, { server: true, client: true })
    await this._joinSwarm(this.localMetaCore, { server: true, client: true })

    let remotePeers

    if(this.peerPubKey) {
      await this._joinSwarm(this.remoteMetaCore, { server: true, client: true })
      await this.metaBase.addInput(this.remoteMetaCore)
      
      remotePeers = await this.metadb.get('__peers')
    } else {
      await this._joinSwarm(this.remoteMetaCore, { server: true, client: true })
    }
    
    if(!remotePeers) {
      await this.metadb.put('__peers', {
        [this.keyPair.publicKey.toString('hex')]: {
          writer: this.localWriter.key.toString('hex'),
          meta: this.localMetaCore.key.toString('hex')
        }
      })
    } else {
      for (const key in remotePeers.value) {
        const peer = remotePeers.value[key]

        const peerWriter = new Hypercore(
          this.storageIsString ? `${this.storage}/peers/${peer.writer}` : this.storage, 
          peer.writer, 
          { 
            encryptionKey: this.encryptionKey, 
            storageNamespace: `${this.drivePath}:peers:${peer.writer}` 
          }
        )

        this.cores.push(peerWriter)
        
        await this._joinSwarm(peerWriter, { server: true, client: true })
        await this.base.addInput(peerWriter)

        if(peer.meta !== this.remoteMetaCore.key.toString('hex')) {
          const peerMeta = new Hypercore(this.storageIsString ? `${this.storage}/peers/${peer.meta}` : this.storage, peer.meta, { storageNamespace: `${this.drivePath}:peers:${peer.meta}` })
          this.cores.push(peerMeta)
          await this._joinSwarm(peerMeta, { server: true, client: true })
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
    const core = new Hypercore( this.storageIsString ? `${this.storage}/${key.toString('hex')}` : this.storage, 
      key,
      { 
        encryptionKey: this.encryptionKey, 
        storageNamespace: `${this.drivePath}:${key.toString('hex')}` 
      }
    )

    this.cores.push(core)

    await this.base.addInput(core)

    await this._joinSwarm(core, { server: true, client: true })
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
  
    try {
      swarm.on('connection', async (socket, info) => {
        socket.pipe(core.replicate(info.client)).pipe(socket)
      })
  
      const topic = core.discoveryKey;
      const discovery = swarm.join(topic, { server, client })

      this.connections.push(discovery)
   
      if (server) {
        await discovery.flushed()
        this.emit('connected')
        return
      }

      await swarm.flush()
      this.emit('connected')

    } catch(e) {
      return this.emit('disconnected')
    }
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

const Hypercore = require('hypercore')
const EventEmitter = require('events')
const Autobee = require('./autobee')
const Hyperswarm = require('hyperswarm')
const HyperFTS = require('./hyper-fts')

class Database extends EventEmitter {
  constructor(storage, opts = {}) {
    super()

    const key = opts.peerPubKey ? opts.peerPubKey : null;
    
    this.opts = opts
    this.keyPair = opts.keyPair
    this.bee = null
    this.metadb = null
    this.acl = opts.acl
    this.peerPubKey = key
    this.encryptionKey = opts.encryptionKey
    this.storage = typeof storage === 'string' ? `${storage}/Database` : storage
    this.storageIsString = typeof storage === 'string' ? true : false
    this.joinSwarm = typeof opts.joinSwarm === 'boolean' ? opts.joinSwarm : true
    
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

    // Init Meta Autobase
    this.localMetaCore = new Hypercore(this.storageIsString ? `${this.storage}/meta-local` : this.storage)
    this.remoteMetaCore = new Hypercore(this.storageIsString ? `${this.storage}/meta-remote` : this.storage, this.peerPubKey)
    this.metaIndex = new Hypercore(this.storageIsString ? `${this.storage}/meta-index` : this.storage, null)

    this.collections = {}
    this.cores = [
      this.localInput,
      this.localOutput,
      this.localMetaCore,
      this.remoteMetaCore,
      this.metaIndex
    ]
    this.connections = []

    // Init local search index
    if(opts.fts) {
      this.fts = new HyperFTS(this.storageIsString ? `${this.storage}/fts` : this.storage, this.encryptionKey)
    }
  }

  async ready() {
    let remotePeers

    if(this.opts.fts) {
      await this.fts.ready()
    }
    
    await this._joinSwarm(this.localInput, { server: true, client: true })
    await this._joinSwarm(this.localMetaCore, { server: true, client: true })


    this.bee = new Autobee({
      inputs: [this.localInput], 
      defaultInput: this.localInput, 
      outputs: this.localOutput,
      valueEncoding: 'json'
    })

    this.metadb = new Autobee({
      inputs: [this.localMetaCore], 
      defaultInput: this.localMetaCore, 
      outputs: this.metaIndex,
      valueEncoding: 'json'
    })

    await this.bee.ready()
    await this.metadb.ready()

    if(this.peerPubKey) {
      await this._joinSwarm(this.remoteMetaCore, { server: true, client: true })
      await this.metadb.addInput(this.remoteMetaCore)
      
      remotePeers = await this.metadb.get('__peers')
    } else {
      await this._joinSwarm(this.remoteMetaCore, { server: true, client: true })
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
        
        await this._joinSwarm(peerWriter, { server: true, client: true })
        await this.bee.addInput(peerWriter)

        if(peer.meta !== this.remoteMetaCore.key.toString('hex')) {
          const peerMeta = new Hypercore(this.storageIsString ? `${this.storage}/peers/${peer.meta}` : this.storage, peer.meta)
          this.cores.push(peerMeta)
          await this._joinSwarm(peerMeta, { server: true, client: true })
          await this.metadb.addInput(peerMeta)
        }
      }
    }
  }

  async collection(name) {
    const collection = await this.bee.sub(name)

    collection.ftsIndex = async (props) => {
      if(!this.opts.fts) throw('Full text search is currently disabled because the option was set to false')

      const stream = collection.createReadStream()

      await this.fts.index({ name, props, stream })
    }

    collection.search = async (query, opts) => {
      if(!this.opts.fts) throw('Full text search is currently disabled because the option was set to false')
      
      return this.fts.search({ db:collection, name, query, opts })
    }

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

    await this.bee.addInput(core)

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
  
    if(this.joinSwarm) {
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
}

module.exports = Database

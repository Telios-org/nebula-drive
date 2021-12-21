const EventEmitter = require('events');
const pump = require('pump');
const Hyperswarm = require('hyperswarm');
const blake = require('blakejs');

class Swarm extends EventEmitter {
  constructor({ acl, keyPair, workerKeyPairs, publicKey, topic, ephemeral, isServer, isClient }) {
    super()

    this.fileSwarm = new Hyperswarm({  // Networking for streaming file data
      firewall(remotePublicKey) {
        if (workerKeyPairs[remotePublicKey.toString('hex')]) {
          return true;
        }
        return false;
      }
    })
    this.keyPair = keyPair
    this.publicKey = publicKey
    this.discoveryTopic = blake.blake2bHex(topic, null, 32)
    this.ephemeral = ephemeral
    this.closing = false
    this.fileDiscovery = null
    this.isServer = isServer
    this.isClient = isClient
    this.server = new Hyperswarm({
      keyPair,
      firewall(remotePublicKey) {
        // validate if you want a connection from remotePublicKey
        if (acl && acl.length) {
          return acl.indexOf(remotePublicKey.toString('hex')) === -1;
        }

        return false;
      }
    })
  }

  async ready() {
    this.fileSwarm.on('connection', async (socket, info) => {
      this.emit('file-requested', socket)
    })

    this.server.on('connection', (socket, info) => {
      const peerPubKey = socket.remotePublicKey.toString('hex')

      socket.on('data', data => {
        this.emit('message', peerPubKey, data)
      })
    })

    try {
      await this.server.listen(this.keyPair)
    } catch(e) {
      return this.emit('disconnected')
    }

    try {
      this.fileDiscovery = this.fileSwarm.join(Buffer.from(this.discoveryTopic, 'hex'), { server: true, client: false })
      await this.fileDiscovery.flushed()
    } catch(e) {
      return this.emit('disconnected')
    }

    this.emit('connected')
  }

  connect(publicKey) {
    const noiseSocket = this.node.connect(Buffer.from(publicKey, 'hex'))

    noiseSocket.on('open', function () {
      // noiseSocket fully open with the other peer
      this.emit('onPeerConnected', noiseSocket)
    });
  }

  async close() {
    const promises = []

    promises.push(this.fileDiscovery.destroy())
    promises.push(this.server.leave(this.keyPair.publicKey))

    try {
      await Promise.all(promises)
    } catch (err) {
      throw err
    }
  }
}

module.exports = Swarm

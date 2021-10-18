const EventEmitter = require('events');
const fs = require('fs');
const path = require('path');
const pump = require('pump');
const Hyperswarm = require('hyperswarm');
const blake = require('blakejs');
const DHT = require('@hyperswarm/dht');

class Swarm extends EventEmitter {
  constructor({ db, acl, keyPair, workerKeyPairs, publicKey, topic, ephemeral, isServer, isClient }) {
    super();

    this.repSwarm = new Hyperswarm(); // Networking for replicating hypercore
    this.fileSwarm = new Hyperswarm({  // Networking for streaming file data
      firewall(remotePublicKey) {
        if (workerKeyPairs[remotePublicKey.toString('hex')]) {
          return true;
        }

        return false;
      }
    });
    this.db = db;
    this.keyPair = keyPair;
    this.publicKey = publicKey;
    this.repTopic = Buffer.from(topic, 'hex');
    this.discoveryTopic = blake.blake2bHex(topic, null, 32);
    this.ephemeral = ephemeral;
    this.peers = {};
    this.plexConnections = [];
    this.closing = false;
    this.repDiscovery = null;
    this.fileDiscovery = null;
    this.isServer = isServer;
    this.isClient = isClient;
    this.node = new DHT({ keyPair });
    this.server = this.node.createServer({
      firewall(remotePublicKey) {
        // validate if you want a connection from remotePublicKey
        if (acl && acl.length) {
          return acl.indexOf(remotePublicKey.toString('hex')) === -1;
        }

        return false;
      }
    });
  }

  async ready() {
    this.repSwarm.on('connection', async (socket, info) => {
      this.replicate(socket, info);
    });

    this.fileSwarm.on('connection', async (socket, info) => {
      this.emit('file-requested', socket);
    });

    this.server.on('connection', encryptedConnection => {
      const peerPubKey = encryptedConnection.remotePublicKey.toString('hex');

      encryptedConnection.on('data', socket => {
        this.emit('message', peerPubKey, socket);
      });
    });

    await this.server.listen(this.keyPair);

    this.repDiscovery = this.repSwarm.join(this.repTopic, { server: this.isServer, client: this.isClient });
    await this.repDiscovery.flushed();
    await this.repDiscovery.refresh({ client: this.isClient, server: this.isServer });

    this.fileDiscovery = this.fileSwarm.join(Buffer.from(this.discoveryTopic, 'hex'), { server: true, client: false });
    await this.fileDiscovery.flushed();
    await this.fileDiscovery.refresh({ server: true, client: false });
  }

  connect(publicKey) {
    const noiseSocket = this.node.connect(Buffer.from(publicKey, 'hex'));

    noiseSocket.on('open', function () {
      // noiseSocket fully open with the other peer
      this.emit('onPeerConnected', noiseSocket);
    });
  }

  async replicate(socket, info) {
    try {
      let stream = await this.db.replicate(info.client, { stream: socket, live: true });
      pump(socket, stream, socket);
    } catch (err) {
      console.log(err)
    }
  }

  async close() {
    const promises = [];

    promises.push(this.repSwarm.destroy());
    promises.push(this.fileSwarm.destroy());
    promises.push(this.node.destroy());
    promises.push(this.server.close());

    try {
      await Promise.all(promises);
    } catch (err) {
      throw err;
    }
  }
}

module.exports = Swarm;

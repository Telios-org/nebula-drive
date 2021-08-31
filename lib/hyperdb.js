const Crypto = require('./crypto');
const MultiHyperbee = require('@telios2/multi-hyperbee');
const Collection = require('./collection');
const EventEmitter = require('events');

class HyperDB extends EventEmitter {
  constructor(storage, secret) {
    super();

    this.db = new MultiHyperbee(storage, {
      keyEncoding: 'utf-8', // can be set to undefined (binary), utf-8, ascii or and abstract-encoding
      valueEncoding: 'json'
    });

    this.secret = secret || Crypto.generateAEDKey();
    this.collections = {};
  }

  async ready() {
    await this.db.ready();
  }

  async collection(name) {
    const sub = await this.db.sub(name);
    const collection = new Collection(sub, name, this.secret);
    
    this.collections[name] = collection;
    return collection;
  }

  async put(key, value) {
    return this.db.put(key, value);
  }

  async get(key) {
    return this.db.get(key);
  }

  async del(key) {
    await this.db.del(key);
  }

  setSecret(secret) {
    this.secret = secret;
  }
}

module.exports = HyperDB;

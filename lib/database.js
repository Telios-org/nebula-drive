const Autobase = require('autobase')
const Hypercore = require('hypercore-encrypt')
const EventEmitter = require('events')
const Autobee = require('./autobee')

class Database extends EventEmitter {
  constructor(storage, secret) {
    super()
    this.feed = new Hypercore(storage, { encryptionKey: secret })
    this.base = new Autobase([this.feed], { input: this.feed })

    this.db = new Autobee(this.base)

    this.secret = secret || this.feed.encryptionKey.toString('hex')
    this.collections = {}
  }

  async ready() {
    await this.db.ready()
  }

  async collection(name) {
    await this.db.sub(name)
    return new Collection(name, this.db)
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
}

module.exports = Database

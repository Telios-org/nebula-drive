const Crypto = require('./crypto');

class Collection {
  constructor(sub, name, secret) {
    this.db = sub;
    this.secret = secret;
    this.name = secret ? Crypto.hash(name, secret) : name;
  }

  async put(key, value) {
    if(typeof key !== 'string') {
      key = key.toString();
    }

    if (this.secret) {
      key = Crypto.hash(key, this.secret);
      value = Crypto.encryptAED(JSON.stringify(value), this.secret);
    }

    return this.db.put(key, value.toString('hex'));
  }

  async get(key) {
    if(typeof key !== 'string') {
      key = key.toString();
    }

    if (this.secret) {
      key = Crypto.hash(key, this.secret);
      
      const result = await this.db.get(key);

      if(!result || !result.value) {
        return result;
      }

      const decoded = Crypto.decryptAED(Buffer.from(result.value, 'hex'), this.secret);
      return JSON.parse(decoded);
    }

    return this.db.get(key);
  }

  async del(key) {
    await this.db.del(key);
  }
}

module.exports = Collection;
const hypercore = require('hypercore');

class Hypercore {
  constructor(storage, key, opts) {
    this.storage = storage
    this.opts = opts

    if(key && typeof key === 'object') {
      this.opts = key
    }

    if(typeof storage !== 'string') {
      this.storage = (filename, opts) => {
        return storage(`${this.opts ? this.opts.storageNamespace + ':' :  ''}${filename}`, opts)
      }
    }

    return new hypercore(this.storage, key, opts)
  }
}

module.exports = Hypercore
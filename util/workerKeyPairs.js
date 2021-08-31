const DHT = require('@hyperswarm/dht');

class WorkerKeyPairs {
  constructor(count) {
    this.keyPairs = {};

    for(let i = 0; i < count; i += 1) {
      const keyPair = DHT.keyPair();
      this.keyPairs[keyPair.publicKey.toString('hex')] = { active: false, ...keyPair }; 
    }
  }

  getKeyPair() {
    for(let key in this.keyPairs) {

      if(!this.keyPairs[key].active) {
        this.keyPairs[key].active = true;
        return this.keyPairs[key];
      }
    }

    return null;
  }

  release(key) {
    if(this.keyPairs[key]) {
      this.keyPairs[key].active = false;
    }
  }
}

module.exports = WorkerKeyPairs;
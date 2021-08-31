const EventEmitter = require('events');

class RequestChunker extends EventEmitter {
  constructor(files, batchSize) {
    super();

    this.BATCH_SIZE = batchSize || 5;
    this.files = files;
    this.queue = [];
    this.resetTimeout = 2000;
    this.timer = this.resetTimeout;
    this.interval = null;

    if(files) {
      return this.chunkFileRequests(files);
    }
  }

  chunkFileRequests(files) {
    const size = this.BATCH_SIZE;
    const chunked = [];

    for (let i = 0; i < files.length; i += size) {
      chunked.push(files.slice(i, i + size));
    }

    return chunked;
  }

  addFile(file) {
    let exists = false;

    if(!this.interval) {
      this.startTimer();
    }

    for(let i = 0; i < this.queue.length; i += 1) {
      if(this.queue[i].hash === file.hash) {
        exists = true;
        this.queue.splice(i, 1);
      }
    }

    if(!exists) {
      this.queue.push(file);
    }

    this.timer = this.resetTimeout;
  }

  processQueue() {
    this.emit('process-queue', this.queue);
  }

  startTimer() {
    this.interval = setInterval(() => {
      if(this.timer > 0) {
        this.timer -= 100;
      }

      if(this.timer === 0) {
        this.clearTimer();
        this.processQueue();
      }
    }, 100);
  }

  clearTimer() {
    clearInterval(this.interval);
    this.interval = null;
    this.timer = this.resetTimeout;
  }

  reset() {
    this.queue = [];
  }
}

module.exports = RequestChunker;
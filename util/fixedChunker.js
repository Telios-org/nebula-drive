const MemoryStream = require('memorystream');
const EventEmitter = require('events');

class Chunker extends EventEmitter {
  constructor(readStream, maxBlockSize) {
    super();

    this.chunkSize = maxBlockSize;
    this.chunkStream = readStream;
    this.stream = new MemoryStream();
    this.buffer = Buffer.from([]);

    this.openStream();

    return this.stream;
  }

  resetBuffer() {
    this.buffer = Buffer.from([]);
  }

  processBuffer() {
    if(this.buffer.length === this.chunkSize) {
      this.stream.write(this.buffer);
      this.resetBuffer();
    }
  }

  openStream() {
    this.chunkStream.on('error', (err) => {
      this.stream.destroy(err);
    });
  
    this.chunkStream.on('data', chunk => {
      this.processData(chunk);
    });
  
    this.chunkStream.on('end', () => {
      this.stream.end(this.buffer);
      this.resetBuffer();
    });
  }

  processData(data) {
    const chunks = this.reduce(data);
  
    chunks.forEach((chunk) => {
      this.stream.write(chunk);
    });
  }

  reduce(data) {
    const chunks = [];

    if(this.buffer.length) {
      const intoBuffer = data.slice(0, this.chunkSize - this.buffer.length);
      this.buffer = Buffer.concat([this.buffer, intoBuffer]);
      
      data = data.slice(intoBuffer.length);
      
      this.processBuffer();
    }
  
    while (data.length > this.chunkSize) {
      const chunk = data.slice(0, this.chunkSize);
      data = data.slice(this.chunkSize);

      chunks.push(chunk);
    }

    if (data.length + this.buffer.length <= this.chunkSize) {
      this.buffer = Buffer.concat([this.buffer, data]);
      this.processBuffer();
    }
  
    return chunks;
  }
}

module.exports = Chunker;
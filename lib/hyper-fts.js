const natural = require('natural')
const stopwords = require('stopwords')
const Corestore = require('corestore')
const Hyperbee = require('hyperbee')
const tokenizer = new natural.AggressiveTokenizer()
const pump = require('pump')
const concat = require('concat-stream')

/**
 * This implementation of full text search on hypercore was refactored from 
 * Paul Frazee's Hyper search experiment (https://github.com/pfrazee/hyper-search-experiments)
 */

class HyperFTS {
  constructor(storage, encryptionKey) {
    this.store = new Corestore(storage)
    this.encryptionKey = encryptionKey
    this.indexes = new Map()
  }

  async ready() {
    await this.store.ready()
  }

  async index({ name, props, stream }) {
    const bee = this._getDB(name)

    if (!bee.tx) bee.tx = bee.batch()
    const tx = bee.tx

    return new Promise((resolve, reject) => {
      const promises = []

      stream.on('data', async data => {   
        let text = ''

        for(const prop of props) {
          if(data.value[prop]) text += data.value[prop] 
        }

        const id = data.key

        if(!text) {
          reject('Property to index cannot be null')
        }

        const tokens = this._toTokens(text)
      
        for (let token of tokens) {
          promises.push(tx.put(`idx:${token}:${id}`, {}))
        }
      })

      stream.on('error', err => {
        reject(err)
      })

      stream.on('end', async () => {
        await Promise.all(promises)
        await tx.flush()
        resolve()
      })
    })
  }

  async search({ db, name, query, opts }) {
    const bee = this._getDB(name)
    const queryTokens = this._toTokens(query)
    const listsPromises = []
    const limit = opts?.limit || 10

    for (let qt of queryTokens) {
      listsPromises.push(bee.list({gt: `idx:${qt}:\x00`, lt: `idx:${qt}:\xff`}))
    }

    const listsResults = await Promise.all(listsPromises)
    const docIdHits = {}

    for (let listResults of listsResults) {
      for (let item of listResults) {
        const docId = item.key.split(':')[2]
        docIdHits[docId] = (docIdHits[docId] || 0) | 1
      }
    }

    const docIdsSorted = Object.keys(docIdHits).sort((a, b) => docIdHits[b] - docIdHits[a])
    return Promise.all(docIdsSorted.slice(0, limit).map(docId => db.get(docId)))
  }

  _getDB(name) {
    let bee = this.indexes.get(name)

    if(bee) {
      return bee
    }

    const core = this.store.get({ name, encryptionKey: this.encryptionKey })

    bee = new Hyperbee(core, {
      keyEncoding: 'utf-8',
      valueEncoding: 'json'
    })

    bee.list = async (opts) => {
      let stream = await bee.createReadStream(opts)
      return new Promise((resolve, reject) => {
        pump(
          stream,
          concat(resolve),
          err => {
            if (err) reject(err)
          }
        )
      })
    }

    this.indexes.set(name, bee)

    return bee
  }

  _toTokens (str) {
    let arr = Array.isArray(str) ? str : tokenizer.tokenize(str)
    return [...new Set(arr.map(token => token.toLowerCase()).filter(token => !stopwords.english.includes(token)))].map(token => natural.Metaphone.process(token))
  }
}

module.exports = HyperFTS
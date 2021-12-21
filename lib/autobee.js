const codecs = require('codecs')
const Autobase = require('autobase')
const { InputNode } = require('autobase/lib/nodes/index.js')
const Hyperbee = require('hyperbee')
const HyperbeeMessages = require('hyperbee/lib/messages.js')

class Autobee {
  constructor ({inputs, defaultInput, outputs, valueEncoding } = {}) {
    inputs = inputs || []
    valueEncoding = valueEncoding || 'json'
    this._valueEncoding = valueEncoding
    this._valueEncoder = codecs(valueEncoding)
    
    this.autobase = new Autobase(inputs, {outputs, input: defaultInput})

    this.view = this.autobase.linearize({
      unwrap: true,
      apply: this._apply.bind(this)
    })
    
    this.indexBee = new Hyperbee(this.view, {
      extension: false,
      keyEncoding: 'utf-8',
      valueEncoding
    })

    this._inputBees = new Map()
  }

  async ready () {
    return this.autobase.ready()
  }

  get writable () {
    return !!this.autobase.inputs.find(core => core.writable)
  }

  get config () {
    return {
      inputs: this.autobase.inputs,
      defaultInput: this.autobase.defaultInput,
      defaultIndexes: this.defaultIndexes
    }
  }

  bee (key) {
    if (key.key) {
      // was given a hypercore
      key = key.key
    }

    let keyBuf, keyStr
    if (Buffer.isBuffer(key)) {
      keyBuf = key
      keyStr = key.toString('hex')
    } else {
      keyBuf = Buffer.from(key, 'hex')
      keyStr = key
    }

    if (!this._inputBees.has(keyStr)) {
      const core = this.autobase.inputs.find(core => core.key.equals(keyBuf))
      if (!core) throw new Error('Not an input')
      const bee = new Hyperbee(core, {extension: false, keyEncoding: 'utf-8', valueEncoding: this._valueEncoding})
      modifyBee(bee, this.autobase)
      this._inputBees.set(keyStr, bee)
    }
    return this._inputBees.get(keyStr)
  }

  get defaultBee () {
    if (!this.autobase.defaultInput) throw new Error('No default input has been set')
    return this.bee(this.autobase.defaultInput.key)
  }

  addInput (input) {
    if (this.autobase.inputs.find(core => core.key.equals(input.key))) {
      return
    }
    this.autobase.addInput(input)
  }

  removeInput (input) {
    if (!this.autobase.inputs.find(core => core.key.equals(input.key))) {
      return
    }
    this._inputBees.delete(input.key.toString('hex'))
    this.autobase.removeInput(input)
  }

  createReadStream (...args) {
    return this.autobase.createReadStream(...args)
  }

  createHistoryStream (...args) {
    return this.indexBee.createHistoryStream(...args)
  }

  async get (...args) {
    return await this.indexBee.get(...args)
  }

  async put (...args) {
    return await this.defaultBee.put(...args)
  }

  async del (...args) {
    return await this.defaultBee.del(...args)
  }

  async sub (prefix, opts) {
    const indexBeeSub = this.indexBee.sub(prefix, opts)
    const defaultBeeSub = this.defaultBee.sub(prefix, opts)
    indexBeeSub.put = defaultBeeSub.put.bind(defaultBeeSub)
    indexBeeSub.del = defaultBeeSub.put.bind(defaultBeeSub)

    return indexBeeSub
  }

  async _apply (batch) {
    const b = this.indexBee.batch({ update: false })
    for (const node of batch) {
      let op = undefined
      try {
        op = HyperbeeMessages.Node.decode(node.value)
      } catch (e) {
        // skip: this is most likely the header message
        continue
      }

      // TODO: handle conflicts

      if (op.key) {
        const key = op.key.toString('utf-8')
        const value = op.value ? this._valueEncoder.decode(op.value) : undefined

        if (value) await b.put(key, value)
        else await b.del(key)
      } 
    }
    await b.flush()
  }
}

function modifyBee (bee, autobase) {
  // HACK
  // we proxy the core given to bee to abstract away all of the autobase wrapping
  // there's probably a better way to do this!
  // -prf
  const core = bee._feed

  bee._feed = new Proxy(core, {
    get (target, prop) {
      if (prop === 'append') {
        return v => {
          return autobase.append(v, null, core)
        }
      } else if (prop === 'get') {
        return async (index, opts) => {
          
          opts = opts || {}
          const _valueEncoding = opts.valueEncoding
          opts.valueEncoding = {
            buffer: true,
            encodingLength: () => {},
            encode: () => {},
            decode: (buf, offset, end) => {
              try {
                const parsed = InputNode.decode(buf)
                buf = parsed.value
              } catch (e) {
                // this should never happen?
              }
              return _valueEncoding ? _valueEncoding.decode(buf, 0, buf.length) : args[0]
            }
          }
          return await core.get(index + 1, opts)
        }
      } else if (prop === 'length') {
        return Math.max(0, core.length - 1)
      }
      return core[prop]
    }
  })
}

module.exports = Autobee
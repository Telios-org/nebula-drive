
const Hyperbee = require('hyperbee');

class Autobee {
  constructor(autobase, opts) {

    this.autobase = autobase;
    this.subs = {}
    const index = autobase.createRebasedIndex({
      ...opts,
      unwrap: true,
      apply: this._apply.bind(this)
    })

    this.bee = new Hyperbee(index, {
      ...opts,
      extension: false
    })
  }

  ready() {
    return this.autobase.ready()
  }

  _encode(value, change, seq) {
    return JSON.stringify({ value, change: change.toString('hex'), seq })
  }

  _decode(raw) {
    return JSON.parse(raw)
  }

  async _apply(batch, clocks, change) {
    const self = this
    const localClock = clocks.local
    let b = this.bee.batch({ update: false })

    for (const node of batch) {

      const op = JSON.parse(node.value.toString())

      if(op.type === 'del') {
        if (op.value.sub) {
          const sub = this.subs[op.value.sub]

          b = sub.batch({ update: false })
          await b.del(op.key)
        }
      }

      if (op.type === 'put') {
        if (op.value.__sub) {
          const sub = this.subs[op.value.__sub]

          delete op.value.__sub;

          b = sub.batch({ update: false })
        }

        const existing = await b.get(op.key, { update: false })
        await b.put(op.key, this._encode(op.value, change, node.seq))
        if (!existing) continue
        await handleConflict(existing)
      }
    }

    return await b.flush()

    async function handleConflict(existing) {
      const { change: existingChange, seq: existingSeq } = self._decode(existing.value)
      // If the existing write is not causally contained in the current clock.
      // TODO: Write a helper for this.
      const conflictKey = ['_conflict', existing.key].join('/')
      if (!localClock.has(existingChange) || (localClock.get(existingChange) < existingSeq)) {
        await b.put(conflictKey, existing.value)
      } else {
        await b.del(conflictKey)
      }
    }
  }

  async put(key, value, opts) {
    const op = Buffer.from(JSON.stringify({ type: 'put', key, value }))
    return await this.autobase.append(op, opts)
  }

  async get(key, opts = {}) {
    let b = this.bee

    if (opts.sub) {
      b = this.subs[opts.sub]
    }

    const node = await b.get(key)
    if (!node) return null
    node.value = this._decode(node.value).value
    return node
  }

  async del(key, value) {
    const op = Buffer.from(JSON.stringify({ type: 'del', key, value}))
    return await this.autobase.append(op)
  }

  async sub(key) {
    const sub = await this.bee.sub(key)
    this.subs[key] = sub
    return this
  }

  createReadStream(opts) {
    return this.autobase.createReadStream(opts)
  }

  createHistoryStream(opts) {
    return this.bee.createHistoryStream(opts)
  }
}

module.exports = Autobee;
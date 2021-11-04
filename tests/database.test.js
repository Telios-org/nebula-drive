const tape = require('tape')
const _test = require('tape-promise').default
const test = _test(tape)
const Database = require('../lib/database')
const ram = require('random-access-memory')
const DHT = require('@hyperswarm/dht')

test('Database - Create new db', async t => {
  t.plan(1)
  
  const keyPair = DHT.keyPair()
  const encryptionKey = Buffer.alloc(32, 'hello world')

  try {
    const database = new Database(ram, {
      keyPair,
      encryptionKey
    })

    await database.ready()

    t.ok(database.localMetaCore.key.toString('hex'))
  } catch (err) {
    console.log('ERROR: ', err)
    t.error(err)
  }
})

test('Database - Test put/get', async t => {
  t.plan(1)
  
  const keyPair = DHT.keyPair()
  const encryptionKey = Buffer.alloc(32, 'hello world')

  try {
    const database = new Database(ram, {
      keyPair,
      encryptionKey
    })

    await database.ready()
    
    const collection = await database.collection('foobar')
    await collection.put('foo', { hello: 'bar' })

    const item = await collection.get('foo')

    t.equals(item.value.hello, 'bar')
  } catch (err) {
    t.error(err)
  }
})

test('Database - delete from hyperbee', async t => {
  t.plan(1)
  
  const keyPair = DHT.keyPair()
  const encryptionKey = Buffer.alloc(32, 'hello world')

  try {
    const database = new Database(ram, {
      keyPair,
      encryptionKey
    })

    await database.ready()
    
    const collection = await database.collection('foobar')
    await collection.put('foo', { hello: 'bar' })
    await collection.del('foo')

    const item = await collection.get('foo')

    t.equals(item, null)
  } catch (err) {
    t.error(err)
  }
})
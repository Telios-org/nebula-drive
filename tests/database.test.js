const tape = require('tape')
const _test = require('tape-promise').default
const test = _test(tape)
const Database = require('../lib/database')
const ram = require('random-access-memory')

let database = null

test('Database - Create new db', async t => {
  t.plan(1)

  try {
    database = new Database(ram, null)
    await database.db.ready()

    t.ok(database.feed.key.toString('hex'))
  } catch (err) {
    console.log('ERROR: ', err)
    t.error(err)
  }
})

test('Database - Test put/get', async t => {
  t.plan(1)

  try {
    const collection = await database.collection('foobar')
    await collection.put('yo', { hello: 'world' })

    const item = await collection.get('yo')

    t.equals(item.value.hello, 'world')
  } catch (err) {
    t.error(err)
  }
})
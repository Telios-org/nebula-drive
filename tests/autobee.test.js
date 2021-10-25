const tape = require('tape')
const Hypercore = require('hypercore-encrypt')
const Autobase = require('autobase')
const Autobee = require('../lib/autobee')
const ram = require('random-access-memory')

tape('Autobee - create local writer', async t => {
  const localWriterCore = new Hypercore(ram)
  const base = new Autobase([localWriterCore], { input: localWriterCore })
  const autobee = new Autobee(base, {
    keyEncoding: 'utf-8',
    valueEncoding: 'json'
  })

  // const stream = autobee.createReadStream({
  //   live: true
  // })

  // stream.on('data', data => {
  //   const input = JSON.parse(data.value.toString())
  //   console.log(input.key)
  // })

  await autobee.put('foo', { hello: 'world' })
  // await autobee.put('foo1', { hello: '2' })
  // await autobee.put('foo2', { hello: '3' })
  // await autobee.put('foo3', { hello: '4' })

  // await autobee.put('foo5', { hello: '5' })

  // setTimeout(async () => {
  //   await autobee.put('Its me', { hello: 'djame' })
  // }, 5000)


  // setTimeout(async () => {
  //   await autobee.put('foo9', { hello: '9' })
  // }, 6000)

  let result = await autobee.get('foo')
  t.equals(result.value.hello, 'world')
})

tape('Autobee - create sub', async t => {
  const localWriterCore = new Hypercore(ram)
  const base = new Autobase([localWriterCore], { input: localWriterCore })
  const autobee = new Autobee(base, {
    keyEncoding: 'utf-8',
    valueEncoding: 'json'
  })

  await autobee.sub('testsub')

  await autobee.put('foo', { hello: 'world', __sub: 'testsub' })

  const result = await autobee.get('foo', { sub: 'testsub' })

  t.equals(result.value.hello, 'world')
})
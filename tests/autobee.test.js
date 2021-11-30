const tape = require('tape')
const Hypercore = require('../lib/core')
const Autobase = require('autobase')
const Autobee = require('../lib/autobee')
const ram = require('random-access-memory')

tape('Autobee - create local writer', async t => {
  const localOutput= Hypercore(ram, { server: true, client: true })
  const localInput = Hypercore(ram, { server: true, client: true })
  const base = new Autobase([localInput], { input: localInput, outputs: localOutput })
  const autobee = new Autobee(base, {
    keyEncoding: 'utf-8',
    valueEncoding: 'json'
  })

  await autobee.put('foo', { hello: 'world' })

  let result = await autobee.get('foo')
  t.equals(result.value.hello, 'world')
})

tape('Autobee - create sub', async t => {
  const encryptionKey = Buffer.alloc(32, 'hello world')
  const localInput = Hypercore(ram, { encryptionKey, server: true, client: true })
  const localOutput = Hypercore(ram, { encryptionKey })
  const base = new Autobase([localInput], { input: localInput, outputs: localOutput })
  const autobee = new Autobee(base, {
    keyEncoding: 'utf-8', // can be set to undefined (binary), utf-8, ascii or and abstract-encoding
    valueEncoding: 'json'
  })

  await autobee.sub('testsub')

  await autobee.put('foo', { hello: 'world', __sub: 'testsub' })

  const result = await autobee.get('foo', { sub: 'testsub' })
  
  t.equals(result.value.hello, 'world')
})
const tape = require('tape')
const _test = require('tape-promise').default
const test = _test(tape)
const fs = require('fs')
const path = require('path')
const del = require('del')
const Drive = require('..')
const EventEmitter = require('events')
const DHT = require('@hyperswarm/dht')
const ram = require('random-access-memory')

// const { signingKeypair } = Account.makeKeys()

// const keyPair  = {
//   publicKey: Buffer.from(signingKeypair.publicKey, 'hex'),
//   secretKey: Buffer.from(signingKeypair.privateKey, 'hex')
// }
const keyPair = DHT.keyPair()
const keyPair2 = DHT.keyPair()
const keyPair3 = DHT.keyPair()
const keyPair4 = DHT.keyPair()

let drive
let drive2
let drive3
let drive4
let drive5
let hyperFiles = []

const encryptionKey = Buffer.alloc(32, 'hello world')

test('Drive - Create', async t => {
  t.plan(5)

  await cleanup()

  drive = new Drive(__dirname + '/drive', null, {
    storage: ram,
    keyPair,
    encryptionKey,
    swarmOpts: {
      server: true,
      client: true
    }
  })

  await drive.ready()

  t.ok(drive.publicKey, `Drive has public key ${drive.publicKey}`)
  t.ok(drive.keyPair, `Drive has peer keypair`)
  t.ok(drive.db, `Drive has Database`)
  t.ok(drive.drivePath, `Drive has path ${drive.drivePath}`)
  t.equals(true, drive.opened, `Drive is open`)
})

test('Drive - Upload Local Encrypted File', async t => {
  t.plan(24)

  try {
    const readStream = fs.createReadStream(path.join(__dirname, '/data/email.eml'))
    const file = await drive.writeFile('/email/rawEmailEncrypted.eml', readStream, { encrypted: true })

    hyperFiles.push(file)

    t.ok(file.key, `File was encrypted with key`)
    t.ok(file.header, `File was encrypted with header`)
    t.ok(file.hash, `Hash of file was returned ${file.hash}`)
    t.ok(file.size, `Size of file in bytes was returned ${file.size}`)

    for (let i = 0; i < 20; i++) {
      const readStream = fs.createReadStream(path.join(__dirname, '/data/email.eml'))
      const file = await drive.writeFile(`/email/rawEmailEncrypted${i}.eml`, readStream, { encrypted: true })
      t.ok(file)
    }
  } catch (e) {
    t.error(e)
  }
})

test('Drive - Read Local Encrypted File', async t => {
  t.plan(2)

  const origFile = fs.readFileSync(path.join(__dirname, '/data/email.eml'), { encoding: 'utf-8' })
  const stream = await drive.readFile('/email/rawEmailEncrypted.eml')
  let decrypted = ''

  stream.on('data', chunk => {
    decrypted += chunk.toString('utf-8')
  })

  stream.on('end', () => {
    t.ok(decrypted.length, 'Returned encrypted data')
    t.equals(origFile.length, decrypted.length, 'Decrypted file matches original')
  })
})

test('Drive - Create Seed Peer', async t => {
  t.plan(22)

  drive2 = new Drive(__dirname + '/drive2', drive.publicKey, {
    keyPair: keyPair2,
    // encryptionKey: drive.encryptionKey,
    swarmOpts: {
      server: true,
      client: true
    }
  })

  await drive2.ready()
  // await drive2.addPeer(drive.publicKey)

  drive2.on('file-sync', async (file) => {
    t.ok(file.uuid, `File has synced from remote peer`)
  })

  const readStream = fs.createReadStream(path.join(__dirname, '/data/test.doc'))
  const file = await drive.writeFile('/email/test.doc', readStream)

  hyperFiles.push(file)
})

test('Drive - Fetch Files from Remote Drive', async t => {
  t.plan(4)

  drive3 = new Drive(__dirname + '/drive3', null, {
    keyPair: keyPair3,
    swarmOpts: {
      server: true,
      client: true
    }
  })

  await drive3.ready()

  await drive3.fetchFileBatch(hyperFiles, (stream, file) => {
    return new Promise((resolve, reject) => {
      let content = ''

      stream.on('data', chunk => {
        content += chunk.toString()
      })

      stream.on('error', (err) => {
        t.error(err, err.message)
        resolve()
      })

      stream.on('end', () => {
        t.ok(file.hash, `File has hash ${file.hash}`)
        t.ok(content.length, `File downloaded from remote peer`)
        resolve()
      })
    })
  })
})

test('Drive - Fail to Fetch Files from Remote Drive', async t => {
  t.plan(2)

  drive4 = new Drive(__dirname + '/drive4', null, {
    keyPair: keyPair3,
    swarmOpts: {
      server: true,
      client: true
    }
  })
  drive5 = new Drive(__dirname + '/drive5', null, {
    keyPair: keyPair4,
    swarmOpts: {
      server: true,
      client: true
    }
  })

  await drive4.ready()
  await drive5.ready()

  const readStream = fs.createReadStream(path.join(__dirname, '/data/email.eml'))
  const file = await drive4.writeFile('/email/rawEmailEncrypted2.eml', readStream, { encrypted: true })

  await drive4.unlink(file.path)
  
  await drive5.fetchFileBatch([file], (stream, file) => {
    return new Promise((resolve, reject) => {
      let content = ''

      stream.on('data', chunk => {
        content += chunk.toString()
      })

      stream.on('error', (err) => {
        t.ok(err.message, `Error has message: ${err.message}`)
        t.equals(file.hash, err.fileHash, `Failed file hash matches the has in the request,`)
        resolve()
      })
    })
  })
})

test('Drive - Unlink Local File', async t => {
  t.plan(2)

  const drive1Size = drive.info().size
  const drive2Size = drive2.info().size

  drive.on('file-unlink', file => {
    t.ok(drive1Size > drive.info().size, `Drive1 size before: ${drive1Size} > size after: ${drive.info().size}`)
  })

  drive2.on('file-unlink', file => {
    t.ok(drive2Size > drive2.info().size, `Drive2 size before: ${drive2Size} > size after: ${drive2.info().size}`)
  })

  await drive.unlink('/email/rawEmailEncrypted.eml')
})


test('Drive - Receive messages', async t => {
  t.plan(1)

  drive.on('message', (publicKey, data) => {
    const msg = JSON.parse(data.toString())
    t.ok(msg, 'Drive can receive messages.')
  })

  const node = new DHT()
  const noiseSocket = node.connect(keyPair.publicKey)

  noiseSocket.on('open', function () {
    noiseSocket.end(JSON.stringify({
      type: 'newMail',
      meta: 'meta mail message'
    }))
  })
})

test('Drive - Get total size in bytes', t => {
  t.plan(3)

  t.ok(drive.info(), `Drive 1 has size ${drive.info().size}`)
  t.ok(drive2.info(), `Drive 2 has size ${drive2.info().size}`)
  t.ok(drive3.info(), `Drive 3 has size ${drive3.info().size}`)
})

test.onFinish(async () => {
  await drive.close()
  await drive2.close()
  await drive3.close()
  await drive4.close()
  await drive5.close()

  await cleanup()

  process.exit(0)
})


async function cleanup() {
  if (fs.existsSync(path.join(__dirname, '/drive'))) {
    await del([
      path.join(__dirname, '/drive')
    ])
  }

  if (fs.existsSync(path.join(__dirname, '/drive2'))) {
    await del([
      path.join(__dirname, '/drive2')
    ])
  }

  if (fs.existsSync(path.join(__dirname, '/drive3'))) {
    await del([
      path.join(__dirname, '/drive3')
    ])
  }

  if (fs.existsSync(path.join(__dirname, '/drive4'))) {
    await del([
      path.join(__dirname, '/drive4')
    ])
  }

  if (fs.existsSync(path.join(__dirname, '/drive5'))) {
    await del([
      path.join(__dirname, '/drive5')
    ])
  }
}
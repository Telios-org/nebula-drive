const tape = require('tape');
const _test = require('tape-promise').default;
const test = _test(tape);
const HyperDB = require('../lib/hyperdb');

let hyperDB = null;

test('HyperDB - Create new db', async t => {
  t.plan(1);

  try {
    hyperDB = new HyperDB('./tests/storage/MultiB', null);
    await hyperDB.db.ready();

    t.ok(hyperDB.db.feed.key.toString('hex'));
  } catch (err) {
    console.log('ERROR: ', err);
    t.error(err);
  }
});

test('HyperDB - Test put/get', async t => {
  t.plan(1);

  try {
    const collection = await hyperDB.collection('test');

    await collection.put('hello', 
      {
        data: {
          value: 'world'
        }
      }
    );
    
    const item = await collection.get('hello');

    t.equals(item.data.value, 'world');
  } catch (err) {
    t.error(err);
  }
});
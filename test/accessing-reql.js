const path = require('path')
const config = require(path.join(__dirname, '/config.js'))
const r = require(path.join(__dirname, '/../lib'))({pool: false, silent: true})
const util = require(path.join(__dirname, '/util/common.js'))
const assert = require('assert')
const uuid = util.uuid
const net = require('net')
const {beforeEach, afterEach, describe, it} = require('mocha')

describe('accessing-reql', function () {
  let connection // global connection
  let dbName, tableName

  beforeEach(async () => {
    connection = await r.connect(config)
  })

  afterEach(async () => {
    if (connection) await connection.close()
  })

  it('`run` should throw when called without connection', async () => {
    try {
      await r.expr(1).run()
      assert.fail('should throw')
    } catch (e) {
      assert.equal(e.message, '`run` was called without a connection and no pool has been created after:\nr.expr(1)')
    }
  })

  it('`run` should throw when called with a closed connection', async () => {
    try {
      connection.close()
      assert(!connection.open)

      await r.expr(1).run(connection)
      assert.fail('should throw')
    } catch (e) {
      assert.equal(e.message, '`run` was called with a closed connection after:\nr.expr(1)')
    }
  })

  it('should be able to create a db, a table, insert array into table, delete array from table, drop table and drop db', async () => {
    dbName = uuid()
    tableName = uuid()

    let result = await r.dbCreate(dbName).run(connection)
    assert.equal(result.config_changes.length, 1)
    assert.equal(result.dbs_created, 1)

    result = await r.db(dbName).tableCreate(tableName).run(connection)
    assert.equal(result.tables_created, 1)

    result = await r.db(dbName).table(tableName).insert(new Array(100).fill({})).run(connection)
    assert.equal(result.inserted, 100)

    result = await r.db(dbName).table(tableName).delete().run(connection)
    assert.equal(result.deleted, 100)

    result = await r.db(dbName).tableDrop(tableName).run(connection)
    assert.equal(result.config_changes.length, 1)
    assert.equal(result.tables_dropped, 1)

    result = await r.dbDrop(dbName).run(connection)
    assert.equal(result.config_changes.length, 1)
    assert.equal(result.dbs_dropped, 1)
  })

  it('`run` should use the default database', async () => {
    dbName = uuid()
    tableName = uuid()

    let result = await r.dbCreate(dbName).run(connection)
    assert.equal(result.dbs_created, 1)

    result = await r.db(dbName).tableCreate(tableName).run(connection)
    assert.equal(result.tables_created, 1)

    await connection.close()
    assert(!connection.open)

    connection = await r.connect({db: dbName, host: config.host, port: config.port, authKey: config.authKey})
    assert(connection)

    result = await r.tableList().run(connection)
    assert.deepEqual(result, [tableName])
  })

  it('`use` should work', async function () {
    dbName = uuid()
    tableName = uuid()

    let result = await r.dbCreate(dbName).run(connection)
    assert.equal(result.dbs_created, 1)

    result = await r.db(dbName).tableCreate(tableName).run(connection)
    assert.equal(result.tables_created, 1)

    connection.use(dbName)

    result = await r.tableList().run(connection)
    assert.deepEqual(result, [tableName])
  })

  it('`reconnect` should work', async function () {
    await connection.close()
    assert(!connection.open)

    connection = await connection.reconnect()
    assert(connection.open)
  })

  it('`reconnect` should work with options', async function () {
    connection = await connection.reconnect({noreplyWait: true})
    assert(connection.open)

    let result = await r.expr(1).run(connection)
    assert.equal(result, 1)

    connection = await connection.reconnect({noreplyWait: false})
    assert(connection.open)

    result = await r.expr(1).run(connection)
    assert.equal(result, 1)

    connection = await connection.reconnect()
    assert(connection)

    result = await r.expr(1).run(connection)
    assert.equal(result, 1)
  })

  it('`noReplyWait` should throw', async function () {
    try {
      await connection.noReplyWait()
      assert.fail('should throw')
    } catch (e) {
      assert.equal(e.message, 'Did you mean to use `noreplyWait` instead of `noReplyWait`?')
    }
  })

  it('`noreplyWait` should work', async function () {
    dbName = uuid()
    tableName = uuid()
    const largeishObject = Array(10000).fill(Math.random()).map((random) => r.expr({random}))

    await r.dbCreate(dbName).run(connection)
    await r.db(dbName).tableCreate(tableName).run(connection)

    let result = await r.db(dbName).table(tableName).insert(largeishObject).run(connection, {noreply: true})
    assert.equal(result, undefined)

    result = await r.db(dbName).table(tableName).count().run(connection)
    assert.equal(result, 0)

    result = await connection.noreplyWait()
    assert.equal(result, undefined)

    result = await r.db(dbName).table(tableName).count().run(connection)
    assert.equal(result, 10000)
  })

  it('`run` should take an argument', async function () {
    let result = await r.expr(1).run(connection, {readMode: 'primary'})
    assert.equal(result, 1)

    result = await r.expr(1).run(connection, {readMode: 'majority'})
    assert.equal(result, 1)

    result = await r.expr(1).run(connection, {profile: false})
    assert.equal(result, 1)

    result = await r.expr(1).run(connection, {profile: true})
    assert(result.profile)
    assert.equal(result.result, 1)

    result = await r.expr(1).run(connection, {durability: 'soft'})
    assert.equal(result, 1)

    result = await r.expr(1).run(connection, {durability: 'hard'})
    assert.equal(result, 1)
  })

  it('`run` should throw on an unrecognized argument', async function () {
    try {
      await r.expr(1).run(connection, {foo: 'bar'})
      assert.fail('should throw')
    } catch (e) {
      assert.equal(e.message, 'Unrecognized option `foo` in `run`. Available options are readMode <string>, durability <string>, noreply <bool>, timeFormat <string>, groupFormat: <string>, profile <bool>, binaryFormat <bool>, cursor <bool>, stream <bool>.')
    }
  })

  it('`r()` should be a shortcut for r.expr()', async function () {
    const result = await r(1).run(connection)
    assert.deepEqual(result, 1)
  })

  it('`timeFormat` should work', async function () {
    let result = await r.now().run(connection)
    assert(result instanceof Date)

    result = await r.now().run(connection, {timeFormat: 'native'})
    assert(result instanceof Date)

    result = await r.now().run(connection, {timeFormat: 'raw'})
    assert.equal(result.$reql_type$, 'TIME')
  })

  it('`binaryFormat` should work', async function () {
    const result = await r.binary(Buffer.from([1, 2, 3])).run(connection, {binaryFormat: 'raw'})
    assert.equal(result.$reql_type$, 'BINARY')
  })

  it('`groupFormat` should work', async function () {
    const result = await r.expr([
      {name: 'Michel', grownUp: true},
      {name: 'Laurent', grownUp: true},
      {name: 'Sophie', grownUp: true},
      {name: 'Luke', grownUp: false},
      {name: 'Mino', grownUp: false}
    ]).group('grownUp').run(connection, {groupFormat: 'raw'})

    assert.deepEqual(result, {
      '$reql_type$': 'GROUPED_DATA',
      'data': [[false, [{'grownUp': false, 'name': 'Luke'}, {
        'grownUp': false,
        'name': 'Mino'
      }]], [true, [{'grownUp': true, 'name': 'Michel'}, {'grownUp': true, 'name': 'Laurent'}, {
        'grownUp': true,
        'name': 'Sophie'
      }]]]
    })
  })

  it('`profile` should work', async function () {
    let result = await r.expr(true).run(connection, {profile: false})
    assert(result)

    result = await r.expr(true).run(connection, {profile: true})
    assert(result.profile)
    assert.equal(result.result, true)

    result = await r.expr(true).run(connection, {profile: false})
    assert.equal(result, true)
  })

  it('`timeout` option should work', function (done) {
    const port = Math.floor(Math.random() * (65535 - 1025) + 1025)
    const server = net.createServer((socket) => {
      setTimeout(() => {
        socket.destroy()
        server.close()
        done()
      }, 1000)
    }).listen(port, () => {
      r.connect({
        port: port,
        timeout: 1
      }).error((err) => {
        assert.equal(err.message, 'Failed to connect to localhost:' + port + ' in less than 1s.')
      })
    })
  })

  it('`server` should work', async function () {
    const response = await connection.server()
    assert(typeof response.name === 'string')
    assert(typeof response.id === 'string')
  })

  it('`grant` should work', async function () {
    const restrictedDbName = uuid()
    const restrictedTableName = uuid()

    let result = await r.dbCreate(restrictedDbName).run(connection)
    assert.equal(result.config_changes.length, 1)
    assert.equal(result.dbs_created, 1)

    result = await r.db(restrictedDbName).tableCreate(restrictedTableName).run(connection)
    assert.equal(result.tables_created, 1)

    const user = uuid()
    const password = uuid()
    result = await r.db('rethinkdb').table('users').insert({
      id: user,
      password: password
    }).run(connection)
    assert.equal(result.inserted, 1)
    result = await r.db(restrictedDbName).table(restrictedTableName).grant(user, {
      read: true, write: true, config: true
    }).run(connection)
    assert.deepEqual(result, {
      granted: 1,
      permissions_changes: [{
        new_val: {
          config: true,
          read: true,
          write: true
        },
        old_val: null
      }]
    })
  })

  it('If `servers` is specified, it cannot be empty', function () {
    try {
      require(path.join(__dirname, '/../lib'))({
        servers: []
      })
      assert.fail('should throw')
    } catch (e) {
      assert.equal(e.message, 'If `servers` is an array, it must contain at least one server.')
    }
  })

  it('should not throw an error (since 1.13, the token is now stored outside the query): `connection` should extend events.Emitter and emit an error if the server failed to parse the protobuf message', async function () {
    connection.addListener('error', assert.ifError)
    const result = await Array(687).fill(1).reduce((acc, curr) => acc.add(curr), r.expr(1)).run(connection)
    assert.equal(result, 688)
  })
})

rebirthdb-js
-------------

[![Build Status](https://travis-ci.org/RebirthDB/rebirthdb-js.svg?branch=master)](https://travis-ci.org/RebirthDB/rebirthdb-js)
[![Coverage Status](https://coveralls.io/repos/github/RebirthDB/rebirthdb-js/badge.svg?branch=master)](https://coveralls.io/github/RebirthDB/rebirthdb-js?branch=master)

A Node.js driver for RebirthDB (and the older RethinkDB).

### Install

```
npm install rebirthdb-js
```

### Quick start

Rebirthdb-js is the official Javascript driver for RebirthDB.

```js
const r = require( 'rebirthdb-js' )();

const users = await r.table( 'users' ).run();
```

### Features

Rebirthdb-js ships with a few interesting features:


#### Importing the driver

When you import the driver, as soon as you execute the module, you will create
a default connection pool (except if you pass `{ pool: false }`. The options you
can pass are:

- `db`: `<string>` - The default database to use if none is mentioned.
- `user`: `<string>` - The RethinkDB user, default value is admin.
- `password`: `<string>` - The password for the user, default value is an empty string.
- `discovery`: `<boolean>` - When true, the driver will regularly pull data from the table `server_status` to
keep a list of updated hosts, default `false`
- `pool`: `<boolean>` - Set it to `false`, if you do not want to use a connection pool.
- `buffer`: `<number>` - Minimum number of connections available in the pool, default `50`
- `max`: `<number>` - Maximum number of connections available in the pool, default `1000`
- `timeout`: `<number>` - The number of seconds for a connection to be opened, default `20`
- `pingInterval`: <number> - if `> 0`, the connection will be pinged every `pingInterval` seconds, default `-1`
- `timeoutError`: `<number>` - Wait time before reconnecting in case of an error (in ms), default 1000
- `timeoutGb`: `<number>` - How long the pool keep a connection that hasn't been used (in ms), default 60\*60\*1000
- `maxExponent`: `<number>` - The maximum timeout before trying to reconnect is 2^maxExponent x timeoutError, default 6 (~60 seconds for the longest wait)
- `silent`: <boolean> - console.error errors, default `false`
- `servers`: an array of objects `{ host: <string>, port: <number> }` representing RethinkDB nodes to connect to
- `optionalRun`: <boolean> - if `false`, yielding a query will not run it, default `true`
- `log`: <function> - will be called with the log events by the pool master

In case of a single instance, you can directly pass `host` and `port` in the top level parameters.

Examples:

```js
// connect to localhost:8080, and let the driver find other instances
const r = require( 'rebirthdb-js' )( {
    discovery: true
} );

// connect to and only to localhost:8080
const r = require( 'rebirthdb-js' )();

// Do not create a connection pool
const r = require( 'rebirthdb-js' )( {
    pool: false
} );

// Connect to a cluster seeding from `192.168.0.100`, `192.168.0.101`, `192.168.0.102`
const r = require( 'rebirthdb-js' )( {
    servers: [ {
        host: '192.168.0.100',
        port: 28015
    }, {
        host: '192.168.0.101',
        port: 28015
    }, {
        host: '192.168.0.102',
        port: 28015
    } ]
} );

// Connect to a cluster containing `192.168.0.100`, `192.168.0.100`, `192.168.0.102` and
// use a maximum of 3000 connections and try to keep 300 connections available at all time.
const r = require( 'rebirthdb-js' )( {
    servers: [ {
        host: '192.168.0.100',
        port: 28015
    }, {
        host: '192.168.0.101',
        port: 28015
    }, {
        host: '192.168.0.102',
        port: 28015
    } ],
    buffer: 300,
    max: 3000
} );
```

You can also pass `{ cursor: true }` if you want to retrieve RethinkDB streams as cursors
and not arrays by default.

_Note_: The option `{ stream: true }` that asynchronously returns a stream is deprecated. Use `toStream` instead.

_Note_: The option `{ optionalRun: false }` will disable the optional run for all instances of the driver.

_Note_: Connections are created with TCP keep alive turned on, but some routers seem to ignore this setting. To make
sure that your connections are kept alive, set the `pingInterval` to the interval in seconds you want the
driver to ping the connection.

_Note_: The error `__rebirthdb-js_ping__` is used for internal purposes (ping). Do not use it.

#### Connection pool

As mentioned before, `rebirthdb-js` has a connection pool and manage all the connections
itself. The connection pool is initialized as soon as you execute the module.

You should never have to worry about connections in rebirthdb-js. Connections are created
as they are needed, and in case of a host failure, the pool will try to open connections with an
exponential back off algorithm.

The driver execute one query per connection. Now that [rethinkdb/rethinkdb#3296](https://github.com/rethinkdb/rethinkdb/issues/3296)
is solved, this behavior may be changed in the future.

Because the connection pool will keep some connections available, a script will not
terminate. If you have finished executing your queries and want your Node.js script
to exit, you need to drain the pool with:

```js
r.getPoolMaster().drain();
```

The pool master by default will log all errors/new states on `stderr`. If you do not
want to pollute `stderr`, pass `silent: true` when you import the driver and
provide your own `log` method.

```js
const r = require( 'rebirthdb-js' )( {
    silent: true,
    log: message => {
        console.log( message );
    }
} );
```

##### Advanced details about the pool

The pool is composed of a `PoolMaster` that retrieve connections for `n` pools where `n` is the number of
servers the driver is connected to. Each pool is connected to a unique host.

To access the pool master, you can call the method `r.getPoolMaster()`.

The pool emits a few events:
- `draining`: when `drain` is called
- `queueing`: when a query is added/removed from the queue (queries waiting for a connection), the size of the queue is provided
- `size`: when the number of connections changes, the number of connections is provided
- `available-size`: when the number of available connections changes, the number of available connections is provided

You can get the number of connections (opened or being opened).
```js
r.getPoolMaster().getLength();
```

You can also get the number of available connections (idle connections, without
a query running on it).

```js
r.getPoolMaster().getAvailableLength();
```

You can also drain the pool as mentionned earlier with;

```js
r.getPoolMaster().drain();
```

You can access all the pools with:
```js
r.getPoolMaster().getPools();
```

The pool master emits the `healthy` when its state change. Its state is defined as:
- healthy when at least one pool is healthy: Queries can be immediately executed or will be queued.
- not healthy when no pool is healthy: Queries will immediately fail.

A pool being healthy is it has at least one available connection, or it was just
created and opening a connection hasn't failed.

```js
r.getPoolMaster().on( 'healthy', healthy => {
    if ( healthy === true ) {
        console.log('We can run queries.');
    }
    else {
        console.log('No queries can be run.');
    }
} );
```

##### Note about connections

If you do not wish to use rebirthdb-js connection pool, you can implement yours. The
connections created with rebirthdb-js emits a "release" event when they receive an
error, an atom, or the end (or full) sequence.

A connection can also emit a "timeout" event if the underlying connection times out.


#### Arrays by default, not cursors

Rebirthdb-js automatically coerces cursors to arrays. If you need a raw cursor,
you can call the `run` command with the option `{ cursor: true }` or import the
driver with `{ cursor: true }`.

```js
r.expr( [ 1, 2, 3 ] ).run().then( result => {
    console.log( JSON.stringify( result ) ); // prints [1, 2, 3]
} );
```

```js
r
    .expr( [ 1, 2, 3 ] )
    .run( {
        cursor: true
    } )
    .then( cursor => {
        cursor.toArray().then( result => {
            console.log( JSON.stringify( result ) ); // prints [1, 2, 3]
        } );
    } );
```

__Note__: If a query returns a cursor, the connection will not be
released as long as the cursor hasn't fetched everything or has been closed.

#### Readable streams

[Readable streams](http://nodejs.org/api/stream.html#stream_class_stream_readable) can be
synchronously returned with the `toStream([connection])` method.

```js
const fs = require( 'fs' );
const file = fs.createWriteStream( 'file.txt' );

const r = require( 'rebirthdb-js' )();

r
    .table( 'users' )
    .toStream()
    .on( 'error', console.error.bind( console ) )
    .pipe( file )
    .on( 'error', console.error.bind( console ) )
    .on( 'end', () => {
        r.getPool().drain();
    } );
```

_Note:_ The stream will emit an error if you provide it with a single value (streams, arrays
and grouped data work fine).

_Note:_ `null` values are currently dropped from streams.

#### Writable and Transform streams

You can create a [Writable](http://nodejs.org/api/stream.html#stream_class_stream_writable)
or [Transform](http://nodejs.org/api/stream.html#stream_class_stream_transform) streams by
calling `toStream([connection, ]{writable: true})` or
`toStream([connection, ]{transform: true})` on a table.

By default, a transform stream will return the saved documents. You can return the primary
key of the new document by passing the option `format: 'primaryKey'`.

This makes a convenient way to dump a file your database.

```js
const file = fs.createReadStream( 'users.json' )
const table = r.table( 'users' ).toStream( {
    writable: true
} );

file.pipe( transformer ) // transformer would be a Transform stream that splits per line and call JSON.parse
    .pipe( table )
    .on( 'finish', () => {
        console.log( 'Done' );
        r.getPool().drain();
    } );
```


#### Global default values

You can set the maximum nesting level and maximum array length on all your queries with:

```js
r.setNestingLevel( <number> )
```

```js
r.setArrayLimit( <number> )
```

#### Undefined values

Rebirthdb-js will ignore the keys/values where the value is `undefined`.


#### Understandable errors


##### Backtraces

If your query fails, the driver will return an error with a backtrace; your query
will be printed and the broken part will be highlighted.

Backtraces in rebirthdb-js are tested and properly formatted. Typically, long backtraces
are split on multiple lines and if the driver cannot serialize the query,
it will provide a better location of the error.


##### Arity errors

The server may return confusing error messages when the wrong number
of arguments is provided (See [rethinkdb/rethinkdb#2463](https://github.com/rethinkdb/rethinkdb/issues/2463) to track progress).
Rebirthdb-js tries to make up for it by catching errors before sending
the query to the server if possible.


#### Performance

The tree representation of the query is built step by step and stored which avoid
recomputing it if the query is re-run.

The code was partially optimized for v8, and is written in pure JavaScript which avoids
errors like [issue #2839](https://github.com/rethinkdb/rethinkdb/issues/2839)


### Run tests

Update `test/config.js` if your RebirthDB instance doesn't run on the default parameters.

Make sure you run a version of Node that supports `async/await` and run:
```
npm test
```

Longer tests for the pool:

```
mocha long_test/discovery.js -t 50000
mocha long_test/static.js -t 50000
```


### FAQ

- __Is it stable?__

  Yes. Rebirthdb-js is the official javaascript driver for RebirthDB.


- __Can I contribute?__

  Feel free to send a pull request. If you want to implement a new feature, please open
  an issue first, especially if it's a non backward compatible one.

### Browserify

To build the browser version of rebirthdb-js, run:

```
node browserify.js
```

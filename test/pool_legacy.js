const path = require( 'path' )
const config = require( './config.js' )
const rebirthdb = require( path.join( __dirname, '/../lib' ) )
const assert = require( 'assert' )
const {
    uuid
} = require( path.join( __dirname, '/util/common.js' ) )
const {
    before,
    after,
    describe,
    it
} = require( 'mocha' )

describe( 'pool legacy', () => {
    let r

    before( async () => {
        r = await rebirthdb( {
            pool: false,
            silent: true
        } )
    } )

    after( async () => {
        await r.getPoolMaster().drain()
    } )

    const options = {
        max: 10,
        buffer: 2,
        host: config.host,
        port: config.port,
        authKey: config.authKey,
        discovery: false,
        silent: true
    }

    it( '`createPool` should create a PoolMaster and `getPoolMaster` should return it', async function() {
        r = r.createPools( options )
        assert.ok( r.getPoolMaster(), 'expected an instance of pool master' )
        assert.equal( r.getPoolMaster().getPools().length, 1, 'expected number of pools is 1' )
    } )

    it( 'The pool should create a buffer', async function() {
        const result = await new Promise( ( resolve, reject ) => {
            setTimeout( () => {
                const numConnections = r.getPool().getAvailableLength()
                numConnections >= options.buffer ?
                    resolve( numConnections ) :
                    reject( new Error( 'expected number of connections to equal option.buffer within 250 msecs' ) )
            }, 50 )
        } )
        assert.equal( options.buffer, result, 'expected buffer option to result in number of created connections' )
    } )

    it( '`run` should work without a connection if a pool exists and the pool should keep a buffer', async function() {
        const numExpr = 5

        let result = await Promise.all( Array( numExpr ).fill( r.expr( 1 ) ).map( ( expr ) => expr.run() ) )
        assert.deepEqual( result, Array( numExpr ).fill( 1 ) )

        const numConnections = r.getPool().getAvailableLength()
        assert.ok( numConnections >= options.buffer + numExpr, 'expected number of connections (' + numConnections + ') to be at least buffer size (' + options.buffer + ') plus number of run expressions (' + numExpr + ')' )
    } )

    it( 'A noreply query should release the connection', async function() {
        const numConnections = r.getPool().getLength()
        await r.expr( 1 ).run( {
            noreply: true
        } )
        assert.equal( numConnections, r.getPool().getLength(), 'number of connections differ before and after a noreply query' )
    } )

    it( 'The pool should not have more than `options.max` connections', async function() {
        const result = await Promise.all( Array( options.max + 1 ).fill( r.expr( 1 ) ).map( ( expr ) => expr.run() ) )
        assert.deepEqual( result, Array( options.max + 1 ).fill( 1 ) )
        assert.equal( options.max, r.getPool().getLength() )
        assert.ok( options.max >= r.getPool().getAvailableLength() )
        assert.equal( r.getPool().getAvailableLength(), r.getPool().getLength() )
    } )

    it( 'The pool should shrink if a connection is not used for some time', async function() {
        r.getPool().setOptions( {
            timeoutGb: 100
        } )

        const result = await Promise.all( Array( 9 ).fill( r.expr( 1 ) ).map( ( expr ) => expr.run() ) )
        assert.deepEqual( result, Array( 9 ).fill( 1 ) )

        const {
            availableLength,
            length
        } = await new Promise( resolve => {
            setTimeout( function() {
                resolve( {
                    availableLength: r.getPool().getAvailableLength(),
                    length: r.getPool().getLength()
                } )
            }, 1000 )
        } )
        assert.equal( availableLength, options.buffer, 'expected available connections to equal buffer size' )
        assert.equal( length, options.buffer, 'expected pool size to equal buffer size' )
    } )

    it( '`poolMaster.drain` should eventually remove all the connections', async function() {
        await r.getPoolMaster().drain()

        assert.equal( r.getPool().getAvailableLength(), 0 )
        assert.equal( r.getPool().getLength(), 0 )
    } )

    describe( 'cursor', function() {
        let r, dbName, tableName

        before( async function() {
            r = rebirthdb( options )
            dbName = uuid()
            tableName = uuid()

            let result = await r.dbCreate( dbName ).run()
            assert.equal( result.dbs_created, 1 )

            result = await r.db( dbName ).tableCreate( tableName ).run()
            assert.equal( result.tables_created, 1 )

            result = await r.db( dbName ).table( tableName ).insert( Array( 10000 ).fill( {} ) ).run()
            assert.equal( result.inserted, 10000 )

            // Making bigger documents to retrieve multiple batches
            result = await r.db( dbName ).table( tableName ).update( {
                'foo': uuid(),
                'fooo': uuid(),
                'foooo': uuid(),
                'fooooo': uuid(),
                'foooooo': uuid(),
                'fooooooo': uuid(),
                'foooooooo': uuid(),
                'fooooooooo': uuid(),
                'foooooooooo': uuid(),
                date: r.now()
            } ).run()
        } )

        after( async function() {
            let result = await r.dbDrop( dbName ).run()
            assert.equal( result.dbs_dropped, 1 )

            await r.getPoolMaster().drain()
        } )

        it( 'The pool should release a connection only when the cursor has fetch everything or get closed', async function() {
            const result = await Promise.all( Array( options.max ).fill( r.db( dbName ).table( tableName ) ).map( ( expr ) => expr.run( {
                cursor: true
            } ) ) )
            assert.equal( result.length, options.max, 'expected to get the same number of results as number of expressions' )
            assert.equal( r.getPool().getAvailableLength(), 0, 'expected no available connections' )
            await result[ 0 ].toArray()
            assert.equal( r.getPool().getAvailableLength(), 1, 'expected available connections' )
            await result[ 1 ].toArray()
            assert.equal( r.getPool().getAvailableLength(), 2, 'expected available connections' )
            await result[ 2 ].close()
            assert.equal( r.getPool().getAvailableLength(), 3, 'expected available connections' )
            // close the 7 next seven cursors
            await Promise.all( [ ...Array( 7 ).keys() ].map( ( key ) => {
                return result[ ( key + 3 ) ].close()
            } ) )
            assert.equal( r.getPool().getAvailableLength(), options.max, 'expected available connections to equal option.max' )
        } )
    } )

    it( 'If the pool cannot create a connection, it should reject queries', async function() {
        const r = rebirthdb( {
            host: 'notarealhost',
            buffer: 1,
            max: 2,
            silent: true
        } )
        try {
            await r.expr( 1 ).run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, 'None of the pools have an opened connection and failed to open a new one.' )
        }
        await r.getPoolMaster().drain()
    } )

    it( 'If the driver cannot create a connection, it should reject queries - timeout', async function() {
        const r = rebirthdb( {
            host: 'notarealhost',
            buffer: 1,
            max: 2,
            silent: true
        } )
        await new Promise( resolve => {
            setTimeout( resolve, 1000 )
        } )
        try {
            await r.expr( 1 ).run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, 'None of the pools have an opened connection and failed to open a new one.' )
        }
        finally {
            await r.getPoolMaster().drain()
        }
    } )

    it( 'If the pool is drained, it should reject queries', async function() {
        const r = rebirthdb( {
            buffer: 1,
            max: 2,
            silent: true
        } )
        await r.getPoolMaster().drain()
        try {
            await r.expr( 1 ).run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, 'None of the pools have an opened connection and failed to open a new one.' )
        }
        finally {
            await r.getPoolMaster().drain()
        }
    } )

    it( 'If the pool is draining, it should reject queries', async function() {
        const r = rebirthdb( {
            buffer: 1,
            max: 2,
            silent: true
        } )
        r.getPoolMaster().drain()
        try {
            await r.expr( 1 ).run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, 'None of the pools have an opened connection and failed to open a new one.' )
        }
        finally {
            await r.getPoolMaster().drain()
        }
    } )

    it( '`drain` should work in case of failures', async function() {
        const r = rebirthdb( {
            buffer: 1,
            max: 2,
            pool: false,
            silent: true
        } )
        r.createPools( {
            port: 80, // non valid port
            silent: true,
            timeoutError: 100
        } )
        const pool = r.getPool()
        await new Promise( resolve => {
            setTimeout( resolve, 150 )
        } )
        pool.drain()

        // timeoutReconnect should have been canceled
        assert.equal( pool.timeoutReconnect, null )
        pool.options.silent = false
    } )

    it( 'The pool should remove a connection if it errored', async function() {
        const r = rebirthdb( {
            buffer: 1,
            max: 2,
            silent: true
        } )
        r.getPool().setOptions( {
            timeoutGb: 60 * 60 * 1000
        } )

        try {
            let result = await Promise.all( Array( options.max ).fill( r.expr( 1 ) ).map( ( expr ) => expr.run() ) )
            assert.deepEqual( result, Array( options.max ).fill( 1 ) )
        }
        catch ( e ) {
            assert.ifError( e ) // This should not error anymore because since the JSON protocol was introduced.

            assert.equal( e.message, 'Client is buggy (failed to deserialize protobuf)' )

            // We expect the connection that errored to get closed in the next second
            await new Promise( resolve => {
                setTimeout( function() {
                    assert.equal( r.getPool().getAvailableLength(), options.max - 1 )
                    assert.equal( r.getPool().getLength(), options.max - 1 )
                    resolve()
                }, 1000 )
            } )
        }
        finally {
            await r.getPoolMaster().drain()
        }
    } )
} )
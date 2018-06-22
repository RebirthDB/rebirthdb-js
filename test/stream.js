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
const {
    Readable
} = require( 'stream' )

describe( 'stream', () => {
    let r, dbName, tableName, tableName2, dumpTable
    const numDocs = 100 // Number of documents in the "big table" used to test the SUCCESS_PARTIAL
    const smallNumDocs = 5 // Number of documents in the "small table"

    before( async () => {
        r = await rebirthdb( config )
        dbName = uuid()
        tableName = uuid() // Big table to test partial sequence
        tableName2 = uuid() // small table to test success sequence
        dumpTable = uuid() // dump table

        let result = await r.dbCreate( dbName ).run()
        assert.equal( result.dbs_created, 1 )
        // await r.db(dbName).wait().run()
        result = await Promise.all( [
            r.db( dbName ).tableCreate( tableName )( 'tables_created' ).run(),
            r.db( dbName ).tableCreate( tableName2 )( 'tables_created' ).run(),
            r.db( dbName ).tableCreate( dumpTable )( 'tables_created' ).run()
        ] )
        assert.deepEqual( result, [ 1, 1, 1 ] )

        result = await r.db( dbName ).table( tableName ).insert( Array( numDocs ).fill( {} ) ).run()
        assert.equal( result.inserted, numDocs )

        result = await r.db( dbName ).table( tableName2 ).insert( Array( smallNumDocs ).fill( {} ) ).run()
        assert.equal( result.inserted, smallNumDocs )

        result = await r.db( dbName ).table( tableName ).update( {
            date: r.now()
        } ).run()
        assert.equal( result.replaced, numDocs )
    } )

    after( async () => {
        await r.getPool().drain()
    } )

    it( '`table` should return a stream', async function() {
        const stream = await r.db( dbName ).table( tableName ).run( {
            stream: true
        } )
        assert( stream )
        assert( stream instanceof Readable )
        stream.close()
    } )

    it( 'Arrays should return a stream', async function() {
        const data = [ 10, 11, 12, 13, 14, 15, 16 ]
        const stream = await r.expr( data ).run( {
            stream: true
        } )
        assert( stream )
        assert( stream instanceof Readable )

        await new Promise( ( resolve, reject ) => {
            let count = 0
            stream.on( 'data', function() {
                count++
                if ( count === data.length ) {
                    resolve()
                }
            } )
        } )
    } )

    it( 'changes() should return a stream', async function() {
        const data = [ {}, {}, {}, {} ]
        const stream = await r.db( dbName ).table( tableName ).changes().run( {
            stream: true
        } )
        assert( stream )
        assert( stream instanceof Readable )
        const promise = new Promise( ( resolve, reject ) => {
            let count = 0
            stream.on( 'data', function() {
                count++
                if ( count === data.length ) {
                    resolve()
                    stream.close()
                }
            } )
        } )
        await r.db( dbName ).table( tableName ).insert( data ).run()
        await promise
    } )

    it( 'get().changes() should return a stream', async function() {
        const id = uuid()
        await r.db( dbName ).table( tableName ).insert( {
            id
        } ).run()
        const stream = await r.db( dbName ).table( tableName ).get( id ).changes().run( {
            stream: true
        } )
        assert( stream )
        assert( stream instanceof Readable )

        const promise = new Promise( ( resolve, reject ) => {
            let count = 0
            stream.on( 'data', function() {
                count++
                if ( count === 3 ) {
                    resolve()
                    stream.close()
                }
            } )
        } )
        await r.db( dbName ).table( tableName ).get( id ).update( {
            update: 1
        } ).run()
        await r.db( dbName ).table( tableName ).get( id ).update( {
            update: 2
        } ).run()
        await r.db( dbName ).table( tableName ).get( id ).update( {
            update: 3
        } ).run()
        await promise
    } )

    it( '`table` should return a stream - testing empty SUCCESS_COMPLETE', async function() {
        const connection = await r.connect( {
            host: config.host,
            port: config.port,
            authKey: config.authKey
        } )
        assert( connection )

        const stream = await r.db( dbName ).table( tableName ).run( connection, {
            stream: true,
            maxBatchRows: 1
        } )
        assert( stream )
        assert( stream instanceof Readable )
        await stream.close()
        await connection.close()
    } )

    it( 'Test flowing - event data', async function() {
        const connection = await r.connect( {
            host: config.host,
            port: config.port,
            authKey: config.authKey
        } )
        assert( connection )

        const stream = await r.db( dbName ).table( tableName ).run( connection, {
            stream: true,
            maxBatchRows: 1
        } )
        await new Promise( ( resolve, reject ) => {
            let count = 0
            stream.on( 'data', function() {
                count++
                if ( count === numDocs ) {
                    resolve()
                }
            } )
        } )
        await stream.close()
        await connection.close()
    } )

    it( 'Test read', async function() {
        const connection = await r.connect( {
            host: config.host,
            port: config.port,
            authKey: config.authKey
        } )
        assert( connection )

        const stream = await r.db( dbName ).table( tableName ).run( connection, {
            stream: true,
            maxBatchRows: 1
        } )
        await new Promise( ( resolve, reject ) => {
            stream.once( 'readable', function() {
                const doc = stream.read()
                if ( doc === null ) {
                    reject( new Error( 'stream.read() should not return null when readable was emitted' ) )
                }
                let count = 1
                stream.on( 'data', function( data ) {
                    count++
                    if ( count === numDocs ) {
                        resolve()
                    }
                } )
            } )
        } )
        await stream.close()
        await connection.close()
    } )

    it( 'Test flowing - event data', async function() {
        const connection = await r.connect( {
            host: config.host,
            port: config.port,
            authKey: config.authKey
        } )
        assert( connection )

        const stream = await r.db( dbName ).table( tableName ).run( connection, {
            stream: true,
            maxBatchRows: 1
        } )
        await new Promise( ( resolve, reject ) => {
            let count = 0
            stream.on( 'data', function() {
                count++
                if ( count === numDocs ) {
                    resolve()
                }
            } )
            stream.pause()
            if ( count > 0 ) {
                reject( new Error( 'The stream should have been paused' ) )
            }
            stream.resume()
        } )
        await stream.close()
        await connection.close()
    } )

    it( 'Test read with null value', async function() {
        const connection = await r.connect( {
            host: config.host,
            port: config.port,
            authKey: config.authKey
        } )
        assert( connection )

        const stream = await r.db( dbName ).table( tableName ).limit( 10 ).union( [ null ] ).union( r.db( dbName ).table( tableName ).limit( 10 ) ).run( connection, {
            stream: true,
            maxBatchRows: 1
        } )
        await new Promise( ( resolve, reject ) => {
            stream.once( 'readable', function() {
                let count = 0
                stream.on( 'data', function( data ) {
                    count++
                    if ( count === 20 ) {
                        resolve()
                    }
                    else if ( count > 20 ) {
                        reject( new Error( 'Should not get null' ) )
                    }
                } )
            } )
        } )
        await stream.close()
        await connection.close()
    } )

    it( 'Test read', async function() {
        const connection = await r.connect( {
            host: config.host,
            port: config.port,
            authKey: config.authKey
        } )
        assert( connection )

        const stream = await r.db( dbName ).table( tableName ).run( connection, {
            stream: true,
            maxBatchRows: 1
        } )
        await new Promise( ( resolve, reject ) => {
            stream.once( 'readable', function() {
                stream.read() === null ?
                    reject( new Error( 'stream.read() should not return null when readable was emitted' ) ) :
                    resolve()
            } )
        } )
        await stream.close()
        await connection.close()
    } )

    it( 'Import with stream as default', async function() {
        const r1 = rebirthdb( {
            stream: true,
            host: config.host,
            port: config.port,
            authKey: config.authKey,
            buffer: config.buffer,
            max: config.max,
            discovery: false,
            silent: true
        } )
        const stream = await r1.db( dbName ).table( tableName ).run()
        assert( stream instanceof Readable )
        await stream.close()
        await r1.getPool().drain()
    } )

    it( 'toStream', async function() {
        const stream = r.db( dbName ).table( tableName ).toStream()

        await new Promise( ( resolve, reject ) => {
            stream.once( 'readable', function() {
                const doc = stream.read()
                if ( doc === null ) {
                    reject( new Error( 'stream.read() should not return null when readable was emitted' ) )
                }
                let count = 1
                stream.on( 'data', function( data ) {
                    count++
                    if ( count === numDocs ) {
                        resolve()
                    }
                } )
            } )
        } )
        await stream.close()
    } )

    it( 'toStream - with grouped data', async function() {
        const stream = r.db( dbName ).table( tableName ).group( {
            index: 'id'
        } ).toStream()

        await new Promise( ( resolve, reject ) => {
            stream.once( 'readable', function() {
                const doc = stream.read()
                if ( doc === null ) {
                    reject( new Error( 'stream.read() should not return null when readable was emitted' ) )
                }
                let count = 1
                stream.on( 'data', function( data ) {
                    count++
                    if ( count === numDocs ) {
                        resolve()
                    }
                } )
            } )
        } )
        await stream.close()
    } )

    it( 'pipe should work with a writable stream - 200-200', function( done ) {
        const r1 = rebirthdb( {
            buffer: 1,
            max: 2,
            discovery: false,
            silent: true
        } )

        r1.db( dbName ).table( tableName ).toStream( {
                highWaterMark: 200
            } )
            .pipe( r1.db( dbName ).table( dumpTable ).toStream( {
                writable: true,
                highWaterMark: 200
            } ) )
            .on( 'finish', function() {
                r.expr( [
                        r1.db( dbName ).table( tableName ).count(),
                        r1.db( dbName ).table( dumpTable ).count()
                    ] )
                    .run().then( function( result ) {
                        if ( result[ 0 ] !== result[ 1 ] ) {
                            done( new Error( 'All the data should have been streamed' ) )
                        }
                        return r1.db( dbName ).table( dumpTable ).delete()
                    } ).then( ( _ ) => r1.getPool().drain() ).then( done ).error( done )
            } )
    } )

    it( 'pipe should work with a writable stream - 200-20', function( done ) {
        const r1 = rebirthdb( {
            buffer: 1,
            max: 2,
            discovery: false,
            silent: true
        } )

        r1.db( dbName ).table( tableName ).toStream( {
                highWaterMark: 200
            } )
            .pipe( r1.db( dbName ).table( dumpTable ).toStream( {
                writable: true,
                highWaterMark: 20
            } ) )
            .on( 'finish', function() {
                r.expr( [
                    r1.db( dbName ).table( tableName ).count(),
                    r1.db( dbName ).table( dumpTable ).count()
                ] ).run().then( function( result ) {
                    if ( result[ 0 ] !== result[ 1 ] ) {
                        done( new Error( 'All the data should have been streamed' ) )
                    }
                    return r1.db( dbName ).table( dumpTable ).delete()
                } ).then( ( _ ) => r1.getPool().drain() ).then( done ).error( done )
            } )
    } )

    it( 'pipe should work with a writable stream - 20-200', function( done ) {
        const r1 = rebirthdb( {
            buffer: 1,
            max: 2,
            discovery: false,
            silent: true
        } )

        r1.db( dbName ).table( tableName ).toStream( {
                highWaterMark: 20
            } )
            .pipe( r1.db( dbName ).table( dumpTable ).toStream( {
                writable: true,
                highWaterMark: 200
            } ) )
            .on( 'finish', function() {
                r.expr( [
                    r1.db( dbName ).table( tableName ).count(),
                    r1.db( dbName ).table( dumpTable ).count()
                ] ).run().then( function( result ) {
                    if ( result[ 0 ] !== result[ 1 ] ) {
                        done( new Error( 'All the data should have been streamed' ) )
                    }
                    return r1.db( dbName ).table( dumpTable ).delete()
                } ).then( ( _ ) => r1.getPool().drain() ).then( done ).error( done )
            } )
    } )

    it( 'pipe should work with a writable stream - 50-50', function( done ) {
        const r1 = rebirthdb( {
            buffer: 1,
            max: 2,
            discovery: false,
            silent: true
        } )

        r1.db( dbName ).table( tableName ).toStream( {
                highWaterMark: 50
            } )
            .pipe( r1.db( dbName ).table( dumpTable ).toStream( {
                writable: true,
                highWaterMark: 50
            } ) )
            .on( 'finish', function() {
                r.expr( [
                    r1.db( dbName ).table( tableName ).count(),
                    r1.db( dbName ).table( dumpTable ).count()
                ] ).run().then( function( result ) {
                    if ( result[ 0 ] !== result[ 1 ] ) {
                        done( new Error( 'All the data should have been streamed' ) )
                    }
                    return r1.db( dbName ).table( dumpTable ).delete()
                } ).then( ( _ ) => r1.getPool( 0 ).drain() ).then( done ).error( done )
            } )
    } )

    it( 'toStream((writable: true}) should handle options', function( done ) {
        const r1 = rebirthdb( {
            buffer: 1,
            max: 2,
            discovery: false,
            silent: true
        } )

        const stream = r1.db( dbName ).table( dumpTable ).toStream( {
            writable: true,
            highWaterMark: 50,
            conflict: 'replace'
        } )
        stream.write( {
            id: 1,
            foo: 1
        } )
        stream.write( {
            id: 1,
            foo: 2
        } )
        stream.end( {
            id: 1,
            foo: 3
        } )

        stream.on( 'finish', function() {
            r1.db( dbName ).table( dumpTable ).count().then( function( result ) {
                assert.equal( result, 1 )
                return r1.db( dbName ).table( dumpTable ).get( 1 )
            } ).then( function( result ) {
                assert.deepEqual( result, {
                    id: 1,
                    foo: 3
                } )
                return r1.db( dbName ).table( dumpTable ).delete()
            } ).then( ( _ ) => r1.getPool( 0 ).drain() ).then( done ).error( done )
        } )
    } )

    it( 'test pipe all streams', function( done ) {
        // Create a transform stream that will convert data to a string
        const stream = require( 'stream' )
        const addfoobar = new stream.Transform()
        addfoobar._writableState.objectMode = true
        addfoobar._readableState.objectMode = true
        addfoobar._transform = function( data, encoding, done ) {
            data.transform = true
            this.push( data )
            done()
        }
        const addbuzzlol = new stream.Transform()
        addbuzzlol._writableState.objectMode = true
        addbuzzlol._readableState.objectMode = true
        addbuzzlol._transform = function( data, encoding, done ) {
            delete data.id
            data.written = true
            this.push( data )
            done()
        }
        r.db( dbName ).table( tableName ).without( 'id' ).toStream()
            .on( 'error', done )
            .pipe( addfoobar )
            .on( 'error', done )
            .pipe( r.db( dbName ).table( dumpTable ).toStream( {
                transform: true
            } ) )
            .on( 'error', done )
            .pipe( addbuzzlol )
            .on( 'error', done )
            .pipe( r.db( dbName ).table( dumpTable ).toStream( {
                writable: true
            } ) )
            .on( 'error', done )
            .on( 'finish', function() {
                r.db( dbName ).table( dumpTable ).filter( {
                    written: true
                } ).count().run().then( function( result ) {
                    assert( result, numDocs )
                    return r.db( dbName ).table( dumpTable ).filter( {
                        transform: true
                    } ).count().run()
                } ).then( function( result ) {
                    assert( result, numDocs * 2 )
                    return r.db( dbName ).table( dumpTable ).delete()
                } ).then( ( _ ) => r.getPool().drain() ).then( done ).error( done )
            } )
    } )

    it( 'toStream({writable: true}) should throw on something else than a table', async function() {
        const r1 = rebirthdb( {
            buffer: 1,
            max: 2,
            discovery: false,
            silent: true
        } )
        try {
            r.expr( dumpTable ).toStream( {
                writable: true
            } )
            assert.fail( 'should throw' )
        }
        catch ( err ) {
            assert( err.message.match( /^Cannot create a writable stream on something else than a table/ ) )
        }
        finally {
            r1.getPool().drain()
        }
    } )

    it( 'toStream({transform: true}) should throw on something else than a table', async function() {
        const r1 = rebirthdb( {
            buffer: 1,
            max: 2,
            discovery: false,
            silent: true
        } )
        try {
            r.expr( dumpTable ).toStream( {
                transform: true
            } )
            assert.fail( 'should throw' )
        }
        catch ( err ) {
            assert( err.message.match( /^Cannot create a writable stream on something else than a table/ ) )
        }
        finally {
            r1.getPool().drain()
        }
    } )
} )
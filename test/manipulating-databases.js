const path = require( 'path' )
const config = require( './config.js' )
const rebirthdb = require( path.join( __dirname, '/../lib' ) )
const { uuid } = require( path.join( __dirname, '/util/common.js' ) )
const assert = require( 'assert' )
const { before, after, describe, it } = require( 'mocha' )

describe( 'manipulating databases', () => {
    let r

    before( async function() {
        r = await rebirthdb( config )
    } )

    after( async function() {
        await r.getPoolMaster().drain()
    } )

    it( '`expr` should work', async function() {
        const result = await r.expr( 1 ).run()
        assert( result, 1 )
    } )

    it( '`dbList` should return a cursor', async function() {
        const result = await r.dbList().run()
        assert( Array.isArray( result ) )
    } )

    it( '`dbCreate` should create a database', async function() {
        const dbName = uuid() // export to the global scope

        const result = await r.dbCreate( dbName ).run()
        assert.equal( result.dbs_created, 1 )
    } )

    it( '`dbCreate` should throw if no argument is given', async function() {
        try {
            await r.dbCreate().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, '`dbCreate` takes 1 argument, 0 provided.' )
        }
    } )

    it( '`dbCreate` is not defined after a term', async function() {
        try {
            await r.expr( 1 ).dbCreate( 'foo' ).run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, '`dbCreate` is not defined after:\nr.expr(1)' )
        }
    } )

    it( '`dbCreate` is not defined after a term', async function() {
        try {
            await r.expr( 1 ).db( 'foo' ).run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, '`db` is not defined after:\nr.expr(1)' )
        }
    } )

    it( '`db` should throw is the name contains special char', async function() {
        try {
            await r.db( '-_-' ).run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e.message.match( /Database name `-_-` invalid \(Use A-Za-z0-9_ only\)/ ) )
        }
    } )

    it( '`dbList` should show the database we created', async function() {
        const dbName = uuid() // export to the global scope

        let result = await r.dbCreate( dbName ).run()
        assert.equal( result.dbs_created, 1 )

        result = await r.dbList().run()
        assert( Array.isArray( result ) )
        assert( result.find( ( name ) => name === dbName ) !== undefined )
    } )

    it( '`dbDrop` should drop a table', async function() {
        const dbName = uuid() // export to the global scope

        let result = await r.dbCreate( dbName ).run()
        assert.equal( result.dbs_created, 1 )

        result = await r.dbDrop( dbName ).run()
        assert.deepEqual( result.dbs_dropped, 1 )
    } )

    it( '`dbDrop` should throw if given too many arguments', async function() {
        try {
            await r.dbDrop( 'foo', 'bar', 'ette' ).run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, '`dbDrop` takes 1 argument, 3 provided.' )
        }
    } )

    it( '`dbDrop` should throw if no argument is given', async function() {
        try {
            await r.dbDrop().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, '`dbDrop` takes 1 argument, 0 provided.' )
        }
    } )

    it( '`dbDrop` is not defined after a term', async function() {
        try {
            await r.expr( 1 ).dbDrop( 'foo' ).run()
        }
        catch ( e ) {
            assert.equal( e.message, '`dbDrop` is not defined after:\nr.expr(1)' )
        }
    } )

    it( '`dbList` is not defined after a term', async function() {
        try {
            await r.expr( 1 ).dbList( 'foo' ).run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, '`dbList` is not defined after:\nr.expr(1)' )
        }
    } )

    it( '`dbList` should contain dropped databases', async function() {
        const dbName = uuid() // export to the global scope

        let result = await r.dbCreate( dbName ).run()
        assert.equal( result.dbs_created, 1 )

        result = await r.dbDrop( dbName ).run()
        assert.deepEqual( result.dbs_dropped, 1 )

        result = await r.dbList().run()
        assert( Array.isArray( result ) )
        assert( result.find( ( name ) => name === dbName ) === undefined )
    } )
} )
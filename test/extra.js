const path = require( 'path' )
const config = require( './config.js' )
const rebirthdb = require( path.join( __dirname, '/../lib' ) )
const assert = require( 'assert' )
const { uuid } = require( path.join( __dirname, '/util/common.js' ) )
const { after, before, describe, it } = require( 'mocha' )

describe( 'extra', () => {
    let r, dbName, tableName

    before( async () => {
        r = await rebirthdb( config )
        dbName = uuid()
        tableName = uuid() // Big table to test partial sequence

        let result = await r.dbCreate( dbName ).run()
        assert.equal( result.dbs_created, 1 )

        result = await r.db( dbName ).tableCreate( tableName )( 'tables_created' ).run()
        assert.deepEqual( result, 1 )
    } )

    after( async () => {
        await r.getPoolMaster().drain()
    } )

    it( 'Change the default database on the fly in run', async function() {
        const result = await r.tableList().run( {
            db: dbName
        } )
        assert.deepEqual( result, [ tableName ] )
    } )

    it( 'Anonymous function should throw if they return undefined', async function() {
        try {
            r.expr( 1 ).do( function() {} )
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, 'Anonymous function returned `undefined`. Did you forget a `return`? In:\nfunction () {}.' )
        }
    } )

    it( 'toString should work', function() {
        let result = r.expr( 1 ).add( 2 ).toString()
        assert.equal( result, 'r.expr(1).add(2)' )

        result = r.expr( 1 ).toString()
        assert.equal( result, 'r.expr(1)' )
    } )

    it( 'await a query should work - 1', async function() {
        let result = await r.expr( 1 )
        assert.equal( result, 1 )

        result = await r.expr( 1 ).add( 3 )
        assert.equal( result, 4 )
    } )

    it( 'await a query should work - 2', async function() {
        try {
            await r.expr( 1 ).add( 'foo' )
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e.message.match( /Expected type NUMBER but found STRING/ ) )
        }
    } )
} )
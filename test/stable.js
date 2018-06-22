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

describe( 'stable', () => {
    let r, dbName, tableName

    before( async () => {
        r = await rebirthdb( config )
        dbName = uuid()
        tableName = uuid()
    } )

    after( async () => {
        await r.getPoolMaster().drain()
    } )

    // Tests for callbacks
    it( 'Create db', async function() {
        const result = await r.dbCreate( dbName ).run()
        assert.equal( result.dbs_created, 1 )
    } )

    it( 'Create table', async function() {
        const result = await r.db( dbName ).tableCreate( tableName ).run()
        assert.equal( result.tables_created, 1 )
    } )

    it( 'Insert', async function() {
        const result = await r.db( dbName ).table( tableName ).insert( [ {
            name: 'Michel',
            age: 27
        }, {
            name: 'Sophie',
            age: 23
        } ] ).run()
        assert.deepEqual( result.inserted, 2 )
    } )

    it( 'Table', async function() {
        const result = await r.db( dbName ).table( tableName ).run()
        assert( result.length, 2 )
    } )

    it( 'get', async function() {
        const docs = await r.db( dbName ).table( tableName ).run()
        const result = await r.db( dbName ).table( tableName ).get( docs[ 0 ].id ).run()
        assert.deepEqual( result, docs[ 0 ] )
    } )

    it( 'datum', async function() {
        const result = await r.expr( {
            foo: 'bar'
        } ).run()
        assert.deepEqual( result, {
            foo: 'bar'
        } )
    } )

    it( 'date', async function() {
        const result = await r.now().run()
        assert( result instanceof Date )
    } )
} )
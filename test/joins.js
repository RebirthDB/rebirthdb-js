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

describe( 'joins', () => {
    let r, dbName, tableName, pks

    before( async () => {
        r = await rebirthdb( config )
        dbName = uuid()
        tableName = uuid()

        let result = await r.dbCreate( dbName ).run()
        assert.equal( result.dbs_created, 1 )
        result = await r.db( dbName ).tableCreate( tableName ).run()
        assert.equal( result.tables_created, 1 )
        result = await r.db( dbName ).table( tableName ).insert( [ {
            val: 1
        }, {
            val: 2
        }, {
            val: 3
        } ] ).run()
        pks = result.generated_keys
        assert.equal( result.inserted, 3 )

        await r.db( dbName ).table( tableName ).indexCreate( 'val' ).run()
        result = await r.db( dbName ).table( tableName ).indexWait( 'val' ).run()
        assert( result )
    } )

    after( async () => {
        await r.getPoolMaster().drain()
    } )

    it( '`innerJoin` should return -- array-array', async function() {
        const result = await r.expr( [ 1, 2, 3 ] ).innerJoin( r.expr( [ 1, 2, 3 ] ), function( left, right ) {
            return left.eq( right )
        } ).run()
        assert.deepEqual( result, [ {
            left: 1,
            right: 1
        }, {
            left: 2,
            right: 2
        }, {
            left: 3,
            right: 3
        } ] )
    } )

    it( '`innerJoin` should return -- array-stream', async function() {
        const result = await r.expr( [ 1, 2, 3 ] ).innerJoin( r.db( dbName ).table( tableName ), function( left, right ) {
            return left.eq( right( 'val' ) )
        } ).run()
        assert.equal( result.length, 3 )
        assert( result[ 0 ].left )
        assert( result[ 0 ].right )
        assert( result[ 1 ].left )
        assert( result[ 1 ].right )
        assert( result[ 2 ].left )
        assert( result[ 2 ].right )
    } )

    it( '`innerJoin` should return -- stream-stream', async function() {
        const result = await r.db( dbName ).table( tableName ).innerJoin( r.db( dbName ).table( tableName ), function( left, right ) {
            return left.eq( right )
        } ).run()
        assert.equal( result.length, 3 )
        assert( result[ 0 ].left )
        assert( result[ 0 ].right )
        assert( result[ 1 ].left )
        assert( result[ 1 ].right )
        assert( result[ 2 ].left )
        assert( result[ 2 ].right )
    } )

    it( '`innerJoin` should throw if no sequence', async function() {
        try {
            await r.db( dbName ).table( tableName ).innerJoin().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, '`innerJoin` takes 2 arguments, 0 provided after:\nr.db("' + dbName + '").table("' + tableName + '")' )
        }
    } )

    it( '`innerJoin` should throw if no predicate', async function() {
        try {
            await r.db( dbName ).table( tableName ).innerJoin( r.expr( [ 1, 2, 3 ] ) ).run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, '`innerJoin` takes 2 arguments, 1 provided after:\nr.db("' + dbName + '").table("' + tableName + '")' )
        }
    } )

    it( '`outerJoin` should return -- array-array', async function() {
        const result = await r.expr( [ 1, 2, 3 ] ).outerJoin( r.expr( [ 1, 2, 3 ] ), function( left, right ) {
            return left.eq( right )
        } ).run()
        assert.deepEqual( result, [ {
            left: 1,
            right: 1
        }, {
            left: 2,
            right: 2
        }, {
            left: 3,
            right: 3
        } ] )
    } )

    it( '`outerJoin` should return -- array-stream - 1', async function() {
        const result = await r.expr( [ 1, 2, 3, 4 ] ).outerJoin( r.db( dbName ).table( tableName ), function( left, right ) {
            return left.eq( right( 'val' ) )
        } ).run()
        assert.equal( result.length, 4 )
        assert( result[ 0 ].left )
        assert( result[ 0 ].right )
        assert( result[ 1 ].left )
        assert( result[ 1 ].right )
        assert( result[ 2 ].left )
        assert( result[ 2 ].right )
    } )

    it( '`outerJoin` should return -- array-stream - 2', async function() {
        const result = await r.expr( [ 1, 2, 3, 4 ] ).outerJoin( r.db( dbName ).table( tableName ), function( left, right ) {
            return left.eq( right( 'val' ) )
        } ).run()
        assert.equal( result.length, 4 )
        assert( result[ 0 ].left )
        assert( result[ 0 ].right )
        assert( result[ 1 ].left )
        assert( result[ 1 ].right )
        assert( result[ 2 ].left )
        assert( result[ 2 ].right )
        assert( result[ 3 ].left )
        assert.equal( result[ 3 ].right, undefined )
    } )

    it( '`outerJoin` should return -- stream-stream', async function() {
        const result = await r.db( dbName ).table( tableName ).outerJoin( r.db( dbName ).table( tableName ), function( left, right ) {
            return left.eq( right )
        } ).run()
        assert.equal( result.length, 3 )
        assert( result[ 0 ].left )
        assert( result[ 0 ].right )
        assert( result[ 1 ].left )
        assert( result[ 1 ].right )
        assert( result[ 2 ].left )
        assert( result[ 2 ].right )
    } )

    it( '`outerJoin` should throw if no sequence', async function() {
        try {
            await r.db( dbName ).table( tableName ).outerJoin().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, '`outerJoin` takes 2 arguments, 0 provided after:\nr.db("' + dbName + '").table("' + tableName + '")' )
        }
    } )

    it( '`outerJoin` should throw if no predicate', async function() {
        try {
            await r.db( dbName ).table( tableName ).outerJoin( r.expr( [ 1, 2, 3 ] ) ).run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, '`outerJoin` takes 2 arguments, 1 provided after:\nr.db("' + dbName + '").table("' + tableName + '")' )
        }
    } )

    it( '`eqJoin` should return -- pk -- array-stream - function', async function() {
        const result = await r.expr( pks ).eqJoin( function( doc ) {
            return doc
        }, r.db( dbName ).table( tableName ) ).run()
        assert.equal( result.length, 3 )
        assert( result[ 0 ].left )
        assert( result[ 0 ].right )
        assert( result[ 1 ].left )
        assert( result[ 1 ].right )
        assert( result[ 2 ].left )
        assert( result[ 2 ].right )
    } )

    it( '`eqJoin` should return -- pk -- array-stream - r.row', async function() {
        const result = await r.expr( pks ).eqJoin( r.row, r.db( dbName ).table( tableName ) ).run()
        assert.equal( result.length, 3 )
        assert( result[ 0 ].left )
        assert( result[ 0 ].right )
        assert( result[ 1 ].left )
        assert( result[ 1 ].right )
        assert( result[ 2 ].left )
        assert( result[ 2 ].right )
    } )

    it( '`eqJoin` should return -- secondary index -- array-stream - r.row', async function() {
        const result = await r.expr( [ 1, 2, 3 ] ).eqJoin( r.row, r.db( dbName ).table( tableName ), {
            index: 'val'
        } ).run()
        assert.equal( result.length, 3 )
        assert( result[ 0 ].left )
        assert( result[ 0 ].right )
        assert( result[ 1 ].left )
        assert( result[ 1 ].right )
        assert( result[ 2 ].left )
        assert( result[ 2 ].right )
    } )

    it( '`eqJoin` should throw if no argument', async function() {
        try {
            await r.db( dbName ).table( tableName ).eqJoin().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, '`eqJoin` takes at least 2 arguments, 0 provided after:\nr.db("' + dbName + '").table("' + tableName + '")' )
        }
    } )

    it( '`eqJoin` should throw with a non valid key', async function() {
        try {
            await r.expr( [ 1, 2, 3 ] ).eqJoin( r.row, r.db( dbName ).table( tableName ), {
                nonValidKey: 'val'
            } ).run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, 'Unrecognized option `nonValidKey` in `eqJoin` after:\nr.expr([1, 2, 3])\nAvailable options are index <string>, ordered <boolean>' )
        }
    } )

    it( '`eqJoin` should throw if no sequence', async function() {
        try {
            await r.db( dbName ).table( tableName ).eqJoin( 'id' ).run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, '`eqJoin` takes at least 2 arguments, 1 provided after:\nr.db("' + dbName + '").table("' + tableName + '")' )
        }
    } )

    it( '`eqJoin` should throw if too many arguments', async function() {
        try {
            await r.db( dbName ).table( tableName ).eqJoin( 1, 1, 1, 1, 1 ).run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, '`eqJoin` takes at most 3 arguments, 5 provided after:\nr.db("' + dbName + '").table("' + tableName + '")' )
        }
    } )

    it( '`zip` should zip stuff', async function() {
        const result = await r.expr( pks ).eqJoin( function( doc ) {
            return doc
        }, r.db( dbName ).table( tableName ) ).zip().run()
        assert.equal( result.length, 3 )
        assert.equal( result[ 0 ].left, undefined )
    } )
} )
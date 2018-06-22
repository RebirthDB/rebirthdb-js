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

describe( 'dates and times', () => {
    let r, dbName, tableName

    before( async () => {
        r = await rebirthdb( config )
        dbName = uuid()
        tableName = uuid()

        let result = await r.dbCreate( dbName ).run()
        assert.equal( result.dbs_created, 1 )

        result = await r.db( dbName ).tableCreate( tableName ).run()
        assert.equal( result.tables_created, 1 )
    } )

    after( async () => {
        await r.getPoolMaster().drain()
    } )

    it( '`r.row` should work - 1', async function() {
        const result = await r.expr( [ 1, 2, 3 ] ).map( r.row ).run()
        assert.deepEqual( result, [ 1, 2, 3 ] )
    } )

    it( '`r.row` should work - 2', async function() {
        let result = await r.db( dbName ).table( tableName ).insert( {} ).run()
        assert.equal( result.inserted, 1 )

        result = await r.db( dbName ).table( tableName ).update( {
            idCopyUpdate: r.row( 'id' )
        } ).run()
        assert.equal( result.replaced, 1 )
    } )

    it( '`r.row` should work - 3', async function() {
        const result = await r.db( dbName ).table( tableName ).replace( r.row ).run()
        assert.equal( result.replaced, 0 )
    } )

    it( '`r.row` should work - 4', async function() {
        const result = await r.db( dbName ).table( tableName ).replace( function( doc ) {
            return doc.merge( {
                idCopyReplace: doc( 'id' )
            } )
        } ).run()
        assert.equal( result.replaced, 1 )
    } )

    it( '`r.row` should work - 5', async function() {
        const result = await r.db( dbName ).table( tableName ).delete().run()
        assert.equal( result.deleted, 1 )
    } )

    it( '`pluck` should work', async function() {
        let result = await r.expr( {
            a: 0,
            b: 1,
            c: 2
        } ).pluck( 'a', 'b' ).run()
        assert.deepEqual( result, {
            a: 0,
            b: 1
        } )

        result = await r.expr( [ {
            a: 0,
            b: 1,
            c: 2
        }, {
            a: 0,
            b: 10,
            c: 20
        } ] ).pluck( 'a', 'b' ).run()
        assert.deepEqual( result, [ {
            a: 0,
            b: 1
        }, {
            a: 0,
            b: 10
        } ] )
    } )

    it( '`pluck` should throw if no argument has been passed', async function() {
        try {
            await r.db( dbName ).table( tableName ).pluck().run()
            assert.fail( 'should trow' )
        }
        catch ( e ) {
            assert.equal( e.message, '`pluck` takes at least 1 argument, 0 provided after:\nr.db("' + dbName + '").table("' + tableName + '")' )
        }
    } )

    it( '`without` should work', async function() {
        let result = await r.expr( {
            a: 0,
            b: 1,
            c: 2
        } ).without( 'c' ).run()
        assert.deepEqual( result, {
            a: 0,
            b: 1
        } )

        result = await r.expr( [ {
            a: 0,
            b: 1,
            c: 2
        }, {
            a: 0,
            b: 10,
            c: 20
        } ] ).without( 'a', 'c' ).run()
        assert.deepEqual( result, [ {
            b: 1
        }, {
            b: 10
        } ] )
    } )

    it( '`without` should throw if no argument has been passed', async function() {
        try {
            await r.db( dbName ).table( tableName ).without().run()
        }
        catch ( e ) {
            assert.equal( e.message, '`without` takes at least 1 argument, 0 provided after:\nr.db("' + dbName + '").table("' + tableName + '")' )
        }
    } )

    it( '`merge` should work', async function() {
        let result = await r.expr( {
            a: 0
        } ).merge( {
            b: 1
        } ).run()
        assert.deepEqual( result, {
            a: 0,
            b: 1
        } )

        result = await r.expr( [ {
            a: 0
        }, {
            a: 1
        }, {
            a: 2
        } ] ).merge( {
            b: 1
        } ).run()
        assert.deepEqual( result, [ {
            a: 0,
            b: 1
        }, {
            a: 1,
            b: 1
        }, {
            a: 2,
            b: 1
        } ] )

        result = await r.expr( {
            a: 0,
            c: {
                l: 'tt'
            }
        } ).merge( {
            b: {
                c: {
                    d: {
                        e: 'fff'
                    }
                },
                k: 'pp'
            }
        } ).run()
        assert.deepEqual( result, {
            a: 0,
            b: {
                c: {
                    d: {
                        e: 'fff'
                    }
                },
                k: 'pp'
            },
            c: {
                l: 'tt'
            }
        } )

        result = await r.expr( {
            a: 1
        } ).merge( {
            date: r.now()
        } ).run()
        assert.equal( result.a, 1 )
        assert( result.date instanceof Date )

        result = await r.expr( {
            a: 1
        } ).merge( {
            nested: r.row
        }, {
            b: 2
        } ).run()
        assert.deepEqual( result, {
            a: 1,
            nested: {
                a: 1
            },
            b: 2
        } )
    } )

    it( '`merge` should take an anonymous function', async function() {
        let result = await r.expr( {
            a: 0
        } ).merge( function( doc ) {
            return {
                b: doc( 'a' ).add( 1 )
            }
        } ).run()
        assert.deepEqual( result, {
            a: 0,
            b: 1
        } )

        result = await r.expr( {
            a: 0
        } ).merge( {
            b: r.row( 'a' ).add( 1 )
        } ).run()
        assert.deepEqual( result, {
            a: 0,
            b: 1
        } )
    } )

    it( '`literal` should work', async function() {
        const result = await r.expr( {
            a: {
                b: 1
            }
        } ).merge( {
            a: r.literal( {
                c: 2
            } )
        } ).run()
        assert.deepEqual( result, {
            a: {
                c: 2
            }
        } )
    } )

    it( '`literal` is not defined after a term', async function() {
        try {
            await r.expr( 1 ).literal( 'foo' ).run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, '`literal` is not defined after:\nr.expr(1)' )
        }
    } )

    it( '`merge` should throw if no argument has been passed', async function() {
        try {
            await r.db( dbName ).table( tableName ).merge().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, '`merge` takes at least 1 argument, 0 provided after:\nr.db("' + dbName + '").table("' + tableName + '")' )
        }
    } )

    it( '`literal` should work with no argument', async function() {
        const result = await r.expr( {
            foo: 'bar'
        } ).merge( {
            foo: r.literal()
        } ).run()
        assert.deepEqual( result, {} )
    } )

    it( '`append` should work', async function() {
        const result = await r.expr( [ 1, 2, 3 ] ).append( 4 ).run()
        assert.deepEqual( result, [ 1, 2, 3, 4 ] )
    } )

    it( '`append` should throw if no argument has been passed', async function() {
        try {
            await r.db( dbName ).table( tableName ).append().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, '`append` takes 1 argument, 0 provided after:\nr.db("' + dbName + '").table("' + tableName + '")' )
        }
    } )

    it( '`prepend` should work', async function() {
        const result = await r.expr( [ 1, 2, 3 ] ).prepend( 4 ).run()
        assert.deepEqual( result, [ 4, 1, 2, 3 ] )
    } )

    it( '`prepend` should throw if no argument has been passed', async function() {
        try {
            await r.db( dbName ).table( tableName ).prepend().run()
            assert.fail( 'sholud throw' )
        }
        catch ( e ) {
            assert.equal( e.message, '`prepend` takes 1 argument, 0 provided after:\nr.db("' + dbName + '").table("' + tableName + '")' )
        }
    } )

    it( '`difference` should work', async function() {
        const result = await r.expr( [ 1, 2, 3 ] ).prepend( 4 ).run()
        assert.deepEqual( result, [ 4, 1, 2, 3 ] )
    } )

    it( '`difference` should throw if no argument has been passed', async function() {
        try {
            await r.db( dbName ).table( tableName ).difference().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, '`difference` takes 1 argument, 0 provided after:\nr.db("' + dbName + '").table("' + tableName + '")' )
        }
    } )

    it( '`setInsert` should work', async function() {
        let result = await r.expr( [ 1, 2, 3 ] ).setInsert( 4 ).run()
        assert.deepEqual( result, [ 1, 2, 3, 4 ] )

        result = await r.expr( [ 1, 2, 3 ] ).setInsert( 2 ).run()
        assert.deepEqual( result, [ 1, 2, 3 ] )
    } )

    it( '`setInsert` should throw if no argument has been passed', async function() {
        try {
            await r.db( dbName ).table( tableName ).setInsert().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, '`setInsert` takes 1 argument, 0 provided after:\nr.db("' + dbName + '").table("' + tableName + '")' )
        }
    } )

    it( '`setUnion` should work', async function() {
        const result = await r.expr( [ 1, 2, 3 ] ).setUnion( [ 2, 4 ] ).run()
        assert.deepEqual( result, [ 1, 2, 3, 4 ] )
    } )

    it( '`setUnion` should throw if no argument has been passed', async function() {
        try {
            await r.db( dbName ).table( tableName ).setUnion().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, '`setUnion` takes 1 argument, 0 provided after:\nr.db("' + dbName + '").table("' + tableName + '")' )
        }
    } )

    it( '`setIntersection` should work', async function() {
        const result = await r.expr( [ 1, 2, 3 ] ).setIntersection( [ 2, 4 ] ).run()
        assert.deepEqual( result, [ 2 ] )
    } )

    it( '`setIntersection` should throw if no argument has been passed', async function() {
        try {
            await r.db( dbName ).table( tableName ).setIntersection().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, '`setIntersection` takes 1 argument, 0 provided after:\nr.db("' + dbName + '").table("' + tableName + '")' )
        }
    } )

    it( '`setDifference` should work', async function() {
        const result = await r.expr( [ 1, 2, 3 ] ).setDifference( [ 2, 4 ] ).run()
        assert.deepEqual( result, [ 1, 3 ] )
    } )

    it( '`setDifference` should throw if no argument has been passed', async function() {
        try {
            await r.db( dbName ).table( tableName ).setDifference().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, '`setDifference` takes 1 argument, 0 provided after:\nr.db("' + dbName + '").table("' + tableName + '")' )
        }
    } )

    it( '`getField` should work', async function() {
        let result = await r.expr( {
            a: 0,
            b: 1
        } )( 'a' ).run()
        assert.equal( result, 0 )

        result = await r.expr( {
            a: 0,
            b: 1
        } ).getField( 'a' ).run()
        assert.equal( result, 0 )

        result = await r.expr( [ {
            a: 0,
            b: 1
        }, {
            a: 1
        } ] )( 'a' ).run()
        assert.deepEqual( result, [ 0, 1 ] )
    } )

    it( '`(...)` should throw if no argument has been passed', async function() {
        try {
            await r.db( dbName ).table( tableName )().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, '`(...)` takes 1 argument, 0 provided.' )
        }
    } )

    it( '`getField` should throw if no argument has been passed', async function() {
        try {
            await r.db( dbName ).table( tableName ).getField().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, '`(...)` takes 1 argument, 0 provided after:\nr.db("' + dbName + '").table("' + tableName + '")' )
        }
    } )

    it( '`hasFields` should work', async function() {
        const result = await r.expr( [ {
            a: 0,
            b: 1,
            c: 2
        }, {
            a: 0,
            b: 10,
            c: 20
        }, {
            b: 1,
            c: 3
        } ] ).hasFields( 'a', 'c' ).run()
        assert.deepEqual( result, [ {
            a: 0,
            b: 1,
            c: 2
        }, {
            a: 0,
            b: 10,
            c: 20
        } ] )
    } )

    it( '`hasFields` should throw if no argument has been passed', async function() {
        try {
            await r.db( dbName ).table( tableName ).hasFields().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, '`hasFields` takes at least 1 argument, 0 provided after:\nr.db("' + dbName + '").table("' + tableName + '")' )
        }
    } )

    it( '`insertAt` should work', async function() {
        let result = await r.expr( [ 1, 2, 3, 4 ] ).insertAt( 0, 2 ).run()
        assert.deepEqual( result, [ 2, 1, 2, 3, 4 ] )

        result = await r.expr( [ 1, 2, 3, 4 ] ).insertAt( 3, 2 ).run()
        assert.deepEqual( result, [ 1, 2, 3, 2, 4 ] )
    } )

    it( '`insertAt` should throw if no argument has been passed', async function() {
        try {
            await r.db( dbName ).table( tableName ).insertAt().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, '`insertAt` takes 2 arguments, 0 provided after:\nr.db("' + dbName + '").table("' + tableName + '")' )
        }
    } )

    it( '`spliceAt` should work', async function() {
        const result = await r.expr( [ 1, 2, 3, 4 ] ).spliceAt( 1, [ 9, 9 ] ).run()
        assert.deepEqual( result, [ 1, 9, 9, 2, 3, 4 ] )
    } )

    it( '`spliceAt` should throw if no argument has been passed', async function() {
        try {
            await r.db( dbName ).table( tableName ).spliceAt().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, '`spliceAt` takes at least 1 argument, 0 provided after:\nr.db("' + dbName + '").table("' + tableName + '")' )
        }
    } )

    it( '`deleteAt` should work', async function() {
        let result = await r.expr( [ 1, 2, 3, 4 ] ).deleteAt( 1 ).run()
        assert.deepEqual( result, [ 1, 3, 4 ] )

        result = await r.expr( [ 1, 2, 3, 4 ] ).deleteAt( 1, 3 ).run()
        assert.deepEqual( result, [ 1, 4 ] )
    } )

    it( '`deleteAt` should throw if no argument has been passed', async function() {
        try {
            await r.db( dbName ).table( tableName ).deleteAt().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, '`deleteAt` takes at least 1 argument, 0 provided after:\nr.db("' + dbName + '").table("' + tableName + '")' )
        }
    } )

    it( '`deleteAt` should throw if too many arguments', async function() {
        try {
            await r.db( dbName ).table( tableName ).deleteAt( 1, 1, 1, 1 ).run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, '`deleteAt` takes at most 2 arguments, 4 provided after:\nr.db("' + dbName + '").table("' + tableName + '")' )
        }
    } )

    it( '`changeAt` should work', async function() {
        const result = await r.expr( [ 1, 2, 3, 4 ] ).changeAt( 1, 3 ).run()
        assert.deepEqual( result, [ 1, 3, 3, 4 ] )
    } )

    it( '`changeAt` should throw if no argument has been passed', async function() {
        try {
            await r.db( dbName ).table( tableName ).changeAt().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, '`changeAt` takes at least 1 argument, 0 provided after:\nr.db("' + dbName + '").table("' + tableName + '")' )
        }
    } )

    it( '`keys` should work', async function() {
        const result = await r.expr( {
            a: 0,
            b: 1,
            c: 2
        } ).keys().orderBy( r.row ).run()
        assert.deepEqual( result, [ 'a', 'b', 'c' ] )
    } )

    it( '`keys` throw on a string', async function() {
        try {
            await r.expr( 'hello' ).keys().orderBy( r.row ).run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e.message.match( /^Cannot call `keys` on objects of type `STRING` in/ ) )
        }
    } )

    it( '`values` should work', async function() {
        const result = await r.expr( {
            a: 0,
            b: 1,
            c: 2
        } ).values().orderBy( r.row ).run()
        assert.deepEqual( result, [ 0, 1, 2 ] )
    } )

    it( '`object` should work', async function() {
        const result = await r.object( 'a', 1, r.expr( '2' ), 'foo' ).run()
        assert.deepEqual( result, {
            'a': 1,
            '2': 'foo'
        } )
    } )
} )
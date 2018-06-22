const path = require( 'path' )
const config = require( './config.js' )
const rebirthdb = require( path.join( __dirname, '/../lib' ) )
const assert = require( 'assert' )
const { uuid } = require( path.join( __dirname, '/util/common.js' ) )
const { before, after, describe, it } = require( 'mocha' )

describe( 'transformations', () => {
    let r, dbName, tableName

    before( async () => {
        r = await rebirthdb( config )
        dbName = uuid()
        tableName = uuid()
        const numDocs = 100

        let result = await r.dbCreate( dbName ).run()
        assert.equal( result.dbs_created, 1 )

        result = await r.db( dbName ).tableCreate( tableName ).run()
        assert.equal( result.tables_created, 1 )

        result = await r.db( dbName ).table( tableName ).insert( Array( numDocs ).fill( {} ) ).run()
        assert.equal( result.inserted, numDocs )

        result = await r.db( dbName ).table( tableName ).update( {
            val: r.js( 'Math.random()' )
        }, {
            nonAtomic: true
        } ).run()
        result = await r.db( dbName ).table( tableName ).indexCreate( 'val' ).run()
        result = await r.db( dbName ).table( tableName ).indexWait( 'val' ).run()
    } )

    after( async () => {
        await r.getPool().drain()
    } )

    it( '`map` should work on array -- r.row', async function() {
        let result = await r.expr( [ 1, 2, 3 ] ).map( r.row ).run()
        assert.deepEqual( result, [ 1, 2, 3 ] )

        result = await r.expr( [ 1, 2, 3 ] ).map( r.row.add( 1 ) ).run()
        assert.deepEqual( result, [ 2, 3, 4 ] )
    } )

    it( '`map` should work on array -- function', async function() {
        let result = await r.expr( [ 1, 2, 3 ] ).map( function( doc ) {
            return doc
        } ).run()
        assert.deepEqual( result, [ 1, 2, 3 ] )

        result = await r.expr( [ 1, 2, 3 ] ).map( function( doc ) {
            return doc.add( 2 )
        } ).run()
        assert.deepEqual( result, [ 3, 4, 5 ] )
    } )

    it( '`map` should throw if no argument has been passed', async function() {
        try {
            await r.db( dbName ).table( tableName ).map().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e.message.match( /^`map` takes at least 1 argument, 0 provided after/ ) )
        }
    } )

    it( '`withFields` should work on array -- single field', async function() {
        const result = await r.expr( [ {
            a: 0,
            b: 1,
            c: 2
        }, {
            a: 4,
            b: 4,
            c: 5
        }, {
            a: 9,
            b: 2,
            c: 0
        } ] ).withFields( 'a' ).run()
        assert.deepEqual( result, [ {
            a: 0
        }, {
            a: 4
        }, {
            a: 9
        } ] )
    } )

    it( '`withFields` should work on array -- multiple field', async function() {
        const result = await r.expr( [ {
            a: 0,
            b: 1,
            c: 2
        }, {
            a: 4,
            b: 4,
            c: 5
        }, {
            a: 9,
            b: 2,
            c: 0
        } ] ).withFields( 'a', 'c' ).run()
        assert.deepEqual( result, [ {
            a: 0,
            c: 2
        }, {
            a: 4,
            c: 5
        }, {
            a: 9,
            c: 0
        } ] )
    } )

    it( '`withFields` should throw if no argument has been passed', async function() {
        try {
            await r.db( dbName ).table( tableName ).withFields().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e.message.match( /^`withFields` takes at least 1 argument, 0 provided after/ ) )
        }
    } )

    it( '`concatMap` should work on array -- function', async function() {
        const result = await r.expr( [
            [ 1, 2 ],
            [ 3 ],
            [ 4 ]
        ] ).concatMap( function( doc ) {
            return doc
        } ).run()
        assert.deepEqual( result, [ 1, 2, 3, 4 ] )
    } )

    it( '`concatMap` should work on array -- r.row', async function() {
        const result = await r.expr( [
            [ 1, 2 ],
            [ 3 ],
            [ 4 ]
        ] ).concatMap( r.row ).run()
        assert.deepEqual( result, [ 1, 2, 3, 4 ] )
    } )

    it( '`concatMap` should throw if no argument has been passed', async function() {
        try {
            await r.db( dbName ).table( tableName ).concatMap().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e.message.match( /^`concatMap` takes 1 argument, 0 provided after/ ) )
        }
    } )

    it( '`orderBy` should work on array -- string', async function() {
        const result = await r.expr( [ {
            a: 23
        }, {
            a: 10
        }, {
            a: 0
        }, {
            a: 100
        } ] ).orderBy( 'a' ).run()
        assert.deepEqual( result, [ {
            a: 0
        }, {
            a: 10
        }, {
            a: 23
        }, {
            a: 100
        } ] )
    } )

    it( '`orderBy` should work on array -- r.row', async function() {
        const result = await r.expr( [ {
            a: 23
        }, {
            a: 10
        }, {
            a: 0
        }, {
            a: 100
        } ] ).orderBy( r.row( 'a' ) ).run()
        assert.deepEqual( result, [ {
            a: 0
        }, {
            a: 10
        }, {
            a: 23
        }, {
            a: 100
        } ] )
    } )

    it( '`orderBy` should work on a table -- pk', async function() {
        const result = await r.db( dbName ).table( tableName ).orderBy( {
            index: 'id'
        } ).run()
        for ( var i = 0; i < result.length - 1; i++ ) {
            assert( result[ i ].id < result[ i + 1 ].id )
        }
    } )

    it( '`orderBy` should work on a table -- secondary', async function() {
        const result = await r.db( dbName ).table( tableName ).orderBy( {
            index: 'val'
        } ).run()
        for ( var i = 0; i < result.length - 1; i++ ) {
            assert( result[ i ].val < result[ i + 1 ].val )
        }
    } )

    it( '`orderBy` should work on a two fields', async function() {
        const dbName = uuid()
        const tableName = uuid()
        const numDocs = 98

        let result = await r.dbCreate( dbName ).run()
        assert.deepEqual( result.dbs_created, 1 )

        result = await r.db( dbName ).tableCreate( tableName ).run()
        assert.equal( result.tables_created, 1 )

        result = await r.db( dbName ).table( tableName ).insert( Array( numDocs ).fill().map( () => ( {
            a: r.js( 'Math.random()' )
        } ) ) ).run()
        assert.deepEqual( result.inserted, numDocs )

        result = await r.db( dbName ).table( tableName ).orderBy( 'id', 'a' ).run()
        assert( Array.isArray( result ) )
        assert( result[ 0 ].id < result[ 1 ].id )
    } )

    it( '`orderBy` should throw if no argument has been passed', async function() {
        try {
            await r.db( dbName ).table( tableName ).orderBy().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e.message.match( /^`orderBy` takes at least 1 argument, 0 provided after/ ) )
        }
    } )

    it( '`orderBy` should not wrap on r.asc', async function() {
        const result = await r.expr( [ {
            a: 23
        }, {
            a: 10
        }, {
            a: 0
        }, {
            a: 100
        } ] ).orderBy( r.asc( r.row( 'a' ) ) ).run()
        assert.deepEqual( result, [ {
            a: 0
        }, {
            a: 10
        }, {
            a: 23
        }, {
            a: 100
        } ] )
    } )

    it( '`orderBy` should not wrap on r.desc', async function() {
        const result = await r.expr( [ {
            a: 23
        }, {
            a: 10
        }, {
            a: 0
        }, {
            a: 100
        } ] ).orderBy( r.desc( r.row( 'a' ) ) ).run()
        assert.deepEqual( result, [ {
            a: 100
        }, {
            a: 23
        }, {
            a: 10
        }, {
            a: 0
        } ] )
    } )
    it( 'r.desc should work', async function() {
        const result = await r.expr( [ {
            a: 23
        }, {
            a: 10
        }, {
            a: 0
        }, {
            a: 100
        } ] ).orderBy( r.desc( 'a' ) ).run()
        assert.deepEqual( result, [ {
            a: 100
        }, {
            a: 23
        }, {
            a: 10
        }, {
            a: 0
        } ] )
    } )

    it( 'r.asc should work', async function() {
        const result = await r.expr( [ {
            a: 23
        }, {
            a: 10
        }, {
            a: 0
        }, {
            a: 100
        } ] ).orderBy( r.asc( 'a' ) ).run()
        assert.deepEqual( result, [ {
            a: 0
        }, {
            a: 10
        }, {
            a: 23
        }, {
            a: 100
        } ] )
    } )

    it( '`desc` is not defined after a term', async function() {
        try {
            await r.expr( 1 ).desc( 'foo' ).run()
            assert.fail( 'sholud throw' )
        }
        catch ( e ) {
            assert.equal( e.message, '`desc` is not defined after:\nr.expr(1)' )
        }
    } )

    it( '`asc` is not defined after a term', async function() {
        try {
            await r.expr( 1 ).asc( 'foo' ).run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, '`asc` is not defined after:\nr.expr(1)' )
        }
    } )

    it( '`skip` should work', async function() {
        const result = await r.expr( [ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 ] ).skip( 3 ).run()
        assert.deepEqual( result, [ 3, 4, 5, 6, 7, 8, 9 ] )
    } )

    it( '`skip` should throw if no argument has been passed', async function() {
        try {
            await r.db( dbName ).table( tableName ).skip().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e.message.match( /^`skip` takes 1 argument, 0 provided after/ ) )
        }
    } )

    it( '`limit` should work', async function() {
        const result = await r.expr( [ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 ] ).limit( 3 ).run()
        assert.deepEqual( result, [ 0, 1, 2 ] )
    } )

    it( '`limit` should throw if no argument has been passed', async function() {
        try {
            await r.db( dbName ).table( tableName ).limit().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e.message.match( /^`limit` takes 1 argument, 0 provided after/ ) )
        }
    } )

    it( '`slice` should work', async function() {
        const result = await r.expr( [ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 ] ).slice( 3, 5 ).run()
        assert.deepEqual( result, [ 3, 4 ] )
    } )

    it( '`slice` should handle options and optional end', async function() {
        let result = await r.expr( [ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 ] ).slice( 3 ).run()
        assert.deepEqual( result, [ 3, 4, 5, 6, 7, 8, 9 ] )

        result = await r.expr( [ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 ] ).slice( 3, {
            leftBound: 'open'
        } ).run()
        assert.deepEqual( result, [ 4, 5, 6, 7, 8, 9 ] )

        result = await r.expr( [ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 ] ).slice( 3, 5, {
            leftBound: 'open'
        } ).run()
        assert.deepEqual( result, [ 4 ] )
    } )

    it( '`slice` should work -- with options', async function() {
        let result = await r.expr( [ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23 ] ).slice( 5, 10, {
            rightBound: 'closed'
        } ).run()
        assert.deepEqual( result, [ 5, 6, 7, 8, 9, 10 ] )

        result = await r.expr( [ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23 ] ).slice( 5, 10, {
            rightBound: 'open'
        } ).run()
        assert.deepEqual( result, [ 5, 6, 7, 8, 9 ] )

        result = await r.expr( [ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23 ] ).slice( 5, 10, {
            leftBound: 'open'
        } ).run()
        assert.deepEqual( result, [ 6, 7, 8, 9 ] )

        result = await r.expr( [ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23 ] ).slice( 5, 10, {
            leftBound: 'closed'
        } ).run()
        assert.deepEqual( result, [ 5, 6, 7, 8, 9 ] )

        result = await r.expr( [ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23 ] ).slice( 5, 10, {
            leftBound: 'closed',
            rightBound: 'closed'
        } ).run()
        assert.deepEqual( result, [ 5, 6, 7, 8, 9, 10 ] )
    } )

    it( '`slice` should throw if no argument has been passed', async function() {
        try {
            await r.db( dbName ).table( tableName ).slice().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e.message.match( /^`slice` takes at least 1 argument, 0 provided after/ ) )
        }
    } )

    it( '`nth` should work', async function() {
        const result = await r.expr( [ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 ] ).nth( 3 ).run()
        assert( result, 3 )
    } )

    it( '`nth` should throw if no argument has been passed', async function() {
        try {
            await r.db( dbName ).table( tableName ).nth().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e.message.match( /^`nth` takes 1 argument, 0 provided after/ ) )
        }
    } )

    it( '`indexesOf` should work - datum', async function() {
        const result = await r.expr( [ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 ] ).nth( 3 ).run()
        assert( result, 3 )
    } )

    it( '`indexesOf` should work - r.row', async function() {
        const result = await r.expr( [ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 ] ).indexesOf( r.row.eq( 3 ) ).run()
        assert.equal( result, 3 )
    } )

    it( '`indexesOf` should work - function', async function() {
        const result = await r.expr( [ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 ] ).indexesOf( function( doc ) {
            return doc.eq( 3 )
        } ).run()
        assert.equal( result, 3 )
    } )

    it( '`indexesOf` should throw if no argument has been passed', async function() {
        try {
            await r.db( dbName ).table( tableName ).indexesOf().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e.message.match( /^`indexesOf` takes 1 argument, 0 provided after/ ) )
        }
    } )

    it( '`isEmpty` should work', async function() {
        let result = await r.expr( [ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 ] ).isEmpty().run()
        assert.equal( result, false )

        result = await r.expr( [] ).isEmpty().run()
        assert.equal( result, true )
    } )

    it( '`union` should work - 1', async function() {
        const result = await r.expr( [ 0, 1, 2 ] ).union( [ 3, 4, 5 ] ).run()
        assert.deepEqual( result.length, 6 )
        for ( var i = 0; i < 6; i++ ) {
            assert( result.indexOf( i ) >= 0 )
        }
    } )

    it( '`union` should work - 2', async function() {
        const result = await r.union( [ 0, 1, 2 ], [ 3, 4, 5 ], [ 6, 7 ] ).run()
        assert.deepEqual( result.length, 8 )
        for ( var i = 0; i < 8; i++ ) {
            assert( result.indexOf( i ) >= 0 )
        }
    } )

    it( '`union` should work - 3', async function() {
        const result = await r.union().run()
        assert.deepEqual( result, [] )
    } )

    it( '`union` should work with interleave - 1', async function() {
        const result = await r.expr( [ 0, 1, 2 ] ).union( [ 3, 4, 5 ], {
            interleave: false
        } ).run()
        assert.deepEqual( result, [ 0, 1, 2, 3, 4, 5 ] )
    } )

    it( '`union` should work with interleave - 1', async function() {
        const result = await r.expr( [ {
                name: 'Michel'
            }, {
                name: 'Sophie'
            }, {
                name: 'Laurent'
            } ] ).orderBy( 'name' )
            .union( r.expr( [ {
                name: 'Moo'
            }, {
                name: 'Bar'
            } ] ).orderBy( 'name' ), {
                interleave: 'name'
            } ).run()
        assert.deepEqual( result, [ {
                name: 'Bar'
            },
            {
                name: 'Laurent'
            },
            {
                name: 'Michel'
            },
            {
                name: 'Moo'
            },
            {
                name: 'Sophie'
            }
        ] )
    } )

    it( '`sample` should work', async function() {
        const result = await r.expr( [ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 ] ).sample( 2 ).run()
        assert.equal( result.length, 2 )
    } )

    it( '`sample` should throw if given -1', async function() {
        try {
            await r.expr( [ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 ] ).sample( -1 ).run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e.message.match( 'Number of items to sample must be non-negative, got `-1`' ) )
        }
    } )

    it( '`sample` should throw if no argument has been passed', async function() {
        try {
            await r.db( dbName ).table( tableName ).sample().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e.message.match( /^`sample` takes 1 argument, 0 provided after/ ) )
        }
    } )
} )
const path = require( 'path' )
const config = require( './config.js' )
const rebirthdb = require( path.join( __dirname, '/../lib' ) )
const {
    uuid
} = require( path.join( __dirname, '/util/common.js' ) )
const assert = require( 'assert' )
const {
    before,
    after,
    describe,
    it
} = require( 'mocha' )

describe( 'geo', () => {
    let r, dbName, tableName

    const numDocs = 10

    before( async function() {
        r = await rebirthdb( config )
        dbName = uuid()
        tableName = uuid()

        let result = await r.dbCreate( dbName ).run()
        assert.equal( result.dbs_created, 1 )

        result = await r.db( dbName ).tableCreate( tableName ).run()
        assert.equal( result.tables_created, 1 )

        result = await r.db( dbName ).table( tableName ).indexCreate( 'location', {
            geo: true
        } ).run()
        assert.equal( result.created, 1 )
        await r.db( dbName ).table( tableName ).indexWait( 'location' ).run()
        result = await r.db( dbName ).table( tableName ).insert(
            Array( numDocs ).fill( {
                location: r.point( r.random( 0, 1, {
                    float: true
                } ), r.random( 0, 1, {
                    float: true
                } ) )
            } )
        ).run()
        assert.equal( result.inserted, numDocs )
    } )

    after( async function() {
        await r.getPoolMaster().drain()
    } )

    it( '`r.circle` should work - 1', async function() {
        const result = await r.circle( [ 0, 0 ], 2 ).run()
        assert.equal( result.$reql_type$, 'GEOMETRY' )
        assert.equal( result.type, 'Polygon' )
        assert.equal( result.coordinates[ 0 ].length, 33 )
    } )

    it( '`r.circle` should work - 2', async function() {
        let result = await r.circle( r.point( 0, 0 ), 2 ).run()
        assert.equal( result.$reql_type$, 'GEOMETRY' )
        assert.equal( result.type, 'Polygon' )
        assert.equal( result.coordinates[ 0 ].length, 33 )

        result = await r.circle( r.point( 0, 0 ), 2, {
            numVertices: 40
        } ).run()
        assert.equal( result.$reql_type$, 'GEOMETRY' )
        assert.equal( result.type, 'Polygon' )
        assert.equal( result.coordinates[ 0 ].length, 41 )
    } )

    it( '`r.circle` should work - 3', async function() {
        const result = await r.circle( r.point( 0, 0 ), 2, {
            numVertices: 40,
            fill: false
        } ).run()
        assert.equal( result.$reql_type$, 'GEOMETRY' )
        assert.equal( result.type, 'LineString' )
        assert.equal( result.coordinates.length, 41 )
    } )

    it( '`r.circle` should work - 4', async function() {
        const result = await r.circle( r.point( 0, 0 ), 1, {
            unit: 'km'
        } ).eq( r.circle( r.point( 0, 0 ), 1000, {
            unit: 'm'
        } ) ).run()
        assert( result )
    } )

    it( '`r.circle` should throw with non recognized arguments', async function() {
        try {
            await r.circle( r.point( 0, 0 ), 1, {
                foo: 'bar'
            } ).run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e.message.match( /^Unrecognized option `foo` in `circle`/ ) )
        }
    } )

    it( '`r.circle` arity - 1', async function() {
        try {
            await r.circle( r.point( 0, 0 ) ).run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e.message.match( /^`r.circle` takes at least 2 arguments, 1 provided/ ) )
        }
    } )

    it( '`r.circle` arity - 2', async function() {
        try {
            await r.circle( 0, 1, 2, 3, 4 ).run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e.message.match( /^`r.circle` takes at most 3 arguments, 5 provided/ ) )
        }
    } )

    it( '`distance` should work - 1', async function() {
        const result = await r.point( 0, 0 ).distance( r.point( 1, 1 ) ).run()
        assert.equal( Math.floor( result ), 156899 )
    } )

    it( '`r.distance` should work - 1', async function() {
        const result = await r.distance( r.point( 0, 0 ), r.point( 1, 1 ) ).run()
        assert.equal( Math.floor( result ), 156899 )
    } )

    it( '`distance` should work - 2', async function() {
        const result = await r.point( 0, 0 ).distance( r.point( 1, 1 ), {
            unit: 'km'
        } ).run()
        assert.equal( Math.floor( result ), 156 )
    } )

    it( '`distance` arity - 1', async function() {
        try {
            await r.point( 0, 0 ).distance().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e.message.match( /^`distance` takes at least 1 argument, 0 provided/ ) )
        }
    } )

    it( '`distance` arity - 2', async function() {
        try {
            await r.point( 0, 0 ).distance( 1, 2, 3 ).run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e.message.match( /^`distance` takes at most 2 arguments, 3 provided/ ) )
        }
    } )

    it( '`fill` should work', async function() {
        const result = await r.circle( r.point( 0, 0 ), 2, {
            numVertices: 40,
            fill: false
        } ).fill().run()
        assert.equal( result.$reql_type$, 'GEOMETRY' )
        assert.equal( result.type, 'Polygon' )
        assert.equal( result.coordinates[ 0 ].length, 41 )
    } )

    it( '`fill` arity error', async function() {
        try {
            await r.circle( r.point( 0, 0 ), 2, {
                numVertices: 40,
                fill: false
            } ).fill( 1 ).run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e.message.match( /^`fill` takes 0 argument, 1 provided/ ) )
        }
    } )

    it( '`geojson` should work', async function() {
        const result = await r.geojson( {
            'coordinates': [ 0, 0 ],
            'type': 'Point'
        } ).run()
        assert.equal( result.$reql_type$, 'GEOMETRY' )
    } )

    it( '`geojson` arity error', async function() {
        try {
            await r.geojson( 1, 2, 3 ).run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e.message.match( /^`r.geojson` takes 1 argument, 3 provided/ ) )
        }
    } )

    it( '`toGeojson` should work', async function() {
        const result = await r.geojson( {
            'coordinates': [ 0, 0 ],
            'type': 'Point'
        } ).toGeojson().run()
        assert.equal( result.$reql_type$, undefined )
    } )

    it( '`toGeojson` arity error', async function() {
        try {
            await r.point( 0, 0 ).toGeojson( 1, 2, 3 ).run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e.message.match( /^`toGeojson` takes 0 argument, 3 provided/ ) )
        }
    } )

    it( '`getIntersecting` should work', async function() {
        // All points are in [0,1]x[0,1]
        const result = await r.db( dbName ).table( tableName ).getIntersecting( r.polygon( [ 0, 0 ], [ 0, 1 ], [ 1, 1 ], [ 1, 0 ] ), {
            index: 'location'
        } ).count().run()
        assert.equal( result, numDocs )
    } )

    it( '`getIntersecting` arity', async function() {
        try {
            // All points are in [0,1]x[0,1]
            await r.db( dbName ).table( tableName ).getIntersecting( r.polygon( [ 0, 0 ], [ 0, 1 ], [ 1, 1 ], [ 1, 0 ] ) ).count().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e.message.match( /^`getIntersecting` takes 2 arguments, 1 provided/ ) )
        }
    } )

    it( '`getNearest` should work', async function() {
        // All points are in [0,1]x[0,1]
        const result = await r.db( dbName ).table( tableName ).getNearest( r.point( 0, 0 ), {
            index: 'location',
            maxResults: 5
        } ).run()
        assert( result.length <= 5 )
    } )

    it( '`getNearest` arity', async function() {
        try {
            await r.db( dbName ).table( tableName ).getNearest( r.point( 0, 0 ) ).count().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e.message.match( /^`getNearest` takes 2 arguments, 1 provided/ ) )
        }
    } )

    it( '`includes` should work', async function() {
        const point1 = r.point( -117.220406, 32.719464 )
        const point2 = r.point( -117.206201, 32.725186 )
        const result = await r.circle( point1, 2000 ).includes( point2 ).run()
        assert( result )
    } )

    it( '`includes` arity', async function() {
        try {
            await r.circle( [ 0, 0 ], 2000 ).includes().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e.message.match( /^`includes` takes 1 argument, 0 provided/ ) )
        }
    } )

    it( '`intersects` should work', async function() {
        const point1 = r.point( -117.220406, 32.719464 )
        const point2 = r.point( -117.206201, 32.725186 )
        const result = await r.circle( point1, 2000 ).intersects( r.circle( point2, 2000 ) ).run()
        assert( result )
    } )

    it( '`intersects` arity', async function() {
        try {
            // All points are in [0,1]x[0,1]
            const point1 = r.point( -117.220406, 32.719464 )
            const point2 = r.point( -117.206201, 32.725186 )
            await r.circle( point1, 2000 ).intersects( r.circle( point2, 2000 ), 2, 3 ).run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e.message.match( /^`intersects` takes 1 argument, 3 provided/ ) )
        }
    } )

    it( '`r.line` should work - 1', async function() {
        const result = await r.line( [ 0, 0 ], [ 1, 2 ] ).run()
        assert.equal( result.$reql_type$, 'GEOMETRY' )
        assert.equal( result.type, 'LineString' )
        assert.equal( result.coordinates[ 0 ].length, 2 )
    } )

    it( '`r.line` should work - 2', async function() {
        const result = await r.line( r.point( 0, 0 ), r.point( 1, 2 ) ).run()
        assert.equal( result.$reql_type$, 'GEOMETRY' )
        assert.equal( result.type, 'LineString' )
        assert.equal( result.coordinates[ 0 ].length, 2 )
    } )

    it( '`r.line` arity', async function() {
        try {
            await r.line().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e.message.match( /^`r.line` takes at least 2 arguments, 0 provided/ ) )
        }
    } )

    it( '`r.point` should work', async function() {
        const result = await r.point( 0, 0 ).run()
        assert.equal( result.$reql_type$, 'GEOMETRY' )
        assert.equal( result.type, 'Point' )
        assert.equal( result.coordinates.length, 2 )
    } )

    it( '`r.point` arity', async function() {
        try {
            await r.point().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e.message.match( /^`r.point` takes 2 arguments, 0 provided/ ) )
        }
    } )

    it( '`r.polygon` should work', async function() {
        const result = await r.polygon( [ 0, 0 ], [ 0, 1 ], [ 1, 1 ] ).run()
        assert.equal( result.$reql_type$, 'GEOMETRY' )
        assert.equal( result.type, 'Polygon' )
        assert.equal( result.coordinates[ 0 ].length, 4 ) // The server will close the line
    } )

    it( '`r.polygon` arity', async function() {
        try {
            await r.polygon().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e.message.match( /^`r.polygon` takes at least 3 arguments, 0 provided/ ) )
        }
    } )

    it( '`polygonSub` should work', async function() {
        const result = await r.polygon( [ 0, 0 ], [ 0, 1 ], [ 1, 1 ], [ 1, 0 ] ).polygonSub( r.polygon( [ 0.4, 0.4 ], [ 0.4, 0.5 ], [ 0.5, 0.5 ] ) ).run()
        assert.equal( result.$reql_type$, 'GEOMETRY' )
        assert.equal( result.type, 'Polygon' )
        assert.equal( result.coordinates.length, 2 ) // The server will close the line
    } )

    it( '`polygonSub` arity', async function() {
        try {
            await r.polygon( [ 0, 0 ], [ 0, 1 ], [ 1, 1 ] ).polygonSub().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e.message.match( /^`polygonSub` takes 1 argument, 0 provided/ ) )
        }
    } )
} )
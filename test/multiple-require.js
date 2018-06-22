const path = require( 'path' )
const config = require( path.join( __dirname, '/config.js' ) )
const assert = require( 'assert' )
const {
    before,
    after,
    describe,
    it
} = require( 'mocha' )

describe( 'multiple require', () => {
    let r1, r2

    before( async () => {
        const rebirthdb = require( path.join( __dirname, '/../lib' ) )
        r1 = await rebirthdb( config )

        delete require.cache[ require.resolve( path.join( __dirname, '/../lib' ) ) ]
        const rethinkdb_ = require( path.join( __dirname, '/../lib' ) )
        r2 = await rethinkdb_( config )
    } )

    after( async () => {
        await r1.getPoolMaster().drain()
        await r2.getPoolMaster().drain()
    } )

    it( 'Multiple import should not share the same pool', function() {
        assert( r1.getPoolMaster() !== r2.getPoolMaster() )
    } )

    it( 'Multiple import should not share the same nestingLevel value', function() {
        r1.setNestingLevel( 19 )
        r2.setNestingLevel( 100 )
        assert( r1.nestingLevel !== r2.nestingLevel )
        assert.equal( r1.nestingLevel, 19 )
        assert.equal( r2.nestingLevel, 100 )
    } )

    it( 'Multiple import should not share the same `nextVarId`', function() {
        r1.expr( 1 ).do( function( a, b, c ) {
            return 1
        } )
        r2.expr( 2 ).do( function( d ) {
            return 2
        } )
        assert.equal( r1.nextVarId, 4 )
        assert.equal( r2.nextVarId, 2 )
    } )
} )
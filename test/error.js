const path = require( 'path' )
const config = require( './config.js' )
const rebirthdb = require( path.join( __dirname, '/../lib' ) )
const assert = require( 'assert' )
const { ReqlRuntimeError } = require( path.join( __dirname, '/../lib/error.js' ) )
const { before, after, describe, it } = require( 'mocha' )

describe( 'errors', () => {
    let r

    before( async () => {
        r = await rebirthdb( config )
    } )

    after( async () => {
        await r.getPoolMaster().drain()
    } )

    it( 'ReqlResourceError', async function() {
        try {
            await r.expr( [ 1, 2, 3, 4 ] ).run( {
                arrayLimit: 2
            } )
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e instanceof ReqlRuntimeError )
            assert.equal( e.name, 'ReqlResourceError' )
        }
    } )

    it( 'ReqlLogicError', async function() {
        try {
            await r.expr( 1 ).add( 'foo' ).run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e instanceof ReqlRuntimeError )
            assert.equal( e.name, 'ReqlLogicError' )
        }
    } )

    it( 'ReqlOpFailedError', async function() {
        try {
            await r.db( 'DatabaseThatDoesNotExist' ).tableList().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e instanceof ReqlRuntimeError )
            assert.equal( e.name, 'ReqlOpFailedError' )
        }
    } )

    it( 'ReqlUserError', async function() {
        try {
            await r.branch( r.error( 'a' ), 1, 2 ).run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e instanceof ReqlRuntimeError )
            assert.equal( e.name, 'ReqlUserError' )
        }
    } )

    describe( 'Missing tests', function() {
        it( 'ReqlServerError (maybe need to use fakeServer)', function() {} )
        it( 'ReqlInternalError no easy way to trigger', function() {} )
        it( 'ReqlOpIndeterminateError no easy way to trigger', function() {} )
    } )
} )
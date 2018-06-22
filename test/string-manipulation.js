const path = require( 'path' )
const config = require( './config.js' )
const rebirthdb = require( path.join( __dirname, '/../lib' ) )
const assert = require( 'assert' )
const { before, after, describe, it } = require( 'mocha' )

describe( 'string manipulation', () => {
    let r

    before( async () => {
        r = await rebirthdb( config )
    } )

    after( async () => {
        await r.getPool().drain()
    } )

    it( '`match` should work', async function() {
        const result = await r.expr( 'hello' ).match( 'hello' ).run()
        assert.deepEqual( result, {
            'end': 5,
            'groups': [],
            'start': 0,
            'str': 'hello'
        } )
    } )

    it( '`match` should throw if no arguement has been passed', async function() {
        try {
            await r.expr( 'foo' ).match().run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, '`match` takes 1 argument, 0 provided after:\nr.expr("foo")' )
        }
    } )

    it( '`upcase` should work', async function() {
        const result = await r.expr( 'helLo' ).upcase().run()
        assert.equal( result, 'HELLO' )
    } )

    it( '`downcase` should work', async function() {
        const result = await r.expr( 'HElLo' ).downcase().run()
        assert.equal( result, 'hello' )
    } )

    it( '`split` should work', async function() {
        const result = await r.expr( 'foo  bar bax' ).split().run()
        assert.deepEqual( result, [ 'foo', 'bar', 'bax' ] )
    } )

    it( '`split(separator)` should work', async function() {
        const result = await r.expr( '12,37,,22,' ).split( ',' ).run()
        assert.deepEqual( result, [ '12', '37', '', '22', '' ] )
    } )

    it( '`split(separtor, max)` should work', async function() {
        const result = await r.expr( 'foo  bar bax' ).split( null, 1 ).run()
        assert.deepEqual( result, [ 'foo', 'bar bax' ] )
    } )
} )
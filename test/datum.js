const path = require( 'path' )
const config = require( './config.js' )
const rebirthdb = require( path.join( __dirname, '/../lib' ) )
const assert = require( 'assert' )

const { before, after, afterEach, describe, it } = require( 'mocha' )

describe( 'datum', function() {
    let r

    before( async () => {
        r = await rebirthdb( config )
    } )

    after( async () => {
        await r.getPoolMaster().drain()
    } )

    it( 'All raws datum should be defined', async function() {
        let result = await r.expr( 1 ).run()
        assert.equal( result, 1 )

        result = await r.expr( null ).run()
        assert.equal( result, null )

        result = await r.expr( false ).run()
        assert.equal( result, false )

        result = await r.expr( true ).run()
        assert.equal( result, true )

        result = await r.expr( 'Hello' ).run()
        assert.equal( result, 'Hello' )

        result = await r.expr( [ 0, 1, 2 ] ).run()
        assert.deepEqual( result, [ 0, 1, 2 ] )

        result = await r.expr( {
            a: 0,
            b: 1
        } ).run()
        assert.deepEqual( result, {
            a: 0,
            b: 1
        } )
    } )

    it( '`expr` is not defined after a term', async function() {
        try {
            await r.expr( 1 ).expr( 'foo' ).run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, '`expr` is not defined after:\nr.expr(1)' )
        }
    } )

    it( '`r.expr` should take a nestingLevel value and throw if the nesting level is reached', async function() {
        try {
            r.expr( {
                a: {
                    b: {
                        c: {
                            d: 1
                        }
                    }
                }
            }, 2 )
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert.equal( e.message, 'Nesting depth limit exceeded.\nYou probably have a circular reference somewhere.' )
        }
    } )

    describe( 'nesting level', function() {
        afterEach( () => {
            r.setNestingLevel( r._nestingLevel )
        } )

        it( '`r.expr` should throw when setNestingLevel is too small', async function() {
            r.setNestingLevel( 2 )
            try {
                await r.expr( {
                    a: {
                        b: {
                            c: {
                                d: 1
                            }
                        }
                    }
                } ).run()
                assert.fail( 'should throw' )
            }
            catch ( e ) {
                assert.equal( e.message, 'Nesting depth limit exceeded.\nYou probably have a circular reference somewhere.' )
            }
        } )

        it( '`r.expr` should work when setNestingLevel set back the value to 100', async function() {
            r.setNestingLevel( 100 )
            const result = await r.expr( {
                a: {
                    b: {
                        c: {
                            d: 1
                        }
                    }
                }
            } ).run()
            assert.deepEqual( result, {
                a: {
                    b: {
                        c: {
                            d: 1
                        }
                    }
                }
            } )
        } )
    } )

    describe( 'array limit', function() {
        afterEach( () => {
            r.setArrayLimit( r._arrayLimit )
        } )

        it( '`r.expr` should throw when ArrayLimit is too small', async function() {
            try {
                await r.expr( [ 0, 1, 2, 3, 4, 5, 6, 8, 9 ] ).run( {
                    arrayLimit: 2
                } )
                assert.fail( 'should throw' )
            }
            catch ( e ) {
                assert( e.message.match( /^Array over size limit `2` in/ ) )
            }
        } )

        it( '`r.expr` should throw when ArrayLimit is too small - options in run take precedence', async function() {
            r.setArrayLimit( 100 )
            try {
                await r.expr( [ 0, 1, 2, 3, 4, 5, 6, 8, 9 ] ).run( {
                    arrayLimit: 2
                } )
                assert.fail( 'should throw' )
            }
            catch ( e ) {
                assert( e.message.match( /^Array over size limit `2` in/ ) )
            }
        } )

        it( '`r.expr` should throw when setArrayLimit is too small', async function() {
            r.setArrayLimit( 2 )
            try {
                await r.expr( [ 0, 1, 2, 3, 4, 5, 6, 8, 9 ] ).run()
                assert.fail( 'shold throw' )
            }
            catch ( e ) {
                assert( e.message.match( /^Array over size limit `2` in/ ) )
            }
        } )

        it( '`r.expr` should work when setArrayLimit set back the value to 100000', async function() {
            r.setArrayLimit( 100000 )
            const result = await r.expr( [ 0, 1, 2, 3, 4, 5, 6, 8, 9 ] ).run()
            assert.deepEqual( result, [ 0, 1, 2, 3, 4, 5, 6, 8, 9 ] )
        } )
    } )

    it( '`r.expr` should fail with NaN', async function() {
        try {
            await r.expr( NaN ).run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e.message.match( /^Cannot convert `NaN` to JSON/ ) )
        }
    } )

    it( '`r.expr` should not NaN if not run', async function() {
        r.expr( NaN )
    } )

    it( '`r.expr` should fail with Infinity', async function() {
        try {
            await r.expr( Infinity ).run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e.message.match( /^Cannot convert `Infinity` to JSON/ ) )
        }
    } )

    it( '`r.expr` should not Infinity if not run', async function() {
        r.expr( Infinity )
    } )

    it( '`r.expr` should work with high unicode char', async function() {
        const result = await r.expr( '“' ).run()
        assert.equal( result, '“' )
    } )

    it( '`r.binary` should work - with a buffer', async function() {
        const result = await r.binary( Buffer.from( [ 1, 2, 3, 4, 5, 6 ] ) ).run()
        assert( result instanceof Buffer )
        assert.deepEqual( result.toJSON().data, [ 1, 2, 3, 4, 5, 6 ] )
    } )

    it( '`r.binary` should work - with a ReQL term', async function() {
        let result = await r.binary( r.expr( 'foo' ) ).run()
        assert( result instanceof Buffer )
        result = await r.expr( result ).coerceTo( 'STRING' ).run()
        assert.equal( result, 'foo' )
    } )

    it( '`r.expr` should work with binaries', async function() {
        const result = await r.expr( Buffer.from( [ 1, 2, 3, 4, 5, 6 ] ) ).run()
        assert( result instanceof Buffer )
        assert.deepEqual( result.toJSON().data, [ 1, 2, 3, 4, 5, 6 ] )
    } )
} )
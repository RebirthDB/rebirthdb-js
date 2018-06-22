const path = require( 'path' )
const config = require( path.join( __dirname, '/config.js' ) )
const rebirthdb = require( path.join( __dirname, '/../lib' ) )
const util = require( path.join( __dirname, '/util/common.js' ) )
const assert = require( 'assert' )
const uuid = util.uuid
const { before, after, describe, it } = require( 'mocha' )


describe( 'client backtraces', () => {
    let r, dbName, tableName

    before( async () => {
        r = await rebirthdb( config )

        dbName = uuid()
        tableName = uuid()

        let result = await r.dbCreate( dbName ).run()
        assert.equal( result.dbs_created, 1 )

        result = await r.db( dbName ).tableCreate( tableName ).run()
        assert.equal( result.tables_created, 1 )

        result = await r.db( dbName ).table( tableName ).insert( Array( 100 ).fill( {} ) ).run()
        assert.equal( result.inserted, 100 )
        assert.equal( result.generated_keys.length, 100 )
    } )

    after( async () => {
        await r.getPoolMaster().drain()
    } )

    /*
Frames:
[ 1, 1, 1 ]

Error:
Cannot convert `NaN` to JSON in:
r.db("af552fefa372998f05c1dff2fa4c293c").table("89865145147d850616fcbe93011b1d5e")
    .map(function(var_1) {
        return var_1("key").add(NaN)
                                ^^^
    })
*/
    it( 'Test backtrace for r.db(dbName).table(tableName).map(function(doc) { return doc("key").add(NaN)})', async () => {
        try {
            r.nextVarId = 1
            await r.db( dbName ).table( tableName ).map( function( doc ) {
                return doc( 'key' ).add( NaN )
            } ).run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e.message === 'Cannot convert `NaN` to JSON in:\nr.db("' + dbName + '").table("' + tableName + '")\n    .map(function(var_1) {\n        return var_1("key").add(NaN)\n                                ^^^ \n    })\n' )
        }
    } )

    /*
Frames:
[ 1, 1, 1 ]

Error:
Cannot convert `Infinity` to JSON in:
r.db("dd054a14db348f5bcb99bbf14615955c").table("2f66694bbfa2a7bd2f0b0ef0460e7178")
    .map(function(var_1) {
        return var_1("key").add(Infinity)
                                ^^^^^^^^
    })
*/
    it( 'Test backtrace for r.db(dbName).table(tableName).map(function(doc) { return doc("key").add(Infinity)})', async () => {
        try {
            r.nextVarId = 1
            await r.db( dbName ).table( tableName ).map( function( doc ) {
                return doc( 'key' ).add( Infinity )
            } ).run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e.message === 'Cannot convert `Infinity` to JSON in:\nr.db("' + dbName + '").table("' + tableName + '")\n    .map(function(var_1) {\n        return var_1("key").add(Infinity)\n                                ^^^^^^^^ \n    })\n' )
        }
    } )

    /*
Frames:
[ 1, 1, 1 ]

Error:
Cannot convert `undefined` with r.expr() in:
r.db("28f1684f7a5927ce592bc1641bc0f9ac").table("55e82a8517599394fd96d8fb1acfcdef")
    .map(function(var_1) {
        return var_1("key").add(undefined)
                                ^^^^^^^^^
    })
*/
    it( 'Test backtrace for r.db(dbName).table(tableName).map(function(doc) { return doc("key").add(undefined)})', async () => {
        try {
            r.nextVarId = 1
            await r.db( dbName ).table( tableName ).map( function( doc ) {
                return doc( 'key' ).add( undefined )
            } ).run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e.message === 'Cannot convert `undefined` with r.expr() in:\nr.db("' + dbName + '").table("' + tableName + '")\n    .map(function(var_1) {\n        return var_1("key").add(undefined)\n                                ^^^^^^^^^ \n    })\n' )
        }
    } )

    /*
Frames:
[ 1, 1, 1, 'adult', 1 ]

Error:
Cannot convert `NaN` to JSON in:
r.db("aa802b7a7ec470632ddb3c515e7ab30b").table("fe82af2d2203e8fbed96e0cbbc29e936")
    .merge(function(var_1) {
        return r.branch(var_1("location").eq("US"), {
            adult: var_1("age").gt(NaN)
                                   ^^^
        }, {
            radult: var_1("age").gt(18)
        })
    })
*/
    it( 'Test backtrace for r.db(dbName).table(tableName).merge(function(user) { return r.branch( user("location").eq("US"), { adult: user("age").gt(NaN) }, {radult: user("age").gt(18) }) })', async () => {
        try {
            r.nextVarId = 1
            await r.db( dbName ).table( tableName ).merge( function( user ) {
                return r.branch( user( 'location' ).eq( 'US' ), {
                    adult: user( 'age' ).gt( NaN )
                }, {
                    radult: user( 'age' ).gt( 18 )
                } )
            } ).run()
            assert.fail( 'should throw' )
        }
        catch ( e ) {
            assert( e.message === 'Cannot convert `NaN` to JSON in:\nr.db("' + dbName + '").table("' + tableName + '")\n    .merge(function(var_1) {\n        return r.branch(var_1("location").eq("US"), {\n            adult: var_1("age").gt(NaN)\n                                   ^^^ \n        }, {\n            radult: var_1("age").gt(18)\n        })\n    })\n' )
        }
    } )
} )
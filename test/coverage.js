const path = require( 'path' )
const protodef = require( path.join( __dirname, '/../lib/protodef.js' ) )
const fs = require( 'fs' )
const keys = Object.keys( protodef.Term.TermType )
const {
    describe,
    it
} = require( 'mocha' )
const assert = require( 'assert' )

describe( 'coverage', () => {
    // Test that the term appears somewhere in the file, which find terms that were not implemented
    it( 'all terms should be present in term.js', async () => {
        const str = fs.readFileSync( path.join( __dirname, '/../lib/term.js' ), 'utf8' )
        const ignoredKeys = [ // not implemented since we use the JSON protocol
            'DATUM',
            'MAKE_OBJ',
            'BETWEEN_DEPRECATED',
            'ERROR' // define in index, error is defined for behaving like a promise
        ]
        const missing = []

        for ( const key of keys ) {
            if ( ignoredKeys.includes( key ) ) {
                continue
            }
            if ( str.match( new RegExp( key ) ) === null ) {
                missing.push( key )
            }
        }

        if ( missing.length > 0 ) {
            assert.fail( 'Some terms were not found:', JSON.stringify( missing ) )
        }
    } )

    it( 'All terms should be present in error.js', async () => {
        const str = fs.readFileSync( path.join( __dirname, '/../lib/error.js' ), 'utf8' )
        const ignoredKeys = [
            'DATUM',
            'MAKE_OBJ',
            'BETWEEN_DEPRECATED'
        ]
        const missing = []

        for ( const key of keys ) {
            if ( ignoredKeys.includes( key ) ) {
                continue
            }
            if ( str.match( new RegExp( key ) ) === null ) {
                missing.push( key )
            }
        }

        if ( missing.length > 0 ) {
            assert.fail( 'Some terms were not found: ' + JSON.stringify( missing ) )
        }
    } )
} )
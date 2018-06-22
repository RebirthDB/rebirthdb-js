const b = require( 'browserify' )( './lib' )
const fs = require( 'fs' )

const REQUIRE_FILES = [
    'connection.js',
    'cursor.js',
    'dequeue.js',
    'error.js',
    'helper.js',
    'linked_list.js',
    'metadata.js',
    'pool.js',
    'pool_master.js',
    'protodef.js',
    'stream.js',
    'term.js',
    'transform_stream.js',
    'writable_stream.js'
];

b.add( './lib/index.js' );

for ( const file of REQUIRE_FILES ) {
    b.require( './lib/' + file, {
        expose: './lib/' + file
    } );
}

b.require( './lib/index.js', {
    expose: 'rebirthdb-js'
} );

b.bundle( ( err, result ) => {
    if ( err ) {
        console.error( err );
        return;
    }

    if ( !fs.existsSync( './dist' ) ) {
        fs.mkdirSync( './dist' );
    }

    fs.writeFileSync( './dist/rebirthdb-js.js', result );
} );
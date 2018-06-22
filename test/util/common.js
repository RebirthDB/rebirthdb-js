const { it } = require( 'mocha' )

function s4() {
    return Math.floor( ( 1 + Math.random() ) * 0x10000 ).toString( 16 ).substring( 1 );
}

function uuid() {
    return s4() + s4() + s4() + s4() + s4() + s4() + s4() + s4();
}

function It( testName, generatorFn ) {
    it( testName, done => {
        Promise.coroutine( generatorFn )( done )
    } )
}

function sleep( timer ) {
    return new Promise( resolve => {
        setTimeout( resolve, timer )
    } );
}

module.exports.uuid = uuid
module.exports.It = It
module.exports.sleep = sleep
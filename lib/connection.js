const net = require( 'net' )
const tls = require( 'tls' )
const events = require( 'events' )
const util = require( 'util' )
const crypto = require( 'crypto' )
const path = require( 'path' )
const helper = require( path.join( __dirname, '/helper.js' ) )
const Err = require( path.join( __dirname, '/error.js' ) )
const Cursor = require( path.join( __dirname, '/cursor.js' ) )
const ReadableStream = require( path.join( __dirname, '/stream.js' ) )
const Metadata = require( path.join( __dirname, '/metadata.js' ) )

const protodef = require( path.join( __dirname, '/protodef.js' ) )
const responseTypes = protodef.Response.ResponseType

// We'll ping a connection using this special value.
const PING_VALUE = '__rebirthdb-js_ping__'

const PROTOCOL_VERSION = 0
const AUTHENTIFICATION_METHOD = 'SCRAM-SHA-256'
const KEY_LENGTH = 32 // Because we are currently using SHA 256
const NULL_BUFFER = Buffer.from( '\0', 'binary' )
const CACHE_PBKDF2 = {}

// handshake states to track progress of handshake
const STATE_ERROR = -1
const STATE_INITIAL = 0
const STATE_COMPUTE_SALTED_PASSWORD = 1
const STATE_COMPARE_DIGEST = 2
const STATE_ESTABLISHED = 4

function Connection( r, options, resolve, reject ) {
    this.r = r
    this.state = STATE_INITIAL

    // Retain `options` for reconnecting
    this.options = options || ( options = {} )

    this.host = options.host || r._host
    this.port = options.port || r._port
    if ( options.authKey != null ) {
        if ( options.user != null || options.password != null ) {
            throw new Err.ReqlDriverError( 'Cannot use both authKey and password' )
        }
        this.user = r._user
        this.password = options.authKey
    }
    else {
        if ( options.user === undefined ) {
            this.user = r._user
        }
        else {
            this.user = options.user
        }
        if ( options.password === undefined ) {
            this.password = r._password
        }
        else {
            this.password = options.password
        }
    }

    this.authKey = options.authKey || r._authKey
    this.releaseFeed = options.releaseFeed || r._releaseFeed
    // period in *seconds* for the connection to be opened
    this.timeoutConnect = options.timeout || r._timeoutConnect
    // The connection will be pinged every <pingInterval> seconds
    this.pingInterval = options.pingInterval || r._pingInterval

    if ( options.db ) this.db = options.db // Pass to each query

    this.token = 1
    this.buffer = Buffer.alloc( 0 )

    this.metadata = {}

    this.open = false // true only if the user can write on the socket
    this.timeout = null

    if ( options.connection ) {
        this.connection = options.connection
    }
    else {
        let family = 'IPv4'
        if ( net.isIPv6( this.host ) ) {
            family = 'IPv6'
        }

        const connectionArgs = {
            host: this.host,
            port: this.port,
            family: family
        }

        const tlsOptions = options.ssl || false
        if ( tlsOptions === false ) {
            this.connection = net.connect( connectionArgs )
        }
        else {
            if ( helper.isPlainObject( tlsOptions ) ) {
                // Copy the TLS options in connectionArgs
                helper.loopKeys( tlsOptions, ( tlsOptions, key ) => {
                    connectionArgs[ key ] = tlsOptions[ key ]
                } )
            }
            this.connection = tls.connect( connectionArgs )
        }
    }

    this.connection.setKeepAlive( true )

    this.timeoutOpen = setTimeout( () => {
        this.connection.end() // Send a FIN packet
        reject( new Err.ReqlDriverError( 'Failed to connect to ' + this.host + ':' + this.port + ' in less than ' + this.timeoutConnect + 's' ).setOperational() )
    }, this.timeoutConnect * 1000 )

    this.connection.on( 'end', () => {
        this.open = false
        this.emit( 'end' )
        // We got a FIN packet, so we'll just flush
        this._flush()
    } )

    this.connection.setNoDelay()

    this.connection.on( 'close', () => {
        // We emit end or close just once
        clearTimeout( this.timeoutOpen )
        clearInterval( this.pingIntervalId )
        this.connection.removeAllListeners()
        this.open = false
        this.emit( 'closed' )
        // The connection is fully closed, flush (in case 'end' was not triggered)
        this._flush()
    } )

    this.connection.once( 'error', error => {
        reject( new Err.ReqlDriverError( 'Failed to connect to ' + this.host + ':' + this.port + '\nFull error:\n' + JSON.stringify( error ) ).setOperational() )
    } )

    this.connection.on( 'connect', () => {
        this.connection.removeAllListeners( 'error' )
        this.connection.on( 'error', error => {
            this.emit( 'error', error )
        } )

        const versionBuffer = Buffer.alloc( 4 )
        versionBuffer.writeUInt32LE( protodef.VersionDummy.Version.V1_0, 0 )

        this.randomString = Buffer.from( crypto.randomBytes( 18 ) ).toString( 'base64' )
        const authBuffer = Buffer.from( JSON.stringify( {
            protocol_version: PROTOCOL_VERSION,
            authentication_method: AUTHENTIFICATION_METHOD,
            authentication: 'n,,n=' + this.user + ',r=' + this.randomString
        } ) )

        try {
            this.connection.write( Buffer.concat( [ versionBuffer, authBuffer, NULL_BUFFER ] ) )
        }
        catch ( err ) {
            // The TCP connection is open, but the ReQL connection wasn't established.
            // We can just abort the whole thing
            this.open = false
            reject( new Err.ReqlDriverError( 'Failed to perform handshake with ' + this.host + ':' + this.port ).setOperational() )
        };
    } )

    this.connection.once( 'end', () => {
        this.open = false
    } )

    this.connection.on( 'data', buffer => {
        if ( this.state === STATE_ERROR ) {
            return
        }
        this.buffer = Buffer.concat( [ this.buffer, buffer ] )

        if ( this.open === false ) {
            let handshake
            for ( let i = 0; i < this.buffer.length; i++ ) {
                if ( this.buffer[ i ] === 0 ) {
                    const handshakeStr = this.buffer.slice( 0, i ).toString()
                    this.buffer = this.buffer.slice( i + 1 ) // +1 to remove the null byte
                    try {
                        handshake = JSON.parse( handshakeStr )
                    }
                    catch ( error ) {
                        this._abort()
                        return reject( new Err.ReqlDriverError( 'Could not parse the message sent by the server : \'' + handshakeStr + '\'' ).setOperational() )
                    }
                    try {
                        this._performHandshake( handshake, reject )
                    }
                    catch ( error ) {
                        this._abort()
                        return reject( error )
                    }
                }
            }
            if ( this.state === STATE_ESTABLISHED ) {
                this.open = true
                this.connection.removeAllListeners( 'error' )
                this.connection.on( 'error', () => {
                    this.open = false
                } )
                clearTimeout( this.timeoutOpen )
                this._setPingInterval()
                resolve( this )
            }
        }
        else {
            while ( this.buffer.length >= 12 ) {
                const token = this.buffer.readUInt32LE( 0 ) + 0x100000000 * this.buffer.readUInt32LE( 4 )
                const responseLength = this.buffer.readUInt32LE( 8 )

                if ( this.buffer.length < 12 + responseLength ) break

                const responseBuffer = this.buffer.slice( 12, 12 + responseLength )
                const response = JSON.parse( responseBuffer )

                this._processResponse( response, token )

                this.buffer = this.buffer.slice( 12 + responseLength )
            }
        }
    } )

    this.connection.on( 'timeout', () => {
        this.connection.open = false
        this.emit( 'timeout' )
    } )

    // We want people to be able to jsonify a cursor
    this.connection.toJSON = function() {
        return '"A socket object cannot be converted to JSON due to circular references."'
    }

    // For the pool implementation
    this.node = null
    this.id = Math.random()
}

util.inherits( Connection, events.EventEmitter )

Connection.prototype._performHandshake = function( handshake, reject ) {
    if ( handshake.success !== true ) {
        throw new Err.ReqlDriverError( 'Error ' + handshake.error_code + ':' + handshake.error ).setOperational()
    }
    switch ( this.state ) {
        case STATE_INITIAL:
            this._checkProtocolVersion( handshake )
            return
        case STATE_COMPUTE_SALTED_PASSWORD:
            this._computeSaltedPassword( handshake, reject )
            return
        case STATE_COMPARE_DIGEST:
            this._compareDigest( handshake )
    }
}

Connection.prototype._checkProtocolVersion = function( handshake ) {
    // Expect max_protocol_version, min_protocol_version, server_version, success
    const minVersion = handshake.min_protocol_version
    const maxVersion = handshake.max_protocol_version
    if ( minVersion > PROTOCOL_VERSION || maxVersion < PROTOCOL_VERSION ) {
        throw new Err.ReqlDriverError( 'Unsupported protocol version: ' + PROTOCOL_VERSION + ', expected between ' + minVersion + ' and ' + maxVersion ).setOperational()
    }
    this.state = STATE_COMPUTE_SALTED_PASSWORD
}

Connection.prototype._computeSaltedPassword = function( handshake, reject ) {
    const authentication = helper.splitCommaEqual( handshake.authentication )

    const randomNonce = authentication.r
    const salt = Buffer.from( authentication.s, 'base64' )
    const iterations = parseInt( authentication.i )

    if ( randomNonce.substr( 0, this.randomString.length ) !== this.randomString ) {
        throw new Err.ReqlDriverError( 'Invalid nonce from server' ).setOperational()
    }

    // The salt is constant, so we can cache the salted password.
    const cacheKey = this.password.toString( 'base64' ) + ',' + salt.toString( 'base64' ) + ',' + iterations
    if ( CACHE_PBKDF2.hasOwnProperty( cacheKey ) ) {
        this._sendProof( handshake.authentication, randomNonce, CACHE_PBKDF2[ cacheKey ] )
        this.state = STATE_COMPARE_DIGEST
    }
    else {
        crypto.pbkdf2( this.password, salt, iterations, KEY_LENGTH, 'sha256', ( error, saltedPassword ) => {
            if ( error != null ) {
                reject( new Err.ReqlDriverError( 'Could not derive the key. Error:' + error.toString() ).setOperational() )
            }
            CACHE_PBKDF2[ cacheKey ] = saltedPassword
            this._sendProof( handshake.authentication, randomNonce, saltedPassword )
            this.state = STATE_COMPARE_DIGEST
        } )
    }
}

Connection.prototype._sendProof = function( authentication, randomNonce, saltedPassword ) {
    const clientFinalMessageWithoutProof = 'c=biws,r=' + randomNonce
    const clientKey = crypto.createHmac( 'sha256', saltedPassword ).update( 'Client Key' ).digest()
    const storedKey = crypto.createHash( 'sha256' ).update( clientKey ).digest()

    const authMessage =
        'n=' + this.user + ',r=' + this.randomString + ',' +
        authentication + ',' +
        clientFinalMessageWithoutProof

    const clientSignature = crypto.createHmac( 'sha256', storedKey ).update( authMessage ).digest()
    const clientProof = helper.xorBuffer( clientKey, clientSignature )

    const serverKey = crypto.createHmac( 'sha256', saltedPassword ).update( 'Server Key' ).digest()
    this.serverSignature = crypto.createHmac( 'sha256', serverKey ).update( authMessage ).digest()

    const message = JSON.stringify( {
        authentication: clientFinalMessageWithoutProof + ',p=' + clientProof.toString( 'base64' )
    } )

    try {
        this.connection.write( Buffer.concat( [ Buffer.from( message.toString() ), NULL_BUFFER ] ) )
    }
    catch ( err ) {
        // The TCP connection is open, but the ReQL connection wasn't established.
        // We can just abort the whole thing
        throw new Err.ReqlDriverError( 'Failed to perform handshake with ' + this.host + ':' + this.port ).setOperational()
    }
}

Connection.prototype._compareDigest = function( handshake ) {
    const firstEquals = handshake.authentication.indexOf( '=' )
    const serverSignatureValue = handshake.authentication.slice( firstEquals + 1 )

    if ( !helper.compareDigest( serverSignatureValue, this.serverSignature.toString( 'base64' ) ) ) {
        throw new Err.ReqlDriverError( 'Invalid server signature' ).setOperational()
    }
    this.state = STATE_ESTABLISHED
}

Connection.prototype._setPingInterval = function() {
    if ( this.pingInterval > 0 ) {
        this.pingIntervalId = setInterval( () => {
            this.pendingPing = true
            this.r.error( PING_VALUE ).run( this ).catch( error => {
                this.pendingPing = false
                if ( error.message !== PING_VALUE ) {
                    this.emit( 'error', new Err.ReqlDriverError( 'Could not ping the connection' ).setOperational() )
                    this.open = false
                    this.connection.end()
                }
            } )
        }, this.pingInterval * 1000 )
    }
}

Connection.prototype._abort = function() {
    this.state = STATE_ERROR
    this.removeAllListeners()
    this.close()
}

Connection.prototype._processResponse = function( response, token ) {
    // console.log('Connection.prototype._processResponse: '+token);
    // console.log(JSON.stringify(response, null, 2));

    const type = response.t
    let result
    let cursor
    let stream
    let currentResolve, currentReject
    let datum
    let options
    let error
    let done = false

    if ( type === responseTypes.COMPILE_ERROR ) {
        this.emit( 'release' )
        if ( typeof this.metadata[ token ].reject === 'function' ) {
            this.metadata[ token ].reject( new Err.ReqlCompileError( helper.makeAtom( response ), this.metadata[ token ].query, response ) )
        }

        delete this.metadata[ token ]
    }
    else if ( type === responseTypes.CLIENT_ERROR ) {
        this.emit( 'release' )

        if ( typeof this.metadata[ token ].reject === 'function' ) {
            currentResolve = this.metadata[ token ].resolve
            currentReject = this.metadata[ token ].reject
            this.metadata[ token ].removeCallbacks()
            currentReject( new Err.ReqlClientError( helper.makeAtom( response ), this.metadata[ token ].query, response ) )
            if ( typeof this.metadata[ token ].endReject !== 'function' ) {
                // No pending STOP query, we can delete
                delete this.metadata[ token ]
            }
        }
        else if ( typeof this.metadata[ token ].endResolve === 'function' ) {
            currentResolve = this.metadata[ token ].endResolve
            currentReject = this.metadata[ token ].endReject
            this.metadata[ token ].removeEndCallbacks()
            currentReject( new Err.ReqlClientError( helper.makeAtom( response ), this.metadata[ token ].query, response ) )
            delete this.metadata[ token ]
        }
        else if ( token === -1 ) { // This should not happen now since 1.13 took the token out of the query
            error = new Err.ReqlClientError( helper.makeAtom( response ) + '\nClosing all outstanding queries...' )
            this.emit( 'error', error )
            // We don't want a function to yield forever, so we just reject everything
            helper.loopKeys( this.rejectMap, ( rejectMap, key ) => {
                rejectMap[ key ]( error )
            } )
            this.close()
            delete this.metadata[ token ]
        }
    }
    else if ( type === responseTypes.RUNTIME_ERROR ) {
        const errorValue = helper.makeAtom( response )
        // We don't want to release a connection if we just pinged it.
        if ( this.pendingPing === false || ( errorValue !== PING_VALUE ) ) {
            this.emit( 'release' )
            error = new Err.ReqlRuntimeError( errorValue, this.metadata[ token ].query, response )
        }
        else {
            error = new Err.ReqlRuntimeError( errorValue )
        }

        if ( typeof this.metadata[ token ].reject === 'function' ) {
            currentResolve = this.metadata[ token ].resolve
            currentReject = this.metadata[ token ].reject
            this.metadata[ token ].removeCallbacks()
            error.setName( response.e )
            currentReject( error )
            if ( typeof this.metadata[ token ].endReject !== 'function' ) {
                // No pending STOP query, we can delete
                delete this.metadata[ token ]
            }
        }
        else if ( typeof this.metadata[ token ].endResolve === 'function' ) {
            currentResolve = this.metadata[ token ].endResolve
            currentReject = this.metadata[ token ].endReject
            this.metadata[ token ].removeEndCallbacks()
            delete this.metadata[ token ]
        }
    }
    else if ( type === responseTypes.SUCCESS_ATOM ) {
        this.emit( 'release' )
        // this.metadata[token].resolve is always a function
        datum = helper.makeAtom( response, this.metadata[ token ].options )

        if ( ( Array.isArray( datum ) ) &&
            ( ( this.metadata[ token ].options.cursor === true ) || ( ( this.metadata[ token ].options.cursor === undefined ) && ( this.r._options.cursor === true ) ) ) ) {
            cursor = new Cursor( this, token, this.metadata[ token ].options, 'cursor' )
            if ( this.metadata[ token ].options.profile === true ) {
                this.metadata[ token ].resolve( {
                    profile: response.p,
                    result: cursor
                } )
            }
            else {
                this.metadata[ token ].resolve( cursor )
            }

            cursor._push( {
                done: true,
                response: {
                    r: datum
                }
            } )
        }
        else if ( ( Array.isArray( datum ) ) &&
            ( ( this.metadata[ token ].options.stream === true || this.r._options.stream === true ) ) ) {
            cursor = new Cursor( this, token, this.metadata[ token ].options, 'cursor' )
            stream = new ReadableStream( {}, cursor )
            if ( this.metadata[ token ].options.profile === true ) {
                this.metadata[ token ].resolve( {
                    profile: response.p,
                    result: stream
                } )
            }
            else {
                this.metadata[ token ].resolve( stream )
            }
            cursor._push( {
                done: true,
                response: {
                    r: datum
                }
            } )
        }
        else {
            if ( this.metadata[ token ].options.profile === true ) {
                result = {
                    profile: response.p,
                    result: cursor || datum
                }
            }
            else {
                result = datum
            }
            this.metadata[ token ].resolve( result )
        }

        delete this.metadata[ token ]
    }
    else if ( type === responseTypes.SUCCESS_PARTIAL ) {
        // We save the current resolve function because we are going to call cursor._fetch before resuming the user's yield
        if ( typeof this.metadata[ token ].resolve !== 'function' ) {
            // According to issues/190, we can get a SUCESS_COMPLETE followed by a
            // SUCCESS_PARTIAL when closing an feed. So resolve/reject will be undefined
            // in this case.
            currentResolve = this.metadata[ token ].endResolve
            currentReject = this.metadata[ token ].endReject
            if ( typeof currentResolve === 'function' ) {
                done = true
            }
        }
        else {
            currentResolve = this.metadata[ token ].resolve
            currentReject = this.metadata[ token ].reject
        }

        // We need to delete before calling cursor._push
        this.metadata[ token ].removeCallbacks()

        if ( !this.metadata[ token ].cursor ) { // No cursor, let's create one
            this.metadata[ token ].cursor = true

            let typeResult = 'Cursor'
            let includesStates = false
            if ( Array.isArray( response.n ) ) {
                for ( let i = 0; i < response.n.length; i++ ) {
                    if ( response.n[ i ] === protodef.Response.ResponseNote.SEQUENCE_FEED ) {
                        typeResult = 'Feed'
                    }
                    else if ( response.n[ i ] === protodef.Response.ResponseNote.ATOM_FEED ) {
                        typeResult = 'AtomFeed'
                    }
                    else if ( response.n[ i ] === protodef.Response.ResponseNote.ORDER_BY_LIMIT_FEED ) {
                        typeResult = 'OrderByLimitFeed'
                    }
                    else if ( response.n[ i ] === protodef.Response.ResponseNote.UNIONED_FEED ) {
                        typeResult = 'UnionedFeed'
                    }
                    else if ( response.n[ i ] === protodef.Response.ResponseNote.INCLUDES_STATES ) {
                        includesStates = true
                    }
                    else {
                        currentReject( new Err.ReqlDriverError( 'Unknown ResponseNote ' + response.n[ i ] + ', the driver is probably out of date.' ).setOperational() )
                        return
                    }
                }
            }
            cursor = new Cursor( this, token, this.metadata[ token ].options, typeResult )
            if ( includesStates === true ) {
                cursor.setIncludesStates()
            }
            if ( ( cursor.getType() !== 'Cursor' ) && ( this.releaseFeed === true ) ) {
                this.metadata[ token ].released = true
                this.emit( 'release-feed' )
            }
            if ( ( this.metadata[ token ].options.cursor === true ) || ( ( this.metadata[ token ].options.cursor === undefined ) && ( this.r._options.cursor === true ) ) ) {
                // Return a cursor
                if ( this.metadata[ token ].options.profile === true ) {
                    currentResolve( {
                        profile: response.p,
                        result: cursor
                    } )
                }
                else {
                    currentResolve( cursor )
                }
            }
            else if ( ( this.metadata[ token ].options.stream === true || this.r._options.stream === true ) ) {
                stream = new ReadableStream( {}, cursor )
                if ( this.metadata[ token ].options.profile === true ) {
                    currentResolve( {
                        profile: response.p,
                        result: stream
                    } )
                }
                else {
                    currentResolve( stream )
                }
            }
            else if ( typeResult !== 'Cursor' ) {
                // Return a feed
                if ( this.metadata[ token ].options.profile === true ) {
                    currentResolve( {
                        profile: response.p,
                        result: cursor
                    } )
                }
                else {
                    currentResolve( cursor )
                }
            }
            else {
                // When we get SUCCESS_SEQUENCE, we will delete this.metadata[token].options
                // So we keep a reference of it here
                options = this.metadata[ token ].options

                // Fetch everything and return an array
                cursor.toArray().then( function( result ) {
                    if ( options.profile === true ) {
                        currentResolve( {
                            profile: response.p,
                            result: result
                        } )
                    }
                    else {
                        currentResolve( result )
                    }
                } ).catch( currentReject )
            }
            cursor._push( {
                done: false,
                response: response
            } )
        }
        else { // That was a continue query
            currentResolve( {
                done: done,
                response: response
            } )
        }
    }
    else if ( type === responseTypes.SUCCESS_SEQUENCE ) {
        if ( this.metadata[ token ].released === false ) {
            this.emit( 'release' )
        }

        if ( typeof this.metadata[ token ].resolve === 'function' ) {
            currentResolve = this.metadata[ token ].resolve
            currentReject = this.metadata[ token ].reject
            this.metadata[ token ].removeCallbacks()
        }
        else if ( typeof this.metadata[ token ].endResolve === 'function' ) {
            currentResolve = this.metadata[ token ].endResolve
            currentReject = this.metadata[ token ].endReject
            this.metadata[ token ].removeEndCallbacks()
        }

        if ( !this.metadata[ token ].cursor ) { // No cursor, let's create one
            cursor = new Cursor( this, token, this.metadata[ token ].options, 'Cursor' )

            if ( ( this.metadata[ token ].options.cursor === true ) || ( ( this.metadata[ token ].options.cursor === undefined ) && ( this.r._options.cursor === true ) ) ) {
                if ( this.metadata[ token ].options.profile === true ) {
                    currentResolve( {
                        profile: response.p,
                        result: cursor
                    } )
                }
                else {
                    currentResolve( cursor )
                }

                // We need to keep the options in the else statement, so we clean it inside the if/else blocks
                if ( typeof this.metadata[ token ].endResolve !== 'function' ) {
                    delete this.metadata[ token ]
                }
            }
            else if ( ( this.metadata[ token ].options.stream === true || this.r._options.stream === true ) ) {
                stream = new ReadableStream( {}, cursor )
                if ( this.metadata[ token ].options.profile === true ) {
                    currentResolve( {
                        profile: response.p,
                        result: stream
                    } )
                }
                else {
                    currentResolve( stream )
                }

                // We need to keep the options in the else statement,
                // so we clean it inside the if/else blocks (the one looking
                // if a cursor was already created)
                if ( typeof this.metadata[ token ].endResolve !== 'function' ) {
                    // We do not want to delete the metadata if there is an END query waiting
                    delete this.metadata[ token ]
                }
            }
            else {
                cursor.toArray().then( result => {
                    if ( this.metadata[ token ].options.profile === true ) {
                        currentResolve( {
                            profile: response.p,
                            result: result
                        } )
                    }
                    else {
                        currentResolve( result )
                    }
                    if ( typeof this.metadata[ token ].endResolve !== 'function' ) {
                        delete this.metadata[ token ]
                    }
                } ).catch( currentReject )
            }
            done = true
            cursor._push( {
                done: true,
                response: response
            } )
        }
        else { // That was a continue query
            // If there is a pending STOP query we do not want to close the cursor yet
            done = true
            if ( typeof this.metadata[ token ].endResolve === 'function' ) {
                done = false
            }
            currentResolve( {
                done: done,
                response: response
            } )
        }
    }
    else if ( type === responseTypes.WAIT_COMPLETE ) {
        this.emit( 'release' )
        this.metadata[ token ].resolve()

        delete this.metadata[ token ]
    }
    else if ( type === responseTypes.SERVER_INFO ) {
        this.emit( 'release' )
        datum = helper.makeAtom( response, this.metadata[ token ].options )
        this.metadata[ token ].resolve( datum )
        delete this.metadata[ token ]
    }
}

Connection.prototype.reconnect = function( _options ) {
    const options = _options || {};

    // When `options.connection` is defined, you must create a new socket to reconnect.
    if ( this.options.connection ) {
        throw new Err.ReqlRuntimeError( 'Cannot call `reconnect` if `options.connection` was defined' )
    }

    return new Promise( ( resolve, reject ) => {
        this
            .close( options )
            .then( () => {
                this.r.connect( this.options ).then( resolve ).catch( reject );
            } )
            .catch( reject );
    } );
}

Connection.prototype._send = function( query, token, resolve, reject, originalQuery, options, end ) {
    // console.log('Connection.prototype._send: '+token);
    // console.log(JSON.stringify(query, null, 2));

    if ( this.open === false ) {
        const err = new Err.ReqlDriverError( 'The connection was closed by the other party' )
        err.setOperational()
        reject( err )
        return
    }

    const queryStr = JSON.stringify( query )
    const querySize = Buffer.byteLength( queryStr )

    const buffer = Buffer.alloc( 8 + 4 + querySize )
    buffer.writeUInt32LE( token & 0xFFFFFFFF, 0 )
    buffer.writeUInt32LE( Math.floor( token / 0xFFFFFFFF ), 4 )

    buffer.writeUInt32LE( querySize, 8 )

    buffer.write( queryStr, 12 )

    // noreply instead of noReply because the otpions are translated for the server
    if ( ( !helper.isPlainObject( options ) ) || ( options.noreply !== true ) ) {
        if ( !this.metadata[ token ] ) {
            this.metadata[ token ] = new Metadata( resolve, reject, originalQuery, options )
        }
        else if ( end === true ) {
            this.metadata[ token ].setEnd( resolve, reject )
        }
        else {
            this.metadata[ token ].setCallbacks( resolve, reject )
        }
    }
    else {
        if ( typeof resolve === 'function' ) resolve()
        this.emit( 'release' )
    }

    // This will emit an error if the connection is closed
    try {
        this.connection.write( buffer )
    }
    catch ( err ) {
        this.metadata[ token ].reject( err )
        delete this.metadata[ token ]
    }
}

Connection.prototype._continue = function( token, resolve, reject ) {
    this._send( [ protodef.Query.QueryType.CONTINUE ], token, resolve, reject )
}

Connection.prototype._end = function( token, resolve, reject ) {
    this._send( [ protodef.Query.QueryType.STOP ], token, resolve, reject, undefined, undefined, true )
}

Connection.prototype.use = function( db ) {
    if ( typeof db !== 'string' ) throw new Err.ReqlDriverError( 'First argument of `use` must be a string' )
    this.db = db
}

Connection.prototype.server = function() {
    return new Promise( ( resolve, reject ) => {
        this._send( [ protodef.Query.QueryType.SERVER_INFO ], this._getToken(), resolve, reject, undefined, undefined, true );
    } );
}

// Return the next token and update it.
Connection.prototype._getToken = function() {
    return this.token++
}

Connection.prototype.close = function( _options ) {
    const options = _options || {};

    return new Promise( ( resolve, reject ) => {
        if ( options.noreplyWait === true ) {
            this.noreplyWait()
                .then( r => {
                    this.open = false;
                    this.connection.end();
                    resolve( r );
                } ).catch( reject )
        }
        else {
            this.open = false;
            this.connection.end();
            resolve();
        }
    } );
}

Connection.prototype.noReplyWait = function() {
    throw new Err.ReqlDriverError( 'Did you mean to use `noreplyWait` instead of `noReplyWait`?' )
}

Connection.prototype.noreplyWait = function() {
    const token = this._getToken();

    return new Promise( ( resolve, reject ) => {
        this._send( [ protodef.Query.QueryType.NOREPLY_WAIT ], token, resolve, reject );
    } );
}

Connection.prototype._isConnection = function() {
    return true
}

Connection.prototype._isOpen = function() {
    return this.open
}

Connection.prototype._flush = function() {
    helper.loopKeys( this.metadata, function( metadata, key ) {
        if ( typeof metadata[ key ].reject === 'function' ) {
            metadata[ key ].reject( new Err.ReqlServerError(
                'The connection was closed before the query could be completed.',
                metadata[ key ].query ) )
        }
        if ( typeof metadata[ key ].endReject === 'function' ) {
            metadata[ key ].endReject( new Err.ReqlServerError(
                'The connection was closed before the query could be completed.',
                metadata[ key ].query ) )
        }
    } )
    this.metadata = {}
}

module.exports = Connection
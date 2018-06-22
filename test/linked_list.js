const path = require( 'path' )
const LinkedList = require( path.join( __dirname, '/../lib/linked_list.js' ) )
const assert = require( 'assert' )
const { describe, it } = require( 'mocha' )

describe( 'Linked list', function() {
    const size = 20
    const initSize = 3

    it( 'push and shift', function() {
        const q = new LinkedList()

        for ( let i = 0; i < size; i++ ) {
            q.push( i )
        }
        for ( let i = 0; i < size; i++ ) {
            assert.equal( i, q.shift() )
            assert.equal( q.getLength(), size - 1 - i )
        }
    } )

    it( 'push and pop', function() {
        const q = new LinkedList()
        for ( let i = 0; i < size; i++ ) {
            q.push( i )
        }
        for ( let i = 0; i < size; i++ ) {
            assert.equal( size - 1 - i, q.pop() )
            assert.equal( q.getLength(), size - 1 - i )
        }
    } )

    it( 'unshift and shift', function() {
        const q = new LinkedList()
        for ( let i = 0; i < size; i++ ) {
            q.unshift( i )
        }
        for ( let i = 0; i < size; i++ ) {
            assert.equal( size - 1 - i, q.shift() )
            assert.equal( q.getLength(), size - 1 - i )
        }
    } )

    it( 'unshift and pop', function() {
        const q = new LinkedList()
        for ( let i = 0; i < size; i++ ) {
            q.unshift( i )
        }
        for ( let i = 0; i < size; i++ ) {
            assert.equal( i, q.pop() )
            assert.equal( q.getLength(), size - 1 - i )
        }
    } )

    it( 'a little of everything', function() {
        const q = new LinkedList()
        for ( let i = 0; i < size; i++ ) {
            q.unshift( i )
        }
        for ( let i = 0; i < size; i++ ) {
            q.push( i + 100 )
        }
        for ( let i = 0; i < size - 10; i++ ) {
            assert.equal( size - 1 + 100 - i, q.pop() )
        }
        for ( let i = 0; i < size - 10; i++ ) {
            assert.equal( size - i - 1, q.shift() )
        }

        assert( q.getLength(), size * 2 - 10 - 10 )
    } )

    it( 'push/unshift', function() {
        const q = new LinkedList()
        for ( let i = 0; i < size; i++ ) {
            q.push( i )
            q.unshift( i )
        }
        assert( q.getLength(), size * 2 )
        for ( let i = 0; i < size * 10; i++ ) {
            q.push( -1 )
        }
        assert( q.getLength(), size * ( 2 + 10 ) )
    } )

    it( 'push and shift -- initSize = num push/shift', function() {
        const q = new LinkedList()

        for ( let i = 0; i < initSize; i++ ) {
            q.push( i )
        }
        for ( let i = 0; i < initSize; i++ ) {
            assert.equal( i, q.shift() )
            assert.equal( q.getLength(), initSize - 1 - i )
        }
    } )

    it( 'push and pop -- initSize = num push/shift', function() {
        const q = new LinkedList()
        for ( let i = 0; i < initSize; i++ ) {
            q.push( i )
        }
        for ( let i = 0; i < initSize; i++ ) {
            assert.equal( initSize - 1 - i, q.pop() )
            assert.equal( q.getLength(), initSize - 1 - i )
        }
    } )

    it( 'unshift and shift -- initSize = num push/shift', function() {
        const q = new LinkedList()
        for ( let i = 0; i < initSize; i++ ) {
            q.unshift( i )
        }
        for ( let i = 0; i < initSize; i++ ) {
            assert.equal( initSize - 1 - i, q.shift() )
            assert.equal( q.getLength(), initSize - 1 - i )
        }
    } )

    it( 'unshift and pop -- initSize = num push/shift', function() {
        const q = new LinkedList()
        for ( let i = 0; i < initSize; i++ ) {
            q.unshift( i )
        }
        for ( let i = 0; i < initSize; i++ ) {
            assert.equal( i, q.pop() )
            assert.equal( q.getLength(), initSize - 1 - i )
        }
    } )

    it( 'push and shift -- initSize = num push/shift+1', function() {
        const q = new LinkedList()

        for ( let i = 0; i < initSize + 1; i++ ) {
            q.push( i )
        }
        for ( let i = 0; i < initSize + 1; i++ ) {
            assert.equal( i, q.shift() )
            assert.equal( q.getLength(), initSize + 1 - 1 - i )
        }
    } )

    it( 'push and pop -- initSize = num push/shift+1', function() {
        const q = new LinkedList()
        for ( let i = 0; i < initSize + 1; i++ ) {
            q.push( i )
        }
        for ( let i = 0; i < initSize + 1; i++ ) {
            assert.equal( initSize + 1 - 1 - i, q.pop() )
            assert.equal( q.getLength(), initSize + 1 - 1 - i )
        }
    } )

    it( 'unshift and shift -- initSize = num push/shift+1', function() {
        const q = new LinkedList()
        for ( let i = 0; i < initSize + 1; i++ ) {
            q.unshift( i )
        }
        for ( let i = 0; i < initSize + 1; i++ ) {
            assert.equal( initSize + 1 - 1 - i, q.shift() )
            assert.equal( q.getLength(), initSize + 1 - 1 - i )
        }
    } )

    it( 'unshift and pop -- initSize = num push/shift+1', function() {
        const q = new LinkedList()
        for ( let i = 0; i < initSize + 1; i++ ) {
            q.unshift( i )
        }
        for ( let i = 0; i < initSize + 1; i++ ) {
            assert.equal( i, q.pop() )
            assert.equal( q.getLength(), initSize + 1 - 1 - i )
        }
    } )

    it( 'a little of everything', function() {
        const q = new LinkedList()
        for ( let i = 0; i < size; i++ ) {
            q.unshift( i )
        }
        for ( let i = 0; i < size; i++ ) {
            q.push( i + 100 )
        }
        for ( let i = 0; i < size - 10; i++ ) {
            assert.equal( size - 1 + 100 - i, q.pop() )
        }
        for ( let i = 0; i < size - 10; i++ ) {
            assert.equal( size - i - 1, q.shift() )
        }

        assert( q.getLength(), size * 2 - 10 - 10 )
    } )

    it( 'shift returns undefined if no element', function() {
        const q = new LinkedList()
        assert.equal( q.shift(), undefined )
        assert.equal( q.getLength(), 0 )
    } )

    it( 'pop returns undefined if no element', function() {
        const q = new LinkedList()
        assert.equal( q.pop(), undefined )
        assert.equal( q.getLength(), 0 )
    } )
} )
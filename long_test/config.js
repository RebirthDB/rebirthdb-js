module.exports = {
    debug: false,
    cmd: 'rethinkdb',
    dataDir: 'rebirthdb-js_datadir',
    initialOffset: 180,
    feedQuery: 'r.db("rethinkdb")\n .table("server_status")\n .union(["feedSeparator"])\n .union(r.db("rethinkdb").table("server_status").changes())',
    numServers: 3,
    buffer: 12,
    max: 36
}
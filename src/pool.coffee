_      = require 'lodash'
assert = require 'assert'
amqp   = require 'amqp'
Q      = require 'q'
logger = require('dl-logger')("amqp-vent:pool")


# Very simple hashing function as xor of all bytes
hash = (key) ->
    h = 0
    for i in [0..(key.length)]
        h = h ^ key.charCodeAt(0)
    h


class Pool

    constructor: (servers, options) ->
        servers = [servers] if not _.isArray(servers)
        assert servers.length isnt 0, "need at least one server"

        @size = servers.length
        @_pool = ({url: s} for s in servers)

    when_connection: (options)->
        choosen = @_pool[@_choose_partition(options)]
        choosen.connection ?= @_create_connection(choosen.url)

    _choose_partition: (options) ->
        if @size is 1
            return 0

        unless options?.group?
            return Math.floor(Math.random() * @size)

        return hash(options.group) % @size

    _create_connection: (server) ->
        conn_deferred = Q.defer()
        settings = {url: server}
        conn = amqp.createConnection(settings,
                                    @amqp_options,
                                    conn_deferred.resolve)
        conn.on 'error', (err) ->
            logger.error({err}, "amqp connection error", {server})
            conn_deferred.reject(err)

        conn.on 'ready', ->
            logger.info 'amqp connection ready'

        conn.on 'heartbeat', ->
            logger.debug 'amqp connection heartbeat'

        conn.on 'close', ->
            logger.info 'amqp connection closed'

        conn_deferred.promise

    
module.exports = Pool

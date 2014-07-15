debug = require('debug')("amqp-vent")
amqp = require('amqp')
Q = require('q')
uuid = require('node-uuid')
assert = require('assert')

# use a shared connection per service
connection = null

class Vent

    constructor: (@setup) ->
        assert(@setup, "missing setup options")
        assert(@setup.server, "missing server option")

        @setup.default_channel ?= "firehose"

        @amqp_options =
            reconnect: @setup.reconnect || true
            reconnectBackoffStrategy: @setup.backoff_strategy || "linear"
            reconnectBackoffTime: @setup.backoff_time || 500
            defaultExchangeName: "firehose"

        @_queues = {}
        @_exchanges = {}

    publish: (details) ->
        return cb(new Error("a topic is required for publish")) unless details.topic
        return cb(new Error("a message is required for pulish")) unless details.message

        details.channel ?= @setup._default_channel

        config =
            exchange: details.channel
            routing_key: details.topic

        debug("publish message")
        @_when_exchange(config)
        .then((exchange) ->
            exchange.publish(config.routing_key, details.message)
        )

        @

    subscribe: (details, cb) ->
        ###

            TODO: add support for ack|shift|reject|requeue
        ###
        return cb(new Error("a topic is required for subscribe")) unless details.topic

        details.channel ?= @setup.default_channel
        details.group ?= uuid.v4()

        config =
            exchange: details.channel
            binding_key: details.topic
            queue:  "#{details.channel}:#{details.topic}:#{details.group}"

        debug("subscribe to topic")
        @_when_queue(config)
        .then((queue) ->
            queue.subscribe(cb)
        )

        @

    subscribe_stream: (details, cb) ->
        throw new Error("Not Yet Implemented")

    _when_queue: (config) ->
        unless config.queue of @_queues
            @_queues[config.queue] = @_create_queue(config)

        @_queues[config.queue]

    _create_queue: (config) ->
        debug("create queue: %s", config.queue)
        @_when_connection()
        .then(@_create_queue_instance.bind(@, config))
        .then(@_bind_queue_exchange.bind(@, config))

    _create_queue_instance: (config, connection) ->
        debug("create queue instance: %s", config.queue)
        options =
            durable: true
            autoDelete: true

        queue_deferred = Q.defer()
        connection.queue(config.queue, options, queue_deferred.resolve)
        queue_deferred.promise

    _bind_queue_exchange: (config, queue) ->
        debug("bind queue to exchange: %s", config.binding_key)
        @_when_exchange(config)
        .then((exchange) ->
            debug("exchange ready, binding")
            bound_queue_deferred = Q.defer()
            queue.bind(exchange, config.binding_key)
            queue.on('queueBindOk', ->
                debug("queue and exchange bound")
                bound_queue_deferred.resolve(queue))
            bound_queue_deferred.promise
        )

    _when_connection: ->
        connection ?= @_create_connection()
        connection

    _create_connection: ->
        # fine to log the connection string,
        # since debug is only run in dev
        debug("creating queue connection")

        conn_deferred = Q.defer()
        amqp.createConnection({url: @setup.server}, @amqp_options, conn_deferred.resolve)
        .on('error', (err) ->
                debug(err)
                conn_deferred.reject(err)
            )
        conn_deferred.promise

    _when_exchange: (config) ->
        unless config.exchange of @_exchanges
            @_exchanges[config.exchange] = @_create_exchange(config)

        @_exchanges[config.exchange]

    _create_exchange: (config) ->
        debug("create exchange: %s", config.exchange)

        exch_deferred = Q.defer()
        @_when_connection().then((connection) ->
            options =
                type: 'topic'
                durable: true
                autoDelete: true

            connection.exchange(config.exchange, options, exch_deferred.resolve)
        )
        exch_deferred.promise

exports.Vent = (options) ->
    if typeof(options) is "string"
        options = {server: options}

    new Vent(options)
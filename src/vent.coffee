assert = require('assert')
amqp = require('amqp')
debug = require('debug')
Q = require('q')
{Readable} = require('stream')
uuid = require('node-uuid')

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

    publish: (details, message) ->
        throw new Error("publish details missing") unless details

        details = @_decode_details(details)
        details.message = message if message

        throw new Error("a topic is required for publish") unless details.topic
        throw new Error("a message is required for publish") unless details.message

        details.channel ?= @setup._default_channel

        config =
            exchange: details.channel
            routing_key: details.topic

        debug("publish message %j", {details, message})
        @_when_exchange(config)
        .then((exchange) ->
            exchange.publish(config.routing_key, details.message)
        )
        .fail((err) ->
                console.error(err)
                cb(err)
            )
        @

    subscribe: (details, group, cb) ->
        throw new Error("subscribe details missing") unless details
        unless cb
            cb = group
            group = null

        details = @_decode_details(details)
        details.group = group if group

        throw new Error("a topic is required for subscribe") unless details.topic
        throw new Error("subscribe callback missing") unless typeof(cb) is "function"

        config = @_get_subscriber_config(details)

        debug("subscribe to topic %j", details)
        @_when_queue(config)
        .then((queue) ->
            queue.subscribe(cb)
        )
        .fail((err) ->
            console.error(err)
            cb(err)
        )

        @

    subscribe_stream: (details, group, cb) ->
        throw new Error("subscribe details missing") unless details
        unless cb
            cb = group
            group = null

        details = @_decode_details(details)
        details.group = group if group

        throw new Error("a topic is required for subscribe") unless details.topic
        throw new Error("subscribe callback missing") unless typeof(cb) is "function"

        config = @_get_subscriber_config(details)

        debug("stream subscribe to topic %j", details)
        @_when_queue(config)
        .then((queue) ->
            cb(null, new QueueStream(queue))
        )
        .fail((err) ->
            console.error(err)
            cb(err)
        )

        @

    _decode_details: (details) ->
        return details unless typeof(details) is 'string'

        decoded = details.split(':')
        switch decoded.length
            when 1 then {topic: decoded[0]}
            when 2 then {channel: decoded[0], topic: decoded[1]}
            when 3 then {channel: decoded[0], topic: decoded[1], group: decoded[2]}
            else {}

    _get_subscriber_config: (details) ->
        details.channel ?= @setup.default_channel
        details.group ?= uuid.v4()

        config =
            exchange: details.channel
            binding_key: details.topic
            queue:  "#{details.channel}:#{details.topic}:#{details.group}"

        config

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
            console.error(err)
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
                autoDelete: true

            connection.exchange(config.exchange, options, exch_deferred.resolve)
        )
        exch_deferred.promise

exports.Vent = (options) ->
    if typeof(options) is "string"
        options = {server: options}

    new Vent(options)

class QueueStream extends Readable

    constructor: (@queue)->
        options = {objectMode: true}
        Readable.call(this, options)
        @paused = true
        queue.subscribe(@_on_message.bind(@))

    _on_message: (msg) ->
        if @paused
            # we are backed up, drop message
            console.log("stream backed up. Increase stream 'highWaterMark', or start more processors")
        else
            continue_reading = @push(msg)
            @paused = not continue_reading


    _read: ->
        @paused = false
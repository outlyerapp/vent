_               = require 'lodash'
assert          = require 'assert'
amqp            = require 'amqp'
{EventEmitter}  = require 'events'
logger          = require('dl-logger')("dl:jobs")
Q               = require 'q'
{Readable}      = require 'stream'
uuid            = require 'node-uuid'

# use a shared connection per service
connection = null

class Jobs extends EventEmitter
    """
    Jobs a general purpose Pub/Sub event lib hased on AMQP

    You can subscribe or publish event based on the following concepts
    ## Channels
    A channel is used to group common types of event. This maps to a EXCHANGE
    on both publish and subscribe

    ## Topics
    A topic is the event name to be used. This maps to a ROUTING KEY, because
    of this is supports the same wildcard features of AMQP exchanges.
    See: https://www.rabbitmq.com/tutorials/tutorial-four-python.html

    ## Groups
    A group is useful when subscribing to events. Subscribers with the same
    group name will have (matching) events distributed amongst them. If you
    do NOT specify a group name a UUID will be used. That subscriber will then
    recieve all (matching) events
    When working with a distributed architure you will have several instances
    of the same worker/service. In some cases you will only want one service
    to handle a particular event. This is were groups are useful

    ## options
    durable - is a non-transient setup, meaning that queues and thier content
              will be persisted through restarts
    group   - SEE ABOVE
    """

    constructor: (@setup, options) ->
        assert _.isObject(@setup), "missing setup options"
        assert _.isString(@setup.server), "missing server option"

        default_options =
            channel: "vent"
            durable: false
        @options = _.extend(default_options, options)

        @amqp_options =
            reconnect: @setup.reconnect || true
            reconnectBackoffStrategy: @setup.backoff_strategy || "linear"
            reconnectBackoffTime: @setup.backoff_time || 500
            defaultExchangeName: @options.channel

        @_queues = {}
        @_exchanges = {}

    publish: (event, payload, cb) ->
        """
        publish to event on the specified channel and topic

        "<channel>:<topic>" = event
        """
        assert(event, "event required")
        assert(payload, "payload required")

        event_options = @_parse_event(event)
        pub_options = _.extend({}, @options, event_options)

        cb ?= (err, result) ->
            logger.error({err}, "publishing") if err

        logger.trace("publish message", {@options, pub_options, payload})
        @_when_exchange(pub_options)
            .then (exchange) ->
                exchange.publish(pub_options.topic, payload, {}, cb)

            .fail cb
        @

    subscribe: (event, options, listener) ->
        """
        subscribe to events on the specified channel and topic

        "<channel>:<topic>" = event
        {group, durable} = options
        """
        unless listener
            listener = override_options
            override_options = {}

        assert _.isString(event), "event required"
        assert _.isFunction(listener), "listener required"

        event_options = @_parse_event(event)

        # Combine options ordered by scope
        sub_options = _.extend({}, @options, event_options, options)
        sub_options.group ?= uuid.v4()

        logger.trace("subscribe to topic", {options})
        @_when_queue(sub_options)
            .then (queue) =>
                queue.subscribe(listener)
                @emit('bound', {queue})

            .fail (err) ->
                logger.error({err}, "subscribing") if err
                @emit('error', err)
        @

    subscribe_stream: (details, group, cb) ->
        # throw new Error("subscribe details missing") unless details
        # unless cb
        #    cb = group
        #    group = null
        #
        # details = @_decode_details(details)
        # details.group = group if group
        #
        # throw new Error("a topic is required for subscribe") unless details.topic
        # throw new Error("subscribe callback missing") unless typeof(cb) is "function"
        #
        # config = @_get_subscriber_config(details)
        #
        # logger.trace("stream subscribe to topic %j", details)
        # @_when_queue(config)
        # .then((queue) ->
        #        cb(null, new QueueStream(queue))
        #    )
        # .fail((err) ->
        #        console.error(err)
        #        cb(err)
        #    )

        @

    _parse_event: (event) ->
        assert _.isString(event), "event string required"
        decoded = event.split(':')
        result = {}
        switch decoded.length
            when 1
                result = {topic: decoded[0]}
            when 2
                result = {channel: decoded[0], topic: decoded[1]}
            when 3
                result =
                    channel: decoded[0]
                    topic:   decoded[1]
                    group:   decoded[2]

        assert(result.topic, "topic required")
        result

    _when_queue: (options) ->
        queue_name = @_generate_queue_name(options)
        unless queue_name of @_queues
            @_queues[queue_name] = @_create_queue(options)

        @_queues[queue_name]

    _generate_queue_name: (options) ->
        assert _.isString(options.channel), "channel required"
        assert _.isString(options.topic),   "topic required"
        assert _.isString(options.group),   "group required"

        "#{options.channel}:#{options.topic}:#{options.group}"

    _create_queue: (options) ->
        logger.trace("create queue", {options})
        @_when_connection()
            .then @_create_queue_instance.bind(@, options)
            .then @_create_exchange_bind.bind(@, options)

    _create_queue_instance: (options, connection) ->
        assert _.isBoolean(options.durable), "boolean durable option required"
        assert _.isObject(connection), "connection required"

        queue_name = @_generate_queue_name(options)
        queue_opts = {autoDelete: not options.durable, durable: options.durable}

        queue_deferred = Q.defer()
        logger.trace("create queue instance", {queue_name, queue_opts})
        connection.queue(queue_name, queue_opts, queue_deferred.resolve)
        queue_deferred.promise

    _create_exchange_bind: (options, queue) ->
        @_when_exchange(options)
            .then @_bind_queue.bind(@, queue, options)

    _bind_queue: (queue, options, exchange) ->
        assert _.isObject(queue), "queue required"
        assert _.isObject(exchange), "exchange required"
        assert _.isString(options.topic), "topic option require"

        binding_key = options.topic
        bound_queue_deferred = Q.defer()

        logger.trace("binding queue to exchange", {binding_key})
        queue.bind(exchange, binding_key)
        queue.on 'queueBindOk', ->
            logger.trace("queue and exchange bound", {binding_key})
            bound_queue_deferred.resolve(queue)

        bound_queue_deferred.promise

    _when_connection: ->
        connection ?= @_create_connection()
        connection

    _create_connection: ->
        logger.trace("creating queue connection")

        conn_deferred = Q.defer()
        settings = {url: @setup.server}
        amqp.createConnection(settings, @amqp_options, conn_deferred.resolve)
            .on 'error', (err) ->
                logger.error({err}, "create connection")
                conn_deferred.reject(err)

        conn_deferred.promise

    _when_exchange: (options) ->
        assert _.isString(options.channel), "channel required"

        unless options.channel of @_exchanges
            @_exchanges[options.channel] = @_create_exchange(options)

        @_exchanges[options.channel]

    _create_exchange: (options) ->
        @_when_connection()
            .then @_create_exchange_instance.bind(@, options)

    _create_exchange_instance: (options, connection) ->
        assert _.isString(options.channel), "boolean channel option required"

        logger.trace("create exchange: %s", options.exchange)

        exch_deferred = Q.defer()
        exch_name = options.channel
        exch_options =
            type: 'topic'
            autoDelete: not options.durable,
            durable: options.durable

        logger.trace("create exchange instance", {exch_name, exch_options})
        connection.exchange(options.channel, options, exch_deferred.resolve)

        exch_deferred.promise

module.exports = (setup, options) ->
    setup = {server: setup} if _.isString(setup)
    new Jobs(setup, options)

class QueueStream extends Readable

    constructor: (@queue)->
        options = {objectMode: true}
        Readable.call(this, options)
        @paused = true
        queue.subscribe(@_on_message.bind(@))

    _on_message: (msg) ->
        if @paused
            # we are backed up, drop message
            msg = "stream backed up. Increase stream 'highWaterMark',
                or start more processors"
            logger.warn(msg)
        else
            continue_reading = @push(msg)
            @paused = not continue_reading


    _read: ->
        @paused = false

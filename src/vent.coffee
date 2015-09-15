os              = require 'os'
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

class Vent extends EventEmitter
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

        @connection = null
        @conn_count = 0
        @_queues = {}
        @_exchanges = {}
        @_auto_purge = []
        @_subscribed_queues = {}

        process.on 'SIGINT', @_cleanup

    _cleanup: =>
        logger.info('cleaning up purges queues', @_auto_purge.length)
        for item in @_auto_purge
            logger.info('destroying queue', item?.name)
            item?.destroy()

    publish: (event, payload, options, cb) ->
        """
        publish to event on the specified channel and topic

        "<channel>:<topic>" = event
        """
        assert(event, "event required")
        assert(payload, "payload required")

        if _.isFunction(options)
            cb = options
            options = {}

        event_options = @_parse_event(event)
        pub_options = _.extend({}, @options, event_options, options)

        _cb = (errors) ->
            return unless cb
            if errors then cb(new Error('message publish fail')) else cb()

        logger.trace("publish message", {@options, pub_options, payload})
        @_when_exchange(pub_options)
            .then (exchange) ->
                exchange.publish(pub_options.topic, payload, {}, _cb)

            .fail cb
        @

    subscribe: (event, options, listener) ->
        """
        subscribe to events on the specified channel and topic

        "<channel>:<topic>" = event
        {group, durable} = options
        or
        "<group_name>" = options
        """
        unless listener
            listener = options
            options = {}

        if _.isString(options)
            options = {group: options}

        assert _.isString(event), "event required"
        assert _.isFunction(listener), "listener required"

        event_options = @_parse_event(event)

        # Combine options ordered by scope
        sub_options = _.extend({}, @options, event_options, options)
        auto_purge = sub_options.auto_purge and not sub_options.group?
        sub_options.group ?= uuid.v4()

        logger.trace("subscribe to topic", {options})
        @_when_queue(sub_options)
            .then (queue) =>
                queue.subscribe(listener)
                    .addCallback((ok) =>
                        ctag = ok.consumerTag
                        @_remember_subscription(event, listener, {queue, auto_purge, ctag})
                    )
                @emit('bound', {queue})

            .fail (err) ->
                logger.error({err}, "subscribing") if err
                @emit('error', err)
        @

    _remember_subscription: (event, listener, options) ->
        unless @_subscribed_queues[event]
            @_subscribed_queues[event] = []
        @_subscribed_queues[event].push([listener, options])

    unsubscribe: (event, listener) ->
        return unless @_subscribed_queues[event]

        unsubscribe = (options) =>
            {queue, auto_purge, ctag} = options
            queue.unsubscribe(ctag)
            # If the queue is supposed to be auto_pruged, lets clean it up
            if auto_purge
                auto_purge_idx = @_auto_purge.indexOf()
                if auto_purge_idx >= 0
                    @_auto_purge.splice(auto_purge_idx, 1)
                delete @_queues[queue.name] if @_queues[queue.name]?
                queue.destroy()

        @_subscribed_queues[event] = @_subscribed_queues[event].filter (tuple) ->
            [l, options] = tuple
            if l is listener
                unsubscribe(options)
                return false
            true
        @

    subscribe_stream: (event, options, cb) ->
        """
        subscribe to a stream of events on the specified channel and topic

        "<channel>:<topic>" = event
        {group, durable} = options
        or
        "<group_name>" = options
        """
        unless listener
            listener = override_options
            override_options = {}

        if _.isString(options)
            options = {group: options}

        assert _.isString(event), "event required"
        assert _.isFunction(cb), "completion required"

        event_options = @_parse_event(event)

        # Combine options ordered by scope
        sub_options = _.extend({}, @options, event_options, options)
        sub_options.group ?= uuid.v4()

        logger.trace("subscribe to topic stream", {options})
        @_when_queue(sub_options)
            .then (queue) =>
                cb(null, new QueueStream(queue, options))
                @emit('bound', {queue})

            .fail (err) ->
                logger.error({err}, "subscribing to stream") if err
                @emit('error', err)

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

        name = "#{options.channel}:#{options.topic}:#{options.group}"
        if options.partition?
            partition_key = 0
            for l in "#{os.hostname()}:#{process.env.PORT || '0'}"
                partition_key ^= l.charCodeAt(0)
            name += ":p#{partition_key % 4}"
        name

    _create_queue: (options) ->
        logger.trace("create queue", {options})
        @_when_connection()
            .then @_create_queue_instance.bind(@, options)
            .then @_create_exchange_bind.bind(@, options)

    _create_queue_instance: (options, connection) ->
        assert _.isBoolean(options.durable), "boolean durable option required"
        assert _.isObject(connection), "connection required"

        queue_name = @_generate_queue_name(options)

        args = options.args or {}
        if options.ttl?
            args['x-message-ttl'] = options.ttl


        auto_delete = not options.durable
        if options.auto_delete?
            auto_delete = options.auto_delete

        queue_opts =
            autoDelete: auto_delete
            durable: options.durable
            'arguments': args

        queue_deferred = Q.defer()
        logger.trace("create queue instance", {queue_name, queue_opts})
        connection.queue queue_name, queue_opts, (queue) =>
            if options.auto_purge
                logger.debug('list queue for auto_purge', queue.name)
                @_auto_purge.push(queue)
            queue_deferred.resolve(queue)

        queue_deferred.promise

    _create_exchange_bind: (options, queue) ->
        @_when_exchange(options)
            .then @_bind_queue.bind(@, queue, options)

    _bind_queue: (queue, options, exchange) ->
        assert _.isObject(queue), "queue required"
        assert _.isObject(exchange), "exchange required"
        assert _.isString(options.topic), "topic option require"

        binding_key = if options.partition then "10" else options.topic
        bound_queue_deferred = Q.defer()

        logger.trace("binding queue to exchange", {queue: queue.name, exchange: exchange.name, binding_key})
        queue.bind(exchange, binding_key)
        queue.on 'queueBindOk', ->
            logger.trace("queue and exchange bound", {binding_key})
            bound_queue_deferred.resolve(queue)

        bound_queue_deferred.promise

    _when_connection: ->
        @connection ?= @_create_connection()
        @connection

    _create_connection: ->
        conn_deferred = Q.defer()
        settings = {url: @setup.server}
        @conn_count++
        logger.debug("creating queue connection", {settings, @conn_count})
        conn = amqp.createConnection(settings,
                                    @amqp_options,
                                    conn_deferred.resolve)
        conn.on 'error', (err) ->
            logger.error({err}, "amqp connection error")
            conn_deferred.reject(err)

        conn.on 'ready', ->
            logger.info 'amqp connection ready'

        conn.on  'heartbeat', ->
            logger.debug 'amqp connection heartbeat'

        conn.on 'close', ->
            logger.info 'amqp connection closed'

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
        promise = exch_deferred.promise
        exch_name = options.channel
        exch_options =
            type: options.type or 'topic'
            autoDelete: false
            durable: options.durable

        if options.partition?
            exch_name = "#{exch_name}.#{options.group}-splitter"
            _.extend(exch_options,
                type: 'x-consistent-hash'
                autoDelete: true
            )
            exchange = null
            promise = promise.then @_create_bound_exchange.bind(@, options, connection)

        logger.trace("create exchange instance", {exch_name, exch_options})
        connection.exchange(exch_name, exch_options, exch_deferred.resolve)
        promise

    _create_bound_exchange: (options, connection, exchange) ->
        @_create_exchange_instance(_.omit(options, 'partition'), connection)
            .then @_bind_exchange.bind(@, exchange, options)

    _bind_exchange: (exchange, options, source) ->
        assert _.isString(options.topic), "topic option require"

        binding_key = options.topic
        exchange_deferred = Q.defer()

        logger.trace("binding exchange to other exchange", {exchange: exchange.name, source: source.name, binding_key})
        exchange.bind(source, binding_key, ->
            exchange_deferred.resolve(exchange)
        )
        exchange_deferred.promise


module.exports = (setup, options) ->
    setup = {server: setup} if _.isString(setup)
    new Vent(setup, options)

class QueueStream extends Readable

    constructor: (@queue, options)->
        highWaterMark = options.high_watermark or 16
        super({objectMode: true, highWaterMark})

        # enable ack so the stream can request messages when its ready
        sub_options =
            ack: true
            prefetchCount: options.prefetch or 1

        @queue.subscribe(sub_options, @_on_message.bind(@))

    _on_message: (msg) =>
        @push(msg)

    _read: ->
        @queue.shift()

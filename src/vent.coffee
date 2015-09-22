os              = require 'os'
assert          = require 'assert'
{EventEmitter}  = require 'events'
_               = require 'lodash'
amqp            = require 'amqplib'
logger          = require('dl-logger')("dl:vent")
uuid            = require 'node-uuid'
w               = require 'when'


DEFAULT_OPTIONS =
    channel: 'vent'
    reconnect: true
    heartbeat: 5
    durable: false


class Vent extends EventEmitter
    ###
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
    ###

    constructor: (@url, options) ->
        assert _.isString(@url), "missing url"
        @options = _.extend({}, DEFAULT_OPTIONS, options)
        @_reset_connection()

    publish: (event, payload, options, cb) ->
        ###
        TODO: need to fix it after switch to new aqmplib

        publish to event on the specified channel and topic

        "<channel>:<topic>" = event
        ###
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
        ###
        subscribe to events on the specified channel and topic

        "<channel>:<topic>" = event
        {group, durable} = options
        or
        "<group_name>" = options

        you can subscripe with 'ack' option. In that case listener will be
        called with callback functions as second argument. You are supposed
        to call that callback when processing is done
        ###
        unless listener
            listener = options
            options = {}

        if _.isString(options)
            options = {group: options}

        assert _.isString(event), "event required"
        assert _.isFunction(listener), "listener required"

        event_options = @_parse_event(event)
        options = _.extend({}, @options, event_options, options)
        options.auto_delete = false if options.group?
        options.group ?= uuid.v4()

        @_create_subscription_channel(options, listener)
        @

    unsubscribe: (event, options, listener) ->
        ## TODO: need brand new implementation
        return unless @_subscribed_queues[event]

        unsubscribe = (options) =>
            {queue, ctag} = options
            queue.unsubscribe(ctag)

        @_subscribed_queues[event] = @_subscribed_queues[event].filter (tuple) ->
            [l, options] = tuple
            if l is listener
                unsubscribe(options)
                return false
            true
        @

    subscribe_stream: (event, options, cb) ->
        ###
        Createa stream wubscribed to all of events on the specified channel and topic

        Options are the same as for @subscribe method, plus optional high_watermark
        option for created stream.

        Stream subscriptions are always using acknowledgments to manage flow.
        ###
        unless cb
            cb = options
            options = {}

        if _.isString(options)
            options = {group: options}

        stream = new vent_stream.ConsumerStream(_.pick(options, high_watermark)

        @subscribe(event, _.extend({}, options, ack: true), stream.push_message)
        @cb(null, stream)
        @

    #
    # Connection and channel handling
    # -------------------------------
    #
    # In case of connection error, order of events is as follows:
    #  * conenction error
    #  * channel closed
    #  * connection closed
    #
    # On connection error we will reset connection. When it comes to conneciton
    # closed handler, we can figgure out if vent is closed depending whether any
    # subscription was reconneted. Potential subscription reconnect logic will
    # kick in on channel closed event.
    
    _open_connection: =>
        ###
        Open new connection

        It is subject to _.memoize, so it will be called only once per connection
        ###
        conn_options = _.pick(@options, 'hearbeat')
        logger.debug("creating queue connection", {url, conn_options})
        amqp.connect(@url, con_options).then (conn) =>
            logger.debug('amqp connection created', {conn})
            conn.on 'error', (err) ->
                    logger.error("Connection error", {err, url})
                    @emit('error', err)
                    @_reset_connection()
                .on 'close', ->
                    logger.info('Connection closed', {url})
                    if not @_connect.has()
                        @emit('close')
                .on 'blocked', ->
                    logger.warn('Connection blocked', {url})
                .on 'unblocked', ->
                    logger.info('Connection unblocked', {url})

    _reset_connection: ->
        logger.debug('connection reset')
        @_connect = _.memoize(@_open_connection)
        @_subscriptions = []

    _create_channel: ->
        @_connect().then (conn) ->
            conn.createChannel()

    _create_subscription_channel: (options, listener) ->
        @_create_channel().then (ch) =>
            queue_name = @_generate_queue_name(options)
            queue_options = @_generate_queue_options(options)
            exch_name = queue_binding_source = options.channel
            exch_options = @_generate_exchange_options(options)
            topic = options.topic
            consumer = @_wrap_consumer_callback(listener)
            if options.ack
                consumer = @_wrap_ack_callback(listener, channel)

            logger.debug('setting up queue', {queue_name, queue_options, exch_name, topic})
            steps = [
                ch.assertQueue(queue_name, queue_options)
                ch.assertExchange(exch_name, exch_options)
            ]

            if options.partition?
                partition_exch_name = "#{exch_name}.#{options.group}-splitter"
                partition_exch_options =
                    type: 'x-consistent-hash'
                    autoDelete: if options.autoDelete? then options.autoDelete else true
                steps.concat([
                    ch.assertExchange(partition_exch_name)
                    ch.bindExchange(partition_exch_name, exch_options, topic)
                ])
                topic = '10' # for x-consistent hash echange topic is a weight

            steps.concat([
                ch.bindQueue(queue_name, queue_binding_source, topic)
                ch.consume(queue_name, listener)
            ])
            w.all(steps)
            # TODO: add channel bindings to restart whole subscription if channel is closed

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

    _generate_queue_name: (options) ->
        assert _.isString(options.channel), "channel required"
        assert _.isString(options.topic),   "topic required"
        assert _.isString(options.group),   "group required"

        name = "#{options.channel}:#{options.topic}:#{options.group}"
        if options.partition?
            partition_key = 0
            for l in "#{os.hostname()}:#{process.env.PORT || '0'}"
                partition_key = (partition_key + l.charCodeAt(0)) % options.partition
            name += ":p#{partition_key}"
        name

    _generate_queue_options: (options) ->
        assert _.isBoolean(options.durable), "boolean durable option required"

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
            arguments: args

    _generate_exchange_options: (options) ->
        exch_options =
            type: options.type or 'topic'
            autoDelete: false
            durable: options.durable

    _wrap_consumer_callback: (fn) ->
        """ Wrapper for unpacking message content """
        (msg) -> fn(msg.content)

    _wrap_ack_callback: (fn, channel) ->
        """ Wrapper that adds ack callback to argumetns"""
        (msg) -> fn(msg, (err) ->
            if err?
                # TODO: Right now there is not much we can do about errors beside
                # jsut dropping mesage one a floor and logging message. In feature
                # version we should have support for configurable errors queue,
                # where errors coudl be forwarded for operator intervention.
                # We defenitelly don't want to re-put into queue, because if it is
                # problem with message itself, we can end up in indefenite loop
                logger.error('Error in message consumer', {err})
            channel.ack(msg)
        )


module.exports = (setup, options) ->
    setup = {url: setup} if _.isString(setup)
    new Vent(setup, options)

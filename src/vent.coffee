os              = require 'os'
assert          = require 'assert'
{EventEmitter}  = require 'events'
_               = require 'lodash'
amqp            = require 'amqplib'
logger          = require('dl-logger')("dl:vent")
uuid            = require 'node-uuid'
when_           = require 'when'

vent_stream     = require './vent_stream'

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

    By default, subscription that use groups will not be auto-deleted.

    ## options
    durable - is a non-transient setup, meaning that queues and thier content
              will be persisted through restarts
    group   - SEE ABOVE

    ## TODO: Handling errors

    We are planning to add support for metric processing errors. You will be able
    to provide error channel, that will be used to publish all messages that caused
    processing error together with error explanation.
    ###

    constructor: ({@url}, options) ->
        assert _.isString(@url), "missing vent url"
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
        options = _.extend({}, @options, event_options, options)

        @_get_publishing_channel()
            .then(@_assert_exchange.bind(@, options))
            .done((channel) =>
                {content, properties} = @_encode_message(payload)
                message_options = _.chain(options)
                    .pick('mandatory', 'persistent', 'deliveryMode', 'expiration', 'CC')
                    .extend(properties)
                    .value()
                if not channel.publish(options.channel, options.topic, content, message_options) and cb?
                    logger.debug('Overloaded')
                    return channel.once('drain', cb)
                cb?()
            , (err) ->
                logger.error({err}, 'Error when trying to open publishing channel')
                cb(err) if cb?
            )
        @

    subscribe: (event, options, listener) ->
        ###
        subscribe to events on the specified channel and topic

        "<channel>:<topic>" = event
        {group, durable} = options
        or
        "<group_name>" = options

        you can subscripe with 'ack' option. In that case listener can return promise.
        Acknowledgemnt will be sent only one promise is resolved. It will return value
        ack will be sent imediately. Still usefull to limit rate.
        ###
        unless listener
            listener = options
            options = {}

        if _.isString(options)
            options = {group: options}

        assert _.isString(event), "event required"
        assert _.isFunction(listener), "listener required"

        event_options = @_parse_event(event)
        subscriptions = @_subscriptions
        options = _.extend({}, @options, event_options, options)
        options.auto_delete = false if not options.auto_delete? and options.group?
        options.group ?= uuid.v4()
        prepare_channel = null

        for [exch_name, handler, ch] in subscriptions
            if exch_name is options.channel and handler is listener
                prepare_channel = ch
                break

        unless prepare_channel
            prepare_channel = @_create_subscription_channel(options, listener)
            subscriptions.push([options.channel, listener, prepare_channel])

        prepare_channel
            .then (ch) =>
                @_subscribe_queue(ch, options)
                logger.debug('Started subscription', {options})
            .catch (err) ->
                logger.error({err}, 'Error when opening new subscription', {options})
        @

    unsubscribe: (event, options, listener) ->
        ### Remove subscription

        Right now subscription is matched by channel name and listener only. You can't remove
        indvidual topics.
        ###
        unless listener
            listener = options
            options = {}

        event_options = @_parse_event(event)
        options = _.extend({}, @options, event_options, options)

        @_subscriptions = @_subscriptions.filter ([exch_name, handler, when_channel]) ->
            if exch_name is options.channel and handler is listener
                when_channel.then ({ch, queue}) -> ch.close()
                false
            true
        @

    subscribe_stream: (event, options, cb) ->
        ###
        Deprecated, Backward compatible asynchronous stream creation
        ###
        unless cb
            cb = options
            options = {}

        cb(null, @tap(event, options))
        @

    tap: (event, options) ->
        ###
        Create tap, a stream subscribed to choosen event types

        It returns readable stream
        ###
        if _.isString(options)
            options = {group: options}

        stream = options.stream
        unless stream? and _.isFunction(stream.push_message)
            stream = new vent_stream.ConsumerStream(_.pick(options, 'high_watermark', 'highWatermark'))
        @subscribe(event, _.extend({}, options, ack: true), stream.push_message)
        stream.on('close', @unsubscribe.bind(@, event, options, stream.push_message))

    close: =>
        # TODO: add vent close method implementation
        logger.warn('Closing vent is not implemented yet!')

    #
    # Connection and channel handling
    # -------------------------------
    #
    # In case of connection error, order of events is as follows:
    #  * connection error
    #  * channel closed
    #  * connection closed
    #  * connection re-connects (if reconnect option is set)

    _open_connection: =>
        ###
        Open new connection

        It is subject to _.memoize, so it will be called only once per connection
        ###
        url = @_get_connection_url()
        emit = @emit.bind(@)
        amqp.connect(url).then (conn) =>
            logger.debug('Opened new amqp connection', {conn})
            conn.on 'error', (err) ->
                    logger.error({err}, "Connection error", {url})

                    # If it is PRECONDITION_FAILED error, it means that there is no point
                    # keeping reconnect. We can only fail whole process and wait for operator
                    # to fix queues.
                    if err.toString().match(/PRECONDITION-FAILED/)
                        return emit('error', err)

                    # All other errors though should make library keep re-connecting
                    # TODO: implement me

                .on 'close', ->
                    logger.info('Connection closed', {url})
                    unless @_connect? and @_connect.has()
                        emit('close')
                .on 'blocked', ->
                    logger.warn('Connection blocked', {url})
                .on 'unblocked', ->
                    logger.info('Connection unblocked', {url})
            conn

    _get_connection_url: =>
        url = @url
        heartbeat = @options.heartbeat
        if heartbeat?
            separator = if url.indexOf('?') >= 0 then '&' else '?'
            url += "#{separator}heartbeat=#{heartbeat}"
        url

    _reset_connection: ->
        @_connect = _.memoize(@_open_connection)
        @_subscriptions = []
        @_pub_channels = null
        @_confirmed_exchanges = {}

    _create_channel: ->
        @_connect().then (conn) ->
            conn.createChannel().then (channel) ->
                channel
                    .on 'error', (err) ->
                        logger.error({err}, 'Channel error')
                    .on 'close', () ->
                        logger.info('Channel closed', {channel})

    _create_publishing_channels: ->
        @_publishing_channels = [@_create_channel()]

    _get_publishing_channel: ->
        available = @_publishing_channels or @_create_publishing_channels()
        idx = 0
        if available.length > 1
            idx = Math.floor(Math.random() * available.length)
        available[idx]

    _create_subscription_channel: (options, listener) =>
        @_create_channel().then (ch) =>
            queue_name = @_generate_queue_name(options)
            queue_options = @_generate_queue_options(options)
            consumer = @_wrap_consumer_callback(listener, ch, options)
            consumer_options = @_generate_consumer_options(options)
            steps = [
                ch.assertQueue(queue_name, queue_options)
                @_assert_exchange(options, ch)
            ]

            if options.prefetch?
                steps.push(ch.prefetch(options.prefetch))

            steps.push(
                ch.consume(queue_name, consumer, consumer_options)
            )

            # TODO: add channel bindings to restart whole subscription if channel is closed
            when_.all(steps)
                .then(-> {ch, queue: queue_name})

    _subscribe_queue: ({ch, queue}, options) ->
        exch_name = queue_binding_source = options.channel
        queue_binding_source = exch_name
        topic = options.topic
        steps = []

        if options.partition?
            partition_exch_name = "#{exch_name}.#{options.group}-splitter"
            partition_exch_options =
                autoDelete: if options.autoDelete? then options.autoDelete else true
            steps.concat([
                ch.assertExchange(partition_exch_name, 'x-consistent-hash', partition_exch_options)
                ch.bindExchange(partition_exch_name, exch_options, topic)
            ])
            topic = '10' # for x-consistent hash echange topic is a weight
            
        steps.push(
            ch.bindQueue(queue, queue_binding_source, topic)
        )

        # TODO: add channel bindings to restart whole subscription if channel is closed
        when_.all(steps)


    # Different actions options parsing
    # ---------------------------------

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
        autoDelete: false
        durable: options.durable

    _generate_consumer_options: (options) ->
        noAck: not options.ack

    # Channel utilities
    # -----------------

    _assert_exchange: (options, channel) ->
        exch_name = options.channel
        exch_options = @_generate_exchange_options(options)
        if @_confirmed_exchanges[exch_name]
            return when_(channel)

        channel.assertExchange(exch_name, 'topic', exch_options).then =>
            @_confirmed_exchanges[exch_name] = 1
            channel

    # Message encoding
    # ----------------

    _encode_message: (payload) ->
        content = null
        if typeof payload is 'string'
            content = payload
            content_type = 'text/plain'
        else
            content = JSON.stringify(payload)
            content_type = 'application/json'
        content = new Buffer(content)
        {content, properties:
            contentType: content_type
        }

    _decode_message: (msg) =>
        content = msg.content
        content_type = msg.properties.contentType
        switch content_type
            when 'application/json'
                try
                    content = JSON.parse(content)
                catch err
                    logger.warn('Error parsing json message', {err, msg})
                    throw err
            when 'text/plain'
                content = content.toString('utf8')
            when undefined
                content = content.toString('utf8')
            else
                loger.warn('Recived message with unknown content_type', {content_type, msg})
                throw new Error("Do not know how to hange message type: " + content_type)
        content

    # Message callbacks
    # -----------------

    _wrap_consumer_callback: (fn, channel, options) ->
        """ Wrapper for unpacking message content """
        decode = @_decode_message
        wrapped = (msg) ->
            when_.try(-> decode(msg)).then(fn)
        if options.ack
            wrapped = @_wrap_ack_callback(wrapped, channel)
        wrapped

    _wrap_ack_callback: (fn, channel) ->
        """ Wrapper that adds ack callback to argumetns"""
        (msg) ->
            when_(fn(msg))
                .catch (err) ->
                    # TODO: Right now there is not much we can do about errors beside
                    # jsut dropping mesage one a floor and logging message. In feature
                    # version we should have support for configurable errors queue,
                    # where errors coudl be forwarded for operator intervention.
                    # We defenitelly don't want to re-put into queue, because if it is
                    # problem with message itself, we can end up in indefenite loop
                    logger.error({err}, 'Error in message consumer')
                .finally ->
                    channel.ack(msg)


module.exports = (setup, options) ->
    setup = {url: setup} if _.isString(setup)
    logger.debug('setup', {setup})
    new Vent(setup, options)

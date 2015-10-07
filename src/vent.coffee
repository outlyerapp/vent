os              = require 'os'
assert          = require 'assert'
{EventEmitter}  = require 'events'
_               = require 'lodash'
amqp            = require 'amqplib'
logger          = require('dl-logger')("dl:vent")
uuid            = require 'node-uuid'
when_           = require 'when'

VentChannel     = require './vent_channel'
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
        @_subscriptions = []
        @_confirmed_exchanges = {}
        @_get_publisher = @_create_channel()

    publish: (event, payload, options, cb) ->
        ###
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
        exch_name = options.channel

        @_get_publisher
            .then (ch) =>
                unless @_confirmed_exchanges[exch_name]
                    ch.rpc('assertExchange', exch_name, 'topic', @_generate_exchange_options(options))
                @_confirmed_exchanges[exch_name] = 1
                ch
            .then (ch) =>
                {content, properties} = @_encode_message(payload)
                message_options = _.chain(options)
                    .pick('mandatory', 'persistent', 'deliveryMode', 'expiration', 'CC')
                    .extend(properties)
                    .value()
                ch.publish(exch_name, options.topic, content, message_options)
            .done( ->
                cb?()
            , (err) ->
                logger.error({err}, 'Error when trying to publish')
                unless cb
                    throw new Error('Tried to publish without error callback, while channel was in error state')
                cb(err)
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

        Library will keep a subscription (thus a queue) per channel and listener pair.
        Subscribing more topics with the same channel and listener will result in multiple
        bindings per subscription.
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
        prepare_subscription = null

        for [exch_name, handler, sub] in subscriptions
            if exch_name is options.channel and handler is listener
                prepare_subscription = sub
                break

        unless prepare_subscription
            prepare_subscription = @_create_subscription(options, listener)
            subscriptions.push([options.channel, listener, prepare_subscription])

        prepare_subscription
            .then (sub) => @_bind_subscription(sub, options)
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
        close_sub_channel = ({ch, queue}) ->
            ch.close()

        @_subscriptions = @_subscriptions.filter ([exch_name, handler, when_channel]) ->
            if exch_name is options.channel and handler is listener
                when_channel.then(close_sub_channel)
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

    close: ->
        channels_closed = @_for_each_subscription_channel (when_channel) ->
            when_channel.then (ch) -> ch.close()
        channels_closed.push(
            @_get_publisher.then (ch) -> ch.close()
        )

        @_subscriptions = []
        connected = @_connected
        @_connected = null
        when_.all(channels_closed).finally =>
            if connected?
                return connected.then (c) -> c.close()
            true

    #
    # Connection and channel handling
    # -------------------------------
    #
    # In case of connection error, order of events is as follows:
    #  * connection error
    #  * channel closed
    #  * connection closed
    #  * connection re-connects (if reconnect option is set)

    _create_connection: ->
        ###
        Open new connection

        It is subject to _.memoize, so it will be called only once per connection
        ###
        url = @_get_connection_url()
        emit = @emit.bind(@)

        on_error = (err) ->
            logger.error({err}, "AMQP connection error", {url})

            # If it is PRECONDITION_FAILED error, it means that there is no point
            # keeping reconnect. We can only fail whole process and wait for operator
            # to fix queues.
            if err.toString().match(/PRECONDITION-FAILED/)
                emit('error', err)

        on_close = =>
            logger.info('AMQP connection terminated', {url})
            if @_connected? and @options.reconnect
                @_connected = @_keep_trying_until_connected()
                @_connected.then(@_keep_trying_until_all_resubscribed)

        amqp.connect(url).then (conn) =>
            logger.info('AMQP connection is opened now', {url})
            conn.on('error', on_error)
                .on('close', on_close)

    _keep_trying_until_connected: =>
        when_.iterate(=>
            if not @_connected?
                throw new Error('Aborted')
            @_create_connection()
                .catch (err) ->
                    logger.warn({err}, 'Error when establishing connection. Re-trying soon')
                    when_(null).delay(1000)
        , ((conn) -> conn isnt null)
        , (->)
        , null)

    _keep_trying_until_all_resubscribed: =>
        when_.iterate(=>
            if not @_connected?
                throw new Error('Aborted')
            @_open_existing_subscriptions()
                .catch (err) ->
                    logger.warn({err}, 'Error when opening existing subscription channel. This can be issue with your code!', {reason: err.stackAtStateChange})
                    when_(null).delay(1000)
        , ((result) -> result isnt null)
        , (->)
        , null)

    _when_connected: =>
        @_connected ?= @_keep_trying_until_connected()

    _get_connection_url: =>
        url = @url
        heartbeat = @options.heartbeat
        if heartbeat?
            separator = if url.indexOf('?') >= 0 then '&' else '?'
            url += "#{separator}heartbeat=#{heartbeat}"
        url

    _create_channel: ->
        options = _.pick(@options, 'reconnect')
        when_(new VentChannel(@_when_connected, options))

    _create_subscription: (options, listener) =>
        ### Create new subscription

        Subscription is object having:
          @ch - initalised channel promise
          @queue - queue name being subject to all bindings
        ###
        @_create_channel().then (ch) =>
            queue_name = @_generate_queue_name(options)
            queue_options = @_generate_queue_options(options)
            consumer = @_wrap_consumer_callback(listener, ch, options)
            consumer_options = @_generate_consumer_options(options)
            exch_name = options.channel
            exch_options = @_generate_exchange_options(options)

            cmds = [
                ['assertQueue', queue_name, queue_options]
                ['assertExchange', exch_name, 'topic', exch_options]
            ]

            if options.prefetch?
                cmds.push(['prefetch', options.prefetch])

            cmds.push(['consume', queue_name, consumer, consumer_options])
            ch.rpc(cmds).then(-> {ch, queue: queue_name})

    _bind_subscription: ({ch, queue}, options) ->
        exch_name = queue_binding_source = options.channel
        queue_binding_source = exch_name
        topic = options.topic
        cmds = []

        if options.partition?
            partition_exch_name = "#{exch_name}.#{options.group}-splitter"
            partition_exch_options =
                autoDelete: if options.autoDelete? then options.autoDelete else true
            cmds.concat([
                ['assertExchange', partition_exch_name, 'x-consistent-hash', partition_exch_options]
                ['bindExchange', partition_exch_name, exch_options, topic]
            ])
            topic = '10' # for x-consistent-hash exchange topic is a weight
            
        cmds.push(['bindQueue', queue, queue_binding_source, topic])
        ch.rpc(cmds)
            .catch (err) ->
                logger.error({err}, 'Cought bound subscription error')

    _open_existing_subscriptions: =>
        channels_opened = @_for_each_subscription_channel (when_channel) ->
            when_channel.then (ch) -> ch.open()
        when_.all(channels_opened)

    _for_each_subscription_channel: (fn) ->
        ### Returns an array of items returned by fn for each item ###
        r = []
        pick_channel = ({ch, queue}) -> ch
        for [e, h, when_subscription_ready] in @_subscriptions
            p = when_subscription_ready.then(pick_channel)
            r.push(fn(p))
        r

    _close_channel_subscription: (ch) =>
        ### This method gets called when we recive null message on chanenl, which indicates that queue got closed ###

        # If vent set to reconnect, we will re-initalize channel replaying all assertions, otherwise we just
        # remove it from subscriptions list
        if @options.reconnect
            after_channel_closed = (ch, sub_promise) ->
                ch.open()
        else
            after_channel_closed = (ch, sub_promise) =>
                @_subscriptions = @_subscriptions.filter ([e, h, sub_p]) ->
                    sub_promise isnt sub_p

        for [exch_name, handler, when_subscription_ready] in @_subscriptions
            when_subscription_ready.then ({ch, queue}) ->
                logger.debug('CLosing channel', {ch})
                ch.close()
                after_channel_closed(ch, when_subscription_ready)

    # Different rpc command option generators
    # ---------------------------------------

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
        close_channel = @_close_channel_subscription
        wrapped = (msg) ->
            if msg is null
                return close_channel(channel)
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
                    channel.ack(msg) if msg?


module.exports = (setup, options) ->
    setup = {url: setup} if _.isString(setup)
    new Vent(setup, options)

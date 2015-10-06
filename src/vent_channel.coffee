when_ = require('when')
logger = require('dl-logger')("dl:vent-channel")


class VentChannel
    ### Channel facade that will watch for errors and keep reconnencting
    ###

    constructor: (@get_connection, @options) ->
        @_command_queue = []

    _create_channel: (get_connection)->
        queue = @_command_queue

        on_close = =>
            logger.info('AMQP channel closed')
            @_channel = null

        @get_connection()
            .then (conn) -> conn.createChannel()
            .then (chnl) ->
                chnl.on('close', on_close)

                # Apply queued initialization commands, as soon as channel is opened
                executed = (chnl[cmd].apply(chnl, args) for [cmd, args] in queue)
                when_.all(executed)
                    .yield(chnl)

    _when_channel_ready: =>
        if @_closed
            throw new Error('AMQP Channel already closed')
        @_channel ?= @_create_channel()

    open: ->
        @_when_channel_ready()

    rpc: (command, args...) ->
        ### Execute one or many commands at channel.

        Commands will be queued and re-applied on every channel reconneciton
        ###
        if Array.isArray(command)
            return when_.all(@rpc.apply(@, c) for c in command)
            
        queue = @_command_queue
        @_when_channel_ready().then (ch) ->
            queue.push([command, args])
            ch[command].apply(ch, args)
                .yield(ch)

    publish: (exchange, topic, content, options) ->
        @_when_channel_ready().then (ch) ->
            if not ch.publish(exchange, topic, content, options)
                logger.info('AMQP channel overloaded')
                deferred = when_.defer()
                return channel.once('drain', deferred.resolve.bind(deferred, @))
            @

    ack: (msg) ->
        @_when_channel_ready().then (ch) ->
            ch.ack(msg)
            ch

    close: () ->
        @_closed = true
        if @_channel?
            @_channel.then (ch) -> ch.close()
        else
            when_.resolve(true)

module.exports = VentChannel

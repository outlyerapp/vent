when_ = require('when')
logger = require('dl-logger')("dl:vent-channel")


class VentChannel
    ### Channel facade that will watch for errors and keep reconnencting
    ###

    constructor: (@get_connection, @options) ->
        @_command_queue = []

    open_channel: =>
        queue = @_command_queue

        on_error = (err) ->
            # Channel errors will not be emitted on connection errors
            logger.error({err}, 'Channel error')

        on_close = =>
            logger.info('Channel closed')
            if @options.reconnect and not @_closed
                logger.info('Should reconnect channel', {queue})
                # TODO: maybe next tick will be enough.
                # We should manage timeout in one place
                setTimeout(@open_channel, 1000)

        @get_connection()
            .then (conn) -> conn.createChannel()
            .then (chnl) ->
                chnl.on('error', on_error)
                    .on('close', on_close)
                    .on('drain', ->
                        # TODO: could be used to manage outgoing sink pipe flow
                        logger.debug('Channel drained')
                    )

                # Apply queued initialization commands, as soon as channel is opened
                when_.all(chnl[cmd].apply(chnl, args) for [cmd, args] in queue)
                    .yield(chnl)

    _when_channel_ready: ->
        @_channel ?= @open_channel()

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
            ch

    publish: (exchange, topic, content, options) ->
        @_when_channel_ready().then (ch) ->
            if not ch.publish(exchange, topic, content, options)
                logger.debug('Channel overloaded')
                deferred = when_.defer()
                return channel.once('drain', deferred.resolve.bind(deferred, @))
            @

    ack: (msg) ->
        @_when_channel_ready().then (ch) ->
            ch.ack(msg)
            ch

    close: () ->
        # TODO: Figgure out how to close channel clearly
        @_closed = true


module.exports = VentChannel

stream = require 'stream'
when_ = require 'when'


class ConsumerStream extends stream.Readable

    constructor: (options)->
        @_paused_resolvers_queue = []
        highWaterMark = options.high_watermark or 16
        super({objectMode: true, highWaterMark})

    push_message: (msg) =>
        ### This is protected method to use by vent. You should not use it by client ###

        # TODO: we need to test stream flow control
        unless @push(msg)
            when_.promise (resolve) =>
                @_paused_resolvers_queue.push(resolve)

    _read: (size) ->
        queue = @_paused_resolvers_queue
        return if queue.length is 0
        for i in [0..(Math.min(size, queue.length) - 1)]
            queue[i]()
        @_paused_resolvers_queue = queue.slice(i)

    close: ->
        @push(null)
        @emit('close')


# TOOD: new librabry channel supports drain events and flow controll of
# publish method. Would be nice to have writable publisher stream supporting that

exports.ConsumerStream = ConsumerStream

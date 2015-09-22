stream = require 'stream'


class ConsumerStream extends stream.Readable

    constructor: (options)->
        @_ack = null
        highWaterMark = options.high_watermark or 16
        super({objectMode: true, highWaterMark})

    push_message: (msg, cb) =>
        """ This is protected method to us by vent. You should not use it by client """
        if @push(msg)
            cb()
        else
            @_ack = cb

    _read: ->
        if @_ack?
            @_ack()
            @_ack = null

    close: ->
        # TODO: add support for closing stream and removing subscription


# TOOD: new librabry channel supports drain events and flow controll of
# publish method. Would be nice to have writable publisher stream supporting that

exports.ConsumerStream = ConsumerStream

{Vent} = require 'amqp-vent'

jobs = Vent("amqp://guest:guest@localhost:5672/bus", {durable: true})

options = {durable: true, group: "worker"}
jobs.subscribe "job-metrics:agent", options , (msg) ->
    console.log("agent metric", {msg})

count = 1
setInterval( ->
    jobs.publish("job-metrics:agent", {hello: count++})
, 1000)

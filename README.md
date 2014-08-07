## amqp\-vent
#### PubSub event bus over amqp

### Installation
```bash
npm install amqp-vent
```

### Example Subscribe
```bash
{Vent} = require("amqp-vent")
vent = Vent("amqp://guest:guest@localhost:5672/bus")
vent.subscribe(
    topic: "*.happening"
    channel: "random"
    group: "server"
, (msg) ->
    console.log("I hear some stuff was happening", {msg})
)
vent.subscribe(
    topic: "#"
    channel: "random"
    group: "server"
, (msg) ->
    console.log("me too", {msg})
)
```

### Example Publish
```bash
{Vent} = require("amqp-vent")
vent = Vent("amqp://guest:guest@localhost:5672/bus")
vent.publish(
    topic: "stuff.happening"
    channel: "random"
    message:
        hey: "whats up!!"
)
```


### colon separated option
you can also specify the publish/subscribe option with a colon separated string, for convenience
in the format "channel:topic:group"
```
vent.publish("random:stuff.happening", {message: hey: "whats up!!"})
vent.subscribe("random:#:server", event_handler)
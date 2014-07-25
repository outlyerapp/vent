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


### TODO
- add more 'debug' logging
- update examples colon-string event details "channel:topic:group", e.g. "user:destoryed"
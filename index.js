var kafka = require('kafka-node'),
    Producer = kafka.Producer,
    client = new kafka.Client("zookeeper:2182/kafka0.8", "kafka-node-client", {});
    producer = new Producer(client),
    payloads = [
        { topic: 'fridge', messages: "{'temp':50, 'mopened': [1,3], 'tOpened': 12, 'tClosed': 14}" },
        { topic: 'fridge', messages: "{'temp':45, 'mopened': [1,4], 'tOpened': 12, 'tClosed': 14}" },
    ];

producer.on('ready', function () {
    producer.send(payloads, function (err, data) {
        console.log(data);
    });
});

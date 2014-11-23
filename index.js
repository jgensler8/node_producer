var commander = require('commander');

commander
  .version('0.0.1')
  .option('-id, --id [value]', 'ID for this producer')
  .option('-kh, --kafka-domain [value]', 'iKafka idomain to connect to.')
  .option('-kp, --kafka-port [n]', 'Kafka port to cionnect to.', parseInt)
  .parse(process.argv);

// create a kafkaesqe client, providing at least one broker
var kafkaesque = require('kafkaesque')({
  brokers: [{host: commander.kafka | 'kafka', port: commander.port | 9092}],
  clientId: commander.id | "node-producer",
  maxBytes: 2000000
});

// tearup the client
kafkaesque.tearUp(function() {
  setTimeout({
  // read data from serial
  var data = {temp: (Math.rand()+100) % 100};

  // send data over to kafka
  kafkaesque.produce({topic: 'fridge', partition: 0},
                     [data.stringify()],
                     function(err, response) {

    if(err)
    {
      console.log("err", err);
      kafkaesque.tearDown();
      return;
    }
    // shutdown connection
    console.log("response:", response);
  });
 }, 1000);
});

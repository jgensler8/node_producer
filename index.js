var commander = require('commander');

commander
  .version('0.0.1')
  .option('-id, --id [value]', 'ID for this producer')
  .option('-kh, --kafka-domain [value]', 'iKafka idomain to connect to.')
  .option('-kp, --kafka-port [n]', 'Kafka port to cionnect to.', parseInt)
  .parse(process.argv);

console.log(commander.kafkaDomain, commander.kafkaPort, commander.id);
if(commander.id === undefined) commander.id = ((Math.rand() + 10000000) % 10000000);
if(commander.kafkaDomain === undefined) commander.kafkaDomain = 'kafka';
if(commander.kafkaPort === undefined) commander.kafkaPort = 9092;

// create a kafkaesqe client, providing at least one broker
var kafkaesque = require('kafkaesque')({
  brokers: [{host: commander.kafkaDomain, port: commander.kafkaPort}],
  clientId: "node-producer-" + commander.id,
  maxBytes: 2000000
});

// tearup the client
kafkaesque.tearUp(function() {
  setTimeout(function(){
  // read data from serial
  var data = {temp: ((Math.rand()+100) % 100)};

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

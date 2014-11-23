var commander = require('commander');

commander
  .version('0.0.1')
  .option('-id, --id [value]', 'ID for this producer')
  .option('-kh, --kafka-domain [value]', 'iKafka idomain to connect to.')
  .option('-kp, --kafka-port [n]', 'Kafka port to cionnect to.', parseInt)
  .parse(process.argv);

console.log(commander.kafkaDomain, commander.kafkaPort, commander.id);
if(commander.id === undefined) commander.id = ((Math.random() + 10000000) % 10000000);
if(commander.kafkaDomain === undefined) commander.kafkaDomain = 'kafka';
if(commander.kafkaPort === undefined) commander.kafkaPort = 9092;

// create a kafkaesqe client, providing at least one broker
var kafkaesque = require('kafkaesque')({
  brokers: [{host: commander.kafkaDomain, port: commander.kafkaPort}],
  clientId: "node-producer-" + commander.id,
  maxBytes: 2000000
});

function sendData()
{
  // read data from serial
  var data = {temp: ((Math.random()+100) % 100)};

  // send data over to kafka
  kafkaesque.produce({topic: 'fridge', partition: 0},
                     [JSON.stringify(data)],
                     function(err, response) {

    if(err)
    {
      console.log("err", err);
      kafkaesque.tearDown();
    }
    // shutdown connection
    console.log("response:", response);
  });
}

kafkaesque.tearUp( function(){ setInterval(sendData, 1000*1); });

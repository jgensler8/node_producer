var commander = require('commander');

commander
  .version('0.0.1')
  .option('-id, --id [value]', 'ID for this producer')
  .option('-kh, --kafka-domain [value]', 'iKafka idomain to connect to.')
  .option('-kp, --kafka-port [n]', 'Kafka port to cionnect to.', parseInt)
  .option('-emu, --emulated', 'Run in emulated mode (no serial reading, random data)')
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

function sendSerialData()
{
  // read data from serial
  var data = {
        id: commander.id,
	open: ((Math.random()+100)%2),
	temp: ((Math.random()+100) % 100),
        dispensers:
        {
          "0": ((Math.random()+100)%2),
          "1": ((Math.random()+100)%2),
          "2": ((Math.random()+100)%2),
          "3": ((Math.random()+100)%2),
          "4": ((Math.random()+100)%2),
          "5": ((Math.random()+100)%2)
        }
  };

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

function sendRandomData()
{
  // read data from serial
  var data = {
        id: commander.id,
        open: ((Math.random()+100)%2),
        temp: ((Math.random()+100) % 100),
        dispensers:
        {
          "0": ((Math.random()+100)%2),
          "1": ((Math.random()+100)%2),
          "2": ((Math.random()+100)%2),
          "3": ((Math.random()+100)%2),
          "4": ((Math.random()+100)%2),
          "5": ((Math.random()+100)%2)
        }
  };

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

if(commander.emulated) kafkaesque.tearUp( function(){ setInterval(sendRandomData, 1000*1); });
else kafkaesque.tearUp( function(){ setInterval(sendSerailData, 1000*1); });

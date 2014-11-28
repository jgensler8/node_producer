var commander = require('commander');
var http = require('http');

commander
  .version('0.0.1')
  .option('-s, --unix-socket [value]', 'set the UNIX socket to write to')
  .option('-i, --id [value]', 'ID for this producer')
  .option('-kh, --kafka-host [value]', 'Kafka host to connect to.')
  .option('-kp, --kafka-port [n]', 'Kafka port to cionnect to.', parseInt)
  .option('-e, --emulated', 'Run in emulated mode (no socket reading, random data)')
  .option('-m, --ms-send [n]', 'Send a message arg amount of milliseconds', parseInt)
  .parse(process.argv);

if(commander.id === undefined) commander.id = ((Math.random() + 10000000) % 10000000);
if(commander.kafkaHost === undefined) commander.kafkaHost = 'kafka';
if(commander.kafkaPort === undefined) commander.kafkaPort = 9092;
if(!commander.emulated && commander.unixSocket === undefined)
{
  console.log("ERROR: emulated set but no unixSocket specified");
  return -1;
}
if(commander.msSend === undefined) commander.msSend = 1000*1; //one second

// create a kafkaesqe client, providing at least one broker
var kafkaesque = require('kafkaesque')({
  brokers: [{host: commander.kafkaHost, port: commander.kafkaPort}],
  clientId: "node-producer-" + commander.id,
  maxBytes: 2000000
});

function sendSerialData(socket) 
{
  console.log("sending serial data");

  // read data from UNIX socket
  //socket.read();

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
                     [JSON.stringify("data")],
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
  console.log("sending random data");
  // read data from serial
  var data = {
        id: commander.id,
        open: Math.floor((Math.random()+100)%2),
        temp: Math.floor((Math.random()+100) % 100),
        dispensers:
        {
          "0": Math.floor((Math.random()+100)%2),
          "1": Math.floor((Math.random()+100)%2),
          "2": Math.floor((Math.random()+100)%2),
          "3": Math.floor((Math.random()+100)%2),
          "4": Math.floor((Math.random()+100)%2),
          "5": Math.floor((Math.random()+100)%2)
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
    console.log("response:", response);
  });
}

if(commander.emulated === true)
{
  console.log("true");

  // send data over to kafka
  kafkaesque.tearUp(function(){

/*
  kafkaesque.produce({topic: 'fridge', partition: 0},
                     ["hello world"],
                     function(err, response) {

    if(err)
    {
      console.log("err", err);
      kafkaesque.tearDown();
    }
    console.log("response:", response);
  });
*/
  setInterval(sendRandomData, commander.msSend);

  });
}
else
{
  var socket = http.createServer();
  socket.listen(commander.unixSocket);
  kafkaesque.tearUp( function(){ setInterval(sendSerialData, commander.msSend, socket); });
}

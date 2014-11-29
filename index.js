var commander = require('commander');
var net = require('net');

commander
  .version('0.0.1')
  .option('-i, --id [value]', 'ID for this producer')
  .option('-kh, --kafka-host [value]', 'Kafka host to connect to.')
  .option('-kp, --kafka-port [n]', 'Kafka port to cionnect to.', parseInt)
  .option('-e, --emulated', 'Run in emulated mode (no socket reading, random data)')
  .option('-m, --ms-send [n]', 'Send a message arg amount of milliseconds', parseInt)
  .option('-t, --tty-buf [value]', 'set the tty buffer to read from')
  .option('-b, --baudrate [n]', 'set the baudrate for the serial port', parseInt)
  .parse(process.argv);

if(commander.id === undefined) commander.id = ((Math.random() + 10000000) % 10000000);
if(commander.kafkaHost === undefined) commander.kafkaHost = 'kafka';
if(commander.kafkaPort === undefined) commander.kafkaPort = 9092;
if(commander.baudrate === undefined) commander.baudrate = 9600;
if(!commander.emulated && commander.ttyBuf === undefined)
{
  console.log("ERROR: no serial option set!");
  return -1;
}
if(commander.msSend === undefined) commander.msSend = 1000*1; //one second

// create a kafkaesqe client, providing at least one broker
var kafkaesque = require('kafkaesque')({
  brokers: [{host: commander.kafkaHost, port: commander.kafkaPort}],
  clientId: "node-producer-" + commander.id,
  maxBytes: 2000000
});

function sendRandomData()
{
  //console.log("sending random data");

  // read data from serial
  var data = {
        id: commander.id,
        o: Math.floor((Math.random()*10)%2),
        t: Math.floor((Math.random()*10) % 100),
        d:
        {
          "0": Math.floor((Math.random()*10)%2),
          "1": Math.floor((Math.random()*10)%2),
          "2": Math.floor((Math.random()*10)%2),
          "3": Math.floor((Math.random()*10)%2),
          "4": Math.floor((Math.random()*10)%2),
          "5": Math.floor((Math.random()*10)%2)
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
    //console.log("response:", response);
  });
}

if(commander.emulated === true)
{
  console.log("true");

  // send data over to kafka
  kafkaesque.tearUp(function(){

  kafkaesque.metadata({topic: 'fridge'}, function(err, metadata)
  {
    if(err) console.log("ERROR", err);
    else console.log("metadata:", metadata);
  });

  setInterval(sendRandomData, commander.msSend);

  });
}
else
{
  var SerialPort = require("serialport").SerialPort
  var serialPort = new SerialPort(commander.ttyBuf, {
    baudrate: commander.baudrate
  });
  
  serialPort.on('open', function()
  {
    console.log("Serial opened...");
    kafkaesque.tearUp( function(){

kafkaesque.metadata({topic: "fridge"}, function(err, data){ console.log(err, data);});

      console.log("Connected to Kafka...");
      
      var json = "";
    
      serialPort.on('data', function(data) {
        if(String(data).indexOf("  ") === 0)
        {

          try{
            var data = JSON.parse(json);

            var message = {
              id: commander.id,
              o: data.o,
              d: data.d,
              t: data.t
             };
      
            // send data over to kafka
            kafkaesque.produce({topic: 'fridge', partition: 0},
                               [JSON.stringify(message)],
                               function(err, response) {
              if(err)
              {
                console.log("err", err);
                kafkaesque.tearDown();
              }
              // shutdown connection
              //console.log("response:", response);
            });
          
            //also log for the user
            console.log(JSON.stringify(message));
          }
          catch(e)
          {
            console.log(e);
          }
          
          //reset the json variable
          json = "";
        }
        else
        {
           json += data;
        }
        //console.log('data received: ' + data);
      });
    });
  });
}

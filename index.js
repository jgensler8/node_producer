var commander = require('commander');
var http = require('http');

commander
  .version('0.0.1')
  .option('-s, --unix-server [value]', 'set the UNIX server file to read from')
  .option('-i, --id [value]', 'ID for this producer')
  .option('-kh, --kafka-host [value]', 'Kafka host to connect to.')
  .option('-kp, --kafka-port [n]', 'Kafka port to cionnect to.', parseInt)
  .option('-e, --emulated', 'Run in emulated mode (no socket reading, random data)')
  .option('-m, --ms-send [n]', 'Send a message arg amount of milliseconds', parseInt)
  .parse(process.argv);

if(commander.id === undefined) commander.id = ((Math.random() + 10000000) % 10000000);
if(commander.kafkaHost === undefined) commander.kafkaHost = 'kafka';
if(commander.kafkaPort === undefined) commander.kafkaPort = 9092;
if(!commander.emulated && commander.unixServer === undefined)
{
  console.log("ERROR: emulated set but no unixServer specified");
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
        open: Math.floor((Math.random()*10)%2),
        temp: Math.floor((Math.random()*10) % 100),
        dispensers:
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
  var server = http.createServer(function(request, response)
  {
    console.log(request, response);
    
    kafkaesque.tearUp( function(){
      var chunk = "";
      
      server.on('data', function(data)
      {
        
        chunk += data.toString(); // Add string on the end of the variable 'chunk'
        d_index = chunk.indexOf(';'); // Find the delimiter
    
        // While loop to keep going until no delimiter can be found
        while (d_index > -1) {
            try {
                string = chunk.substring(0,d_index); // Create string up until the delimiter
                json = JSON.parse(string); // Parse the current string
                
                console.log(data);
        
                // send data over to kafka
                kafkaesque.produce({topic: 'fridge', partition: 0},
                                   [data],
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
            chunk = chunk.substring(d_index+1); // Cuts off the processed chunk
            d_index = chunk.indexOf(';'); // Find the new delimiter
        }
        
      });
    });
    
  });
  server.listen(commander.unixSocket);
}

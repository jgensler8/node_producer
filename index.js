// create a kafkaesqe client, providing at least one broker
var kafkaesque = require('kafkaesque')({
  brokers: [{host: 'kafka', port: 9092}],
  clientId: 'node-producer',
  maxBytes: 2000000
});

/*
// tearup the client
kafkaesque.tearUp(function() {
  // send two messages to the testing topic
  kafkaesque.produce({topic: 'testing', partition: 0}, 
                     ['wotcher mush', 'orwlight geezer'], 
                     function(err, response) {
    // shutdown connection
    console.log(response);
    kafkaesque.tearDown();
  });
});
*/	
while(true){};

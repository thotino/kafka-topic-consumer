/**
 * project JSDoc description
 * @module {Object} module name
 * @version 1.0.0
 * @author author name
 * @requires dependency 1
 * @requires dependency 2
 * ...
 */

"use strict";

//================================================================================
// dependencies
//================================================================================
const Promise = global.Promise = require("bluebird");
const kafka = require("kafka-node");
const fs = require("fs-extra");
const path = require("path");
const elasticsearchHandler = require("elasticsearch-helper");

//================================================================================
// config
//================================================================================
const configKafka = fs.readJsonSync(path.resolve(__dirname, "../conf/config-kafka.json"));

//================================================================================
// aliases
//================================================================================
const Consumer = kafka.Consumer;
const Offset = kafka.Offset;
//================================================================================
// module
//================================================================================
const client = new kafka.KafkaClient(configKafka.options);
const offset = new Offset();

module.exports.fromQueueToIndex = function fromQueueToIndex(topicName = configKafka.defaultTopic) {
    // return new Promise((resolve, reject) => {
        const topicConsumer = new Consumer(client, [{topic: topicName, partition: 0}]);

        topicConsumer.on("message", (message) => {
            console.log(message.value);
            elasticsearchHandler.helpers.addObjectToIndex(JSON.parse(message.value))
                .then((result) => { if(result.statusCode !== 201) { throw "data not indexed"; } });             
        });

        topicConsumer.on("error", (error) => {
            if (error) { throw error; }
        });

        topicConsumer.on("offsetOutOfRange", (topic) => {
  		topic.maxNum = 2;

		  offset.fetch([topic], (err, offsets) => {
		    if (err) {
		      console.log(err);
		    }

		    const min = Math.min(offsets[topic.topic][topic.partition]);
		    consumer.setOffset(topic.topic, topic.partition, min);
		  });
        });
    // });
    
};

module.exports.receiveMessages = function receiveMessages(topicName = configKafka.defaultTopic) {
    return new Promise((resolve, reject) => {
        const topicConsumer = new Consumer(client, [{topic: topicName, partition: 0}]);

        topicConsumer.on("message", (message) => {
            console.log(message.value);
            return resolve(JSON.parse(message.value));
        });

        topicConsumer.on("error", (error) => {
            return reject(error);
        });

        topicConsumer.on("offsetOutOfRange", (error) => {
            return reject(error);
        });
    });
    
};

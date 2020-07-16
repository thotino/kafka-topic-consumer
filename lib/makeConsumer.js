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
//================================================================================
// config
//================================================================================
const configKafka = fs.readJsonSync("./conf/config-kafka.json");

//================================================================================
// aliases
//================================================================================
const Consumer = kafka.Consumer;

//================================================================================
// module
//================================================================================
const client = new kafka.KafkaClient(configKafka.options);

module.exports.receiveMessages = function receiveMessages(topicName = configKafka.defaultTopic) {
    return new Promise((resolve, reject) => {
        const topicConsumer = new Consumer(client, [{topic: topicName}]);

        topicConsumer.on("message", (message) => {
            console.log(message);
            return resolve(message);
        });

        topicConsumer.on("error", (error) => {
            return reject(error);
        });

        topicConsumer.on("offsetOutOfRange", (error) => {
            return reject(error);
        });
    });
    
};
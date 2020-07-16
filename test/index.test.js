"use strict";
const kafkaCons = require("../index");

const data = {
    test: "test",
    foo: "bar",
};

kafkaCons.makeConsumer.receiveMessages()
    .then((data) => {console.log(data);});

const express = require("express");
const router = express.Router();
const pg = require("pg");
const bodyParser = require("body-parser");
const producer_orders = require("./producer-orders")                // here we have defined the sendOrder function
const producer_registrations = require("./producer-registrations")  // here we have defined the sendNewRegistration function

//router.use(bodyParser.urlencoded({ extended: true }));
router.use(bodyParser.json()); // to parse the order in JSON format

router.get("/", async(req, res) =>{
    res.send("")
})

// Here we will handle the post request for the order:
// 1. We will get the order details from the request body (e.g. with postman: post request -> raw -> json), such as:
//     - order_id (sarà la chiave nel DB)
//     - product_name
//     - category of the product
//     - quantity
//     - sales income for the order
//     - profit income for the order
//     - location where the order comes from (states, like italy, germany and so on)
// 
// 2. We will publish the order on a topic ('orders') using the kafka producer functions and logic that we define in producer-orders.js
// 3. The consumer (consumer-orders.js), subscribed to the topic, will receive the order
// 4. He will save it into postgres
// 5. The raw data are transformed using DBT 
// 6. Grafana is connected to postgres so we can build dashboards like
//    - categories/products with the highest sales
//    - location where we receive most orders from ecc.

router.post("/publish-order", async (req, res) => {
    
    // retrieve order information from the request body 
    // Note: the primary key will be the order_id which is set as "serial" in the db, so it will be automatically incremented
    const {user_id, product_name, product_category, quantity, sales, profit} = req.body;

    console.log("\nOrder received from postman: ", req.body);
    
    /*
    console.log("\n")
    console.log("user making the order:", user_id);
    console.log("product name:", product_name);
    console.log("product category:", product_category);
    console.log("quantity:", quantity);
    console.log("sales:", sales);
    console.log("profit:", profit);
    */

    // now we send the order to the kafka broker
    await producer_orders.sendOrder(req.body)

    res.sendStatus(200)
});

// Here we will handle the post request for a new registration from a user. The flow is the same as described above but
// the producer is producer-registration.js and the consumer is consumer-registration.js
router.post("/registration", async (req, res) => {

    // retrieve new registration information from the request body 
    // Note: the primary key will be the id which is set as "serial" in the db, so it will be automatically incremented
    const {firstname, lastname, country} = req.body

    console.log("\nNew registration received from postman: ", req.body)
    
    await producer_registrations.sendNewRegistration(req.body)

    res.sendStatus(200)
});


module.exports = router;
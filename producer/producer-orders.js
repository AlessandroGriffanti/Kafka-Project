
const { Kafka } = require('kafkajs');

const kafka = new Kafka({ brokers: ['localhost:9092'] });
const producer = kafka.producer();

// function to send the order received 
async function sendOrder(order) {

    console.log("\nOrder received to publish: ", order);
    //console.log("\nUser making the order:", order.user_id);
    //console.log("\nProduct ordered:", order.product_name);

    // key to identify the order built using the id of the user making the order and the product name
    const key = {
        user_making_order: order.user_id,
        product_ordered: order.product_name
    }

    try {
        await producer.connect();
        
        await producer.send({
            topic: 'orders',
            messages: [
                {   
                    key: JSON.stringify(key),
                    value: JSON.stringify(order)
                
                }
            
            ],
        });

        console.log('\nOrder sent:', order);

    } catch (err) {
        console.error('\nError sending message to Kafka:', err);
    }
        finally{
            await producer.disconnect();
        }
}

// export the send order function so that we can use it else where (in particular in the producer-route.js)
module.exports = {
    sendOrder
}
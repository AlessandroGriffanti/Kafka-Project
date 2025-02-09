
/*
    Orders consumer: it is in charge of receiving the new messages ( = new orders) published at "orders" topic and
    save them into the database
*/

require('dotenv').config();
const pg = require("pg");   // to connect to postgres db
const { Kafka } = require('kafkajs')

const kafka = new Kafka({ brokers: ['localhost:9092'] });
const consumer = kafka.consumer({ groupId: 'orders-group' })

const db = new pg.Client({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_DATABASE,
    port: process.env.DB_PORT
});

// async function so we can use await 
async function main(){

    // connect to postgres db
    await db.connect();
    console.log("\n----------------------------------------\nOrders consumer connected to the postgres db")

    // connect to the broker
    await consumer.connect()
    console.log("Orders consumer connected to broker")
    
    // subscribe to a topic. We could subscribe to more than one 
    await consumer.subscribe({ topics: ['orders'], fromBeginning: true }) //, fromBeginning: true 
    console.log("Orders consumer subscribed to the topic: orders\n----------------------------------------\n");

    await consumer.run({
        
        // defines what happens when each message is received
        eachMessage: async ({ topic, message }) => { // partition heartbeat, pause
            
            // we log the message
            console.log({
                "Orders consumer: message received from broker": {
                    key: JSON.parse(message.key),
                    value: JSON.parse(message.value), // we receive the order in JSON format
                    
                    //headers: message.headers,
                }
            })

            // we store it into the postgres database
            try{                                             
                
                let sql = {
                    
                    text: "INSERT INTO orders (user_id, product_name, product_category, quantity, sales, profit, order_date) values ($1, $2, $3, $4, $5, $6, $7)",
                    values: [JSON.parse(message.value).user_id, JSON.parse(message.value).product_name, 
                             JSON.parse(message.value).product_category, JSON.parse(message.value).quantity, 
                             JSON.parse(message.value).sales, JSON.parse(message.value).profit,
                             JSON.parse(message.value).order_date]                            
                }

                await db.query(sql)

        }catch(error){
            console.log(error.message);
            await db.end();
        }

        },
    })

}

//call the main function
main()




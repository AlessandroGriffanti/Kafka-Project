

/*
    Registration consumer: it is in charge of receiving the new messages ( = new subscriptions) published at "registrations" topic and
    save them into the database
*/

require('dotenv').config();
const pg = require("pg");   // to connect to postgres db
const { Kafka } = require('kafkajs')

const kafka = new Kafka({ brokers: ['localhost:9092'] });
const consumer = kafka.consumer({ groupId: 'registration-group' })

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
    console.log("\n----------------------------------------\nRegistration consumer connected to the postgres db")

    // connect to the broker
    await consumer.connect()
    console.log("Registration consumer connected to broker")
    
    // subscribe to a topic. We could subscribe to more than one 
    await consumer.subscribe({ topics: ['registrations'], fromBeginning: true }) //, fromBeginning: true 
    console.log("Registration consumer subscribed to the topic: registrations\n----------------------------------------\n");

    await consumer.run({
        
        // defines what happens when each message is received
        eachMessage: async ({ topic, message }) => { // partition heartbeat, pause
            
            // we log the message
            console.log({
                "Registration consumer: message received from broker": {
                    key: JSON.parse(message.key),
                    value: JSON.parse(message.value), // we receive the order in JSON format
                    
                    //headers: message.headers,
                }
            })

            // we store it into the postgres database
            try{                                             
                let sql = {
                    text: "INSERT INTO users (firstname, lastname, country) values ($1, $2, $3)",
                    values: [JSON.parse(message.value).firstname, JSON.parse(message.value).lastname, 
                            JSON.parse(message.value).country ]
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




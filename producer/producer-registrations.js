
const { Kafka } = require('kafkajs');

const kafka = new Kafka({ brokers: ['localhost:9092'] });
const producer = kafka.producer();

// function to send the new registration received. The idea is that when a new customer subscribes, we send the data to the broker
async function sendNewRegistration(registration) {

    console.log("\nNew registration received to publish: ", registration);

    // key to identify the order built using the id of the user making the order and the product name
    const key = {
        registration_name: registration.firstname,
        registration_surname: registration.lastname
    }

    try {
        await producer.connect();
        
        await producer.send({
            topic: 'registrations',
            messages: [
                {   
                    key: JSON.stringify(key),
                    value: JSON.stringify(registration)
                }
            ],
        });

        console.log('\nRegistration sent:', registration);

    } catch (err) {
        console.error('\nError sending message to Kafka:', err);
    }
        finally{
            await producer.disconnect();
        }
}

// export the sendNewRegistration function so that we can use it else where (in particular in the producer-route.js)
module.exports = {
    sendNewRegistration
}
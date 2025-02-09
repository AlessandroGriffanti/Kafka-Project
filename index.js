const express = require("express");
const app = express();
const port = 4444;
const producer = require("./producer/producer-route") //here we have defined all the endpoints for the various producers


// it handles the producer routes
app.use(producer)

// default route
app.get("/", (req, res) => {
    res.send("welcome to the project!");
});

app.listen(port,  () => {
    console.log("server listening on port: " + port);
});
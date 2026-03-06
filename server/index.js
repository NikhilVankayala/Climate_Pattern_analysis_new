require("dotenv").config({ path: __dirname + "/.env" });

const cors = require('cors');
const express = require('express');
const app = express();
const pool = require('./db');

app.use(cors());

const PORT = process.env.PORT || 9000;

//Functions
const testConnection = "/test";
const getStations = "/stations"

// testConnection
app.get(testConnection, (req, res) => {
  pool.query(`SELECT * FROM selected_stations LIMIT 50;`, 
          
  (err, data) => {
    if (err) {
      return res.json(err)
    }
    else {
      return res.json(data)
    }
  });
});

app.get(getStations, (req, res) => {
  pool.query(`SELECT * FROM selected_stations;`, 
          
  (err, data) => {
    if (err) {
      return res.json(err)
    }
    else {
      return res.json(data)
    }
  });
});

app.get("/", (req, res) => {
  res.send("Hello World!");
});

app.listen(PORT, () => {
    console.log(`Server listening on the port  ${PORT}`);
})
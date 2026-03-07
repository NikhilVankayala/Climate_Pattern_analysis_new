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
const getSomeStations = '/someStations';

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

app.get(getSomeStations, (req, res) => {
  pool.query(`(SELECT * FROM selected_stations WHERE REGION = 'Northeast' ORDER BY RAND() LIMIT 25)
    UNION ALL
    (SELECT * FROM selected_stations WHERE REGION = 'Southeast' ORDER BY RAND() LIMIT 35)
    UNION ALL
    (SELECT * FROM selected_stations WHERE REGION = 'Midwest' ORDER BY RAND() LIMIT 50)
    UNION ALL
    (SELECT * FROM selected_stations WHERE REGION = 'Southwest' ORDER BY RAND() LIMIT 50)
    UNION ALL
    (SELECT * FROM selected_stations WHERE REGION = 'West' ORDER BY RAND() LIMIT 50)
    UNION ALL
    (SELECT * FROM selected_stations WHERE REGION = 'Alaska' ORDER BY RAND() LIMIT 50)
    UNION ALL
    (SELECT * FROM selected_stations WHERE REGION = 'Hawaii' ORDER BY RAND() LIMIT 5)`, 
          
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
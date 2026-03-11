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
const getStationHypothesis1 = "/stationResults1";
const getStationHypothesis2 = "/stationResults2";
const getStationHypothesis3 = "/stationResults3";
const getStationHypothesis4 = "/stationResults4";
const getStationHypothesisData1 = "/stationData1";
const getStationHypothesisData2 = "/stationData2";
const getStationHypothesisData3 = "/stationData3";
const getStationHypothesisData4 = "/stationData4";
const getSearch = "/search";
const getSearchValue = "/searchValue";
const getSearchValue1 = "/searchValue1";
const getSearchValue2 = "/searchValue2";
const getSearchValue3 = "/searchValue3";
const getSearchValue4 = "/searchValue4";

// testConnection
app.get(testConnection, (req, res) => {
  pool.query(`ALTER TABLE hypothesis_2
    ADD COLUMN correlation FLOAT GENERATED ALWAYS AS (expression) STORED;`, 
          
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

app.get(getStationHypothesis1, (req, res) => {
  pool.query(`SELECT
   (COUNT(*) * SUM(early_anomaly * late_anomaly) - SUM(early_anomaly) * SUM(late_anomaly)) /
    (SQRT(COUNT(*) * SUM(early_anomaly * early_anomaly) - SUM(early_anomaly) * SUM(early_anomaly)) *
     SQRT(COUNT(*) * SUM(late_anomaly * late_anomaly) - SUM(late_anomaly) * SUM(late_anomaly))) AS correlation_coefficient_sample, STATION
    FROM hypothesis_1 GROUP BY STATION;`,
    
  (err, data) => {
    if (err) {
      return res.json(err)
    }
    else {
      return res.json(data)
    }
  });
});

app.get(getStationHypothesis2, (req, res) => {
  pool.query(`SELECT
    (COUNT(*) * SUM(spring_temp_anomaly * heat_wave_days) - SUM(spring_temp_anomaly) * SUM(heat_wave_days)) /
    (SQRT(COUNT(*) * SUM(spring_temp_anomaly * spring_temp_anomaly) - SUM(spring_temp_anomaly) * SUM(spring_temp_anomaly)) *
     SQRT(COUNT(*) * SUM(heat_wave_days * heat_wave_days) - SUM(heat_wave_days) * SUM(heat_wave_days))) AS correlation_coefficient_sample, STATION
    FROM hypothesis_2 GROUP BY STATION;`,
    
  (err, data) => {
    if (err) {
      return res.json(err)
    }
    else {
      return res.json(data)
    }
  });
});

app.get(getStationHypothesis3, (req, res) => {
  pool.query(`SELECT 
    STATION,
    COUNT(*) AS total_days,
    SUM(CASE WHEN range_change < 0 THEN 1 ELSE 0 END) AS corr_days
FROM 
    hypothesis_3
GROUP BY 
    STATION;`,
    
  (err, data) => {
    if (err) {
      return res.json(err)
    }
    else {
      return res.json(data)
    }
  });
});

app.get(getStationHypothesis4, (req, res) => {
  pool.query(`SELECT
    (COUNT(*) * SUM(snowmelt_doy * spring_prcp) - SUM(snowmelt_doy) * SUM(spring_prcp)) /
    (SQRT(COUNT(*) * SUM(snowmelt_doy * snowmelt_doy) - SUM(snowmelt_doy) * SUM(snowmelt_doy)) *
     SQRT(COUNT(*) * SUM(spring_prcp * spring_prcp) - SUM(spring_prcp) * SUM(spring_prcp))) AS correlation_coefficient_sample, STATION
    FROM hypothesis_4 GROUP BY STATION;`,
    
  (err, data) => {
    if (err) {
      return res.json(err)
    }
    else {
      return res.json(data)
    }
  });
});

app.get(getStationHypothesisData1, (req, res) => {
  const station = req.query.id
  pool.query(`SELECT baseline_early, baseline_late, std_early, std_late, num_years FROM hypothesis_1 WHERE STATION = '${station}' LIMIT 1;`,
    
  (err, data) => {
    if (err) {
      return res.json(err)
    }
    else {
      return res.json(data)
    }
  });
});

app.get(getStationHypothesisData2, (req, res) => {
  const station = req.query.id
  pool.query(`SELECT (SELECT COUNT(*) FROM hypothesis_2 WHERE STATION = '${station}' AND heat_wave_days != 0 ) AS hot_years, baseline_spring_temp, std_spring_temp, num_years FROM hypothesis_2 WHERE STATION = '${station}' LIMIT 1;`,
    
  (err, data) => {
    if (err) {
      return res.json(err)
    }
    else {
      return res.json(data)
    }
  });
});

app.get(getStationHypothesisData3, (req, res) => {
  const station = req.query.id
  pool.query(`SELECT 
    (SELECT COUNT(*) FROM hypothesis_3 WHERE STATION = '${station}' AND range_change < 0) AS effected_days,
    COUNT(*) AS windy_days
FROM hypothesis_3 WHERE STATION = '${station}';`,
    
  (err, data) => {
    if (err) {
      return res.json(err)
    }
    else {
      return res.json(data)
    }
  });
});

app.get(getStationHypothesisData4, (req, res) => {
  const station = req.query.id
  pool.query(`SELECT (SELECT COUNT(*) FROM hypothesis_4 WHERE STATION = '${station}') AS num_years, baseline_melt_doy, std_melt_doy, baseline_spring_prcp, std_spring_prcp FROM hypothesis_4 WHERE STATION = '${station}' LIMIT 1;`,
    
  (err, data) => {
    if (err) {
      return res.json(err)
    }
    else {
      return res.json(data)
    }
  });
});

app.get(getSearch, (req, res) => {
  const input = req.query.input
  pool.query(`SELECT STATION_ID AS result FROM selected_stations WHERE STATION_ID LIKE '%${input}%'
    UNION ALL
    SELECT NAME FROM selected_stations WHERE NAME LIKE '%${input}%'
    LIMIT 10;`,
    
  (err, data) => {
    if (err) {
      return res.json(err)
    }
    else {
      return res.json(data)
    }
  });
});

app.get(getSearchValue, (req, res) => {
  const input = req.query.out
  pool.query(`SELECT * FROM selected_stations WHERE STATION_ID = '${input}' OR NAME = '${input}';`,
    
  (err, data) => {
    if (err) {
      return res.json(err)
    }
    else {
      return res.json(data)
    }
  });
});

app.get(getSearchValue1, (req, res) => {
  const input = req.query.out.replaceAll("+", " ")
  pool.query(`SELECT * FROM (SELECT STATION, year, early_winter_prcp, late_winter_prcp, NAME FROM hypothesis_1 h1
RIGHT JOIN selected_stations ss on ss.STATION_ID = h1.STATION) AS res WHERE STATION = '${input}' OR NAME = '${input}';`,
    
  (err, data) => {
    if (err) {
      return res.json(err)
    }
    else {
      return res.json(data)
    }
  });
});

app.get(getSearchValue2, (req, res) => {
  const input = req.query.out.replaceAll("+", " ")
  pool.query(`SELECT * FROM (SELECT STATION, year, h2.spring_avg_tmax, h2.heat_wave_days, NAME FROM hypothesis_2 h2
RIGHT JOIN selected_stations ss on ss.STATION_ID = h2.STATION) AS res WHERE STATION = '${input}' OR NAME = '${input}';`,
    
  (err, data) => {
    if (err) {
      return res.json(err)
    }
    else {
      return res.json(data)
    }
  });
});

app.get(getSearchValue3, (req, res) => {
  const input = req.query.out.replaceAll("+", " ")
  pool.query(`SELECT * FROM (SELECT STATION, DATE, h3.AWND, h3.avg_range_after, h3.avg_range_before, NAME FROM hypothesis_3 h3
RIGHT JOIN selected_stations ss on ss.STATION_ID = h3.STATION) AS res WHERE STATION = '${input}' OR NAME = '${input}';`,
    
  (err, data) => {
    if (err) {
      return res.json(err)
    }
    else {
      return res.json(data)
    }
  });
});

app.get(getSearchValue4, (req, res) => {
  const input = req.query.out.replaceAll("+", " ")
  pool.query(`SELECT * FROM (SELECT STATION, year, h4.snowmelt_doy, h4.avg_daily_prcp, NAME FROM hypothesis_4 h4
RIGHT JOIN selected_stations ss on ss.STATION_ID = h4.STATION) AS res WHERE STATION = '${input}' OR NAME = '${input}';`,
    
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
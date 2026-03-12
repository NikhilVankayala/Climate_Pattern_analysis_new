import Map from "./components/map.js"
import Header from "./components/header.js"
import DisplayBox from "./components/display-box.js"
import FilterButton from "./components/filter-button.js"
import Popup from "./components/popup.js"

import './styles/app.css'

//import logo from './logo.svg';
import axios from 'axios';
import React, {useState, useEffect} from 'react';
import SelectSearch from 'react-select-search';
import Select from 'react-select';
import { Bar } from "react-chartjs-2";

import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js';

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend
);

export const options = {
  responsive: true,
  maintainAspectRatio: false,
  plugins: {
    legend: {
      position: 'top',
    },
    title: {
      display: true,
      text: 'Chart.js Bar Chart',
    },
  },
};

function App() {

  const [hypothesis, setHypothesis] = useState("h1");
  const [results, setResults] = useState([]);

  const [hyp5, setHyp5] = useState([]);

  useEffect(() => {
    const fetchData = async () => {
      if(hypothesis == "h2") {
        const res = await axios.get("http://localhost:9000/stationResults2");
        setResults(res.data);
      } else if(hypothesis == "h3") {
        const res = await axios.get("http://localhost:9000/stationResults3");
        setResults(res.data);
      } else if(hypothesis == "h4") {
        const res = await axios.get("http://localhost:9000/stationResults4");
        setResults(res.data);
      } else {
        const res = await axios.get("http://localhost:9000/stationResults1");
        setResults(res.data);
      }

      const res = await axios.get("http://localhost:9000/stationResults5");
      setHyp5(res.data);
    }
    fetchData();
  }, [hypothesis]);

  const [options, setOptions] = useState([]);
  const [input, setInput] = useState("");
  useEffect(() => {
    const fetchData = async () => {
      const res = await axios.get("http://localhost:9000/search", {params: {"input": input}});
      res.data.map((d) => setOptions((prevOptions) => [...prevOptions, {value: d.result, label: d.result}]));
        
    }
    fetchData();
  }, [input]);

  const onSelectChange = value => {
    if(value) {
      setInput( value );
    } else {
      setInput("");
    }
    
  };


  const [selection, setSelection] = useState([]);
  const [output, setOutput] = useState("");
  useEffect(() => {
    const fetchData = async () => {
      if(hypothesis == "h2") {
        const res = await axios.get("http://localhost:9000/searchValue2", {params: {"out": output}});
        setSelection(res.data);
      } else if(hypothesis == "h3") {
        const res = await axios.get("http://localhost:9000/searchValue3", {params: {"out": output}});
        setSelection(res.data);
      } else if(hypothesis == "h4") {
        const res = await axios.get("http://localhost:9000/searchValue4", {params: {"out": output}});
        setSelection(res.data);
      } else {
        const res = await axios.get("http://localhost:9000/searchValue1", {params: {"out": output}});
        setSelection(res.data);
      }

    }
    fetchData();
  }, [output]);
  const onSelect = value => {
    if(value) {
      setOutput( value.value );
      setPopup(true);
    } else {
      setOutput("");
    }
  };

  const [popup, setPopup] = useState(false);
  
  // const labels = ["2000", "2001", "2002", "2003", "2004", "2005", "2006", "2007", "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024", "2025"];

  const labels1 = selection.map(dataPoint => dataPoint.year);
  const set1A = selection.map(dataPoint => dataPoint.early_winter_prcp);
  const set1B = selection.map(dataPoint => dataPoint.late_winter_prcp);
  const data1 = {
    labels: labels1,
    datasets: [
      {
        label: 'Early Winter Percipitation (mm)',
        data: set1A,
        backgroundColor: 'rgba(255, 99, 132, 0.5)',
      },
      {
        label: 'Late Winter Percipitation (mm)',
        data: set1B,
        backgroundColor: 'rgba(53, 162, 235, 0.5)',
      },
    ],
  };

  const labels2 = selection.map(dataPoint => dataPoint.year);
  const set2A = selection.map(dataPoint => dataPoint.spring_avg_tmax);
  const set2B = selection.map(dataPoint => dataPoint.heat_wave_days);
  const data2 = {
    labels: labels2,
    datasets: [
      {
        label: 'Average Spring Temperature (°C)',
        data: set2A,
        backgroundColor: 'rgba(255, 99, 132, 0.5)',
      },
      {
        label: 'Number of Heat Wave Days',
        data: set2B,
        backgroundColor: 'rgba(53, 162, 235, 0.5)',
      },
    ],
  };

  const labels3 = selection.map(dataPoint => dataPoint.DATE);
  const set3A = selection.map(dataPoint => dataPoint.AWND);
  const set3B = selection.map(dataPoint => dataPoint.avg_range_after);
  const set3C = selection.map(dataPoint => dataPoint.avg_range_before);
  const data3 = {
    labels: labels3,
    datasets: [
      {
        label: 'Temperature Range Before Wind Event',
        data: set3C,
        backgroundColor: 'rgba(38, 185, 9, 0.5)',
      },
      /*{
        label: 'Average Wind Speed (m/s)',
        data: set3A,
        backgroundColor: 'rgba(255, 99, 132, 0.5)',
      },*/
      {
        label: 'Temperature Range After Wind Event',
        data: set3B,
        backgroundColor: 'rgba(53, 162, 235, 0.5)',
      },
    ],
  };

  const labels4 = selection.map(dataPoint => dataPoint.year);
  const set4A = selection.map(dataPoint => dataPoint.snowmelt_doy);
  const set4B = selection.map(dataPoint => dataPoint.avg_daily_prcp);
  const data4 = {
    labels: labels4,
    datasets: [
      {
        label: 'Last Day of Snow',
        data: set4A,
        backgroundColor: 'rgba(255, 99, 132, 0.5)',
      },
      {
        label: 'Average Daily Percipitation (mm)',
        data: set4B,
        backgroundColor: 'rgba(53, 162, 235, 0.5)',
      },
    ],
  };

  const labels5 = hyp5.map(dataPoint => dataPoint.REGION);
  const set5A = hyp5.map(dataPoint => dataPoint.h1_support_pct);
  const set5B = hyp5.map(dataPoint => dataPoint.h2_support_pct);
  const set5C = hyp5.map(dataPoint => dataPoint.h4_support_pct);
  const data5 = {
    labels: labels5,
    datasets: [
      {
        label: 'H1 Support',
        data: set5A,
        backgroundColor: 'rgba(255, 99, 132, 0.5)',
      },
      {
        label: 'H2 Support',
        data: set5B,
        backgroundColor: 'rgba(53, 162, 235, 0.5)',
      },
      {
        label: 'H4 Support',
        data: set5C,
        backgroundColor: 'rgba(38, 185, 9, 0.5)',
      },
    ],
  };

  let BestData;
  if (hypothesis == "h2") {
    BestData = data2;
  } else if (hypothesis == "h3") {
    BestData = data3;
  } else if (hypothesis == "h4") {
    BestData = data4;
  } else {
    BestData = data1;
  }

  return (
    <div id="main">
      <Header>
          <div className="dropdown">
            <label for="hypothesisSelect">Hypothesis: </label>
            <select name="hypothsisSelect" id="hypothsisSelect">
                <option value="h1" onClick={() => setHypothesis("h1")}>H1</option>
                <option value="h2" onClick={() => setHypothesis("h2")}>H2</option>
                <option value="h3" onClick={() => setHypothesis("h3")}>H3</option>
                <option value="h4" onClick={() => setHypothesis("h4")}>H4</option>
            </select>
        </div>
        <Select className="search-bar" onInputChange={onSelectChange} onChange={onSelect} options={options} isClearable={true} placeholder="Search by station name or ID"/>
      </Header>
      <div id="content-holder">
        <div id="map-holder">
          <Map hypothesis={hypothesis} results={results}/>
        </div>
        <div id="side-holder">
          <DisplayBox title="Analytical Hypothesises">
            <p><strong>Hypothesis 1:</strong> Dry early winter results in wet late winter.</p>
            <p><strong>Hypothesis 2:</strong> Warmer springs increase frequency of summer heat waves.</p>
            <p><strong>Hypothesis 3:</strong> Higher wind speeds reduce daily temperature range.</p>
            <p><strong>Hypothesis 4:</strong> Earlier snowmelt is associated with altered spring precipitation</p>
          </DisplayBox>

          <DisplayBox title="Regional Comparison">
            <p><strong>Hypothesis 5:</strong> Climate relationships are directionally consistent across regions.</p>
             <div><Bar width={null} height={null} options={options} data={data5} /></div> 
          </DisplayBox>
        </div>
      </div>

      {popup && <Popup>
        <div class="popup-holder">
          <div class="popup-header">Hypothesis: {hypothesis.toUpperCase()}</div>
          { selection.length > 0 ? <div id="graph"><Bar width={null} height={null} options={options} data={BestData} /></div> : <div id="graph-sad">No data for this station.</div>} 
        </div>
        <button id="DIE" onClick={() => setPopup(false)}>×</button>
      </Popup>}
      
    </div>
  )};


export default App;

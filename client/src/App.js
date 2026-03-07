import Map from "./components/map.js"
import Header from "./components/header.js"
import DisplayBox from "./components/display-box.js"

import './styles/app.css'

//import logo from './logo.svg';
import axios from 'axios';
import React, {useState, useEffect} from 'react';

function App() {
  return (
    <div id="main">
      <Header/>
      <div id="content-holder">
        <div id="map-holder">
          <Map/>
        </div>
        <div id="side-holder">
          <DisplayBox title="Analytical Hypothesises">
            <p><strong>Hypothesis 1:</strong> Dry early winter results in wet late winter.</p>
            <p><strong>Hypothesis 2:</strong> Warmer springs increase frequency of summer heat waves.</p>
            <p><strong>Hypothesis 3:</strong> Higher wind speeds reduce daily temperature range.</p>
            <p><strong>Hypothesis 4:</strong> Earlier snowmelt is associated with altered spring precipitation</p>
          </DisplayBox>
        </div>
      </div>
      
    </div>
  )};


export default App;

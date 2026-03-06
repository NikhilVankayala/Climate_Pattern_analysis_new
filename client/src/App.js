import Map from "./components/map.js"

//import logo from './logo.svg';
import axios from 'axios';
import React, {useState, useEffect} from 'react';

function App() {
  const [data, setData] = useState([]);

  /*useEffect(() => {
    axios
    .get(`http://localhost:9000/test`)
    .then(res => res.data)
    .then(data => setData(data));
  }, []);*/

  useEffect(() => {
    const fetchData = async () => {
      const res = await axios.get("http://localhost:9000/test");
      setData(res.data);
    }
    fetchData();
  }, []);

  return (
    <div id="test">
      <div><Map/></div>
      <div>
      {data.map((d) => (
          <p>{d.STATION_ID}</p>
        ))}
      </div>
      
    </div>
  )};


export default App;

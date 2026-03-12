import { MapContainer, TileLayer, UseMap, Marker, Popup } from 'react-leaflet'
import { latLng, latLngBounds } from 'leaflet';
import L from 'leaflet';

import Graph from './graph.js'

import greenIcon from '../styles/green-marker.svg'
import yellowIcon from '../styles/yellow-marker.svg'
import redIcon from '../styles/red-marker.svg'
import grayIcon from '../styles/gray-marker.svg'
import '../styles/map.css'
import '../styles/station.css'

import axios from 'axios';
import React, {useState, useEffect} from 'react';

const Station = ({data, hypothesis, result}) => {
    const [value, setValue] = useState("This station had insuficient data to be part of this hypothesis's calculations.");

    let icon = grayIcon;
    if(result) {
        if(hypothesis == "h2") {
            if(result.correlation_coefficient_sample) {
                if(result.correlation_coefficient_sample > 0.3) {
                    icon = greenIcon;
                } else if (result.correlation_coefficient_sample < -0.3) {
                    icon = redIcon;
                } else {
                    icon = yellowIcon;
                }
            }
        } else if(hypothesis == "h3") {
            let ratio = result.corr_days / result.total_days;

            if(ratio > 0.6) {
                icon = greenIcon;
            } else if (ratio < 0.4) {
                icon = redIcon;
            } else {
                icon = yellowIcon;
            }
        } else if(hypothesis == "h4") {
            if(result.correlation_coefficient_sample > 0.3) {
                icon = redIcon;
            } else if (result.correlation_coefficient_sample < -0.3) {
                icon = greenIcon;
            } else {
                icon = yellowIcon;
            }
        } else {
            if(result.correlation_coefficient_sample > 0.3) {
                icon = redIcon;
            } else if (result.correlation_coefficient_sample < -0.3) {
                icon = greenIcon;
            } else {
                icon = yellowIcon;
            }
        }
    }

    const position = [Number(data.LATITUDE), Number(data.LONGITUDE)];

    const customIcon = L.icon({
        iconUrl: icon,
        iconSize: [18, 14], // Custom [width, height]
        iconAnchor: [9, 14],
    });

    const handleMarkerClick = async (e) => {
        if(result) {
            if(hypothesis == "h2") {
                const res = await axios.get("http://localhost:9000/stationData2", {params: {"id": data.STATION_ID}});
                let temp = "Baseline spring temperature: " + Number(res.data[0].baseline_spring_temp).toPrecision(4) +"°C\n";
                temp += "\tStandard deviation: " + Number(res.data[0].std_spring_temp).toPrecision(4)  + "\n";
                temp += "There were " + res.data[0].hot_years + " years with extreme temperatures\n";
                temp += "This was measured over " + res.data[0].num_years  + " years\n";

                setValue(temp);
            } else if(hypothesis == "h3") {
                const res = await axios.get("http://localhost:9000/stationData3", {params: {"id": data.STATION_ID}});
                let temp = "There were " + res.data[0].windy_days + " days of high winds\n";
                temp += res.data[0].effected_days + " of those days correltated to reduced daily temperature range\n";

                setValue(temp);
            } else if(hypothesis == "h4") {
                const res = await axios.get("http://localhost:9000/stationData4", {params: {"id": data.STATION_ID}});
                let temp = "Baseline day of snowmelt: " + Number(res.data[0].baseline_melt_doy).toPrecision(4) + "\n";
                temp += "\tStandard deviation: " + Number(res.data[0].std_melt_doy).toPrecision(4) + "\n";
                 temp += "Baseline spring percipitation: " + Number(res.data[0].baseline_spring_prcp).toPrecision(4) + "\n";
                temp += "\tStandard deviation: " + Number(res.data[0].std_spring_prcp).toPrecision(4) + "\n";
                temp += "This was measured over " + res.data[0].num_years  + " years\n";

                setValue(temp);
            } else {
                const res = await axios.get("http://localhost:9000/stationData1", {params: {"id": data.STATION_ID}});
                let temp = "Baseline percipitation for early winter: " + Number(res.data[0].baseline_early).toPrecision(4) +"mm\n";
                temp += "\tStandard deviation: " + Number(res.data[0].std_early).toPrecision(4)  + "\n";
                temp += "Baseline percipitation for late winter: " + Number(res.data[0].baseline_late).toPrecision(4)  +"mm\n";
                temp += "\tStandard deviation: " + Number(res.data[0].std_late).toPrecision(4)  + "\n";
                temp += "This was measured over " + res.data[0].num_years  + " years\n";

                setValue(temp);
            }
        }
        
    };

    return(
    <Marker position={position} icon={customIcon} eventHandlers={{click: handleMarkerClick}}>
      <Popup className="request-popup">
        <div className="station-text">
            <div className="station-details">Station Details</div>
            <div className="station-main">
                <span className="station-info"><strong>Station:</strong> {data.NAME} <br/> </span>
                <span className="station-info"><strong>ID:</strong> {data.STATION_ID} <br/> </span>
            </div>
            <div id="divider"></div>
            <div className="station-main">
                <div className="yearly-trends">Yearly Trends</div>
                <pre class="popup-value">{value}</pre>
                <Graph id={data.STATION_ID}></Graph>
            </div>
            
        </div>
      </Popup>
    </Marker>
    );
};

export default Station;
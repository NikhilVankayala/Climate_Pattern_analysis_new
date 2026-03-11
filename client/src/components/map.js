import { MapContainer, TileLayer, UseMap } from 'react-leaflet'
import { latLng, latLngBounds } from 'leaflet';

import Station from "./station.js"

import '../styles/map.css'

import axios from 'axios';
import React, {useState, useEffect} from 'react';

const position = [39.8, -98.6]

const Map = ({hypothesis, results}) => {
    const [someMarkers, setSomeMarkers] = useState([]);
    const [allMarkers, setAllMarkers] = useState([]);

    useEffect(() => {
        const fetchData = async () => {
            const res = await axios.get("http://localhost:9000/stations");
            setSomeMarkers(res.data);
        }
        fetchData();
    }, []);

    return(
    <div id='map'>
        <MapContainer center={position} zoom={4.75} scrollWheelZoom={false}>
            <TileLayer
            url="http://{s}.tile.osm.org/{z}/{x}/{y}.png"
            attribution='&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors'
            />
            <div id='markers'>
                {someMarkers.map((d) => (<Station data={d} hypothesis={hypothesis} result={results.find((result) => d.STATION_ID == result.STATION)}></Station>))}
            </div>
        </MapContainer>
    </div>
    );
};

export default Map;
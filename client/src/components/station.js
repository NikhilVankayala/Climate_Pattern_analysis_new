import { MapContainer, TileLayer, UseMap, Marker, Popup } from 'react-leaflet'
import { latLng, latLngBounds } from 'leaflet';
import L from 'leaflet';

import Graph from './graph.js'

import icon from '../styles/marker.svg'
import '../styles/map.css'

import axios from 'axios';
import React, {useState, useEffect} from 'react';

const Station = ({data}) => {
    const position = [Number(data.LATITUDE), Number(data.LONGITUDE)];

    const customIcon = L.icon({
        iconUrl: icon,
        iconSize: [30, 41], // Custom [width, height]
        iconAnchor: [14.5, 35],
    });

    return(
    <Marker position={position} icon={customIcon}>
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
                <Graph id={data.STATION_ID}></Graph>
            </div>
            
        </div>
      </Popup>
    </Marker>
    );
};

export default Station;
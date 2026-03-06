import { MapContainer, TileLayer, UseMap, Marker, Popup } from 'react-leaflet'
import { latLng, latLngBounds } from 'leaflet';
import L from 'leaflet';

import icon from '../styles/marker.svg'
import '../styles/map.css'
import 'leaflet/dist/leaflet.css';

import axios from 'axios';
import React, {useState, useEffect} from 'react';

const Station = ({data}) => {
    const position = [Number(data.LATITUDE), Number(data.LONGITUDE)];

    const customIcon = L.icon({
        iconUrl: icon,
        iconSize: [35, 46], // Custom [width, height]
        iconAnchor: [17, 46]
    });

    return(
    <Marker position={position} icon={customIcon}>
      <Popup>
        <p>{data.NAME}</p>
      </Popup>
    </Marker>
    );
};

export default Station;
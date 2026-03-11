import '../styles/popup.css'

import { Bar } from "react-chartjs-2";


const Popup = ({children}) => {
    return(
        <div className="popup-dark">
            <div className="popup-body">
                {children}
            </div>
        </div>
    );
};

export default Popup;
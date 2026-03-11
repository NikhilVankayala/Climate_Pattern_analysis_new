import FilterButton from "./filter-button.js"

import '../styles/header.css'

const Header = ({children}) => {
    return(
    <div id="background-bar">
        <div>Weather Station Analysis</div>
        <div className="header-children">{children}</div>
    </div>
    );
};

export default Header;
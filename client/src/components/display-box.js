import '../styles/display-box.css'

const DisplayBox = ({children, title}) => {
    return(
    <div className="shape">
        <div className="display-header">{title}</div>
        <div className="display-box-body">
            {children}
        </div>
    </div>
    );
};

export default DisplayBox;
import '../styles/filter-button.css'

const FilterButton = ({children}) => {
    return (
        <div className="dropdown">
            <label for="hypothesisSelect">Hypothesis: </label>
            <select name="hypothsisSelect" id="hypothsisSelect">
                <option value="h1">H1</option>
                <option value="h2">H2</option>
                <option value="h3">H3</option>
                <option value="h4">H4</option>
            </select>
        </div>
  );
};

export default FilterButton;
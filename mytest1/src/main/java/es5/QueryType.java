package es5;

/**
 * @author yuan.li
 */
public enum QueryType {
	    
	    TERM("term"),
	    NOT_TERM("not_term"),
	    MATCH("match"),
	    WILDCARD("wildcard"),
	    RANGE("range"),
	    GREATER("greater"),
	    LESS("less"),
	    MIN("min");
	    private final String type;

	    QueryType(String type) {
	        this.type = type;
	    }

	    public String getType() {
	        return type;
	    }
}

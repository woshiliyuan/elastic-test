package es5;

import java.io.Serializable;

/**
 * @author yuan.li
 */
public class QueryConditions implements Serializable {

	private static final long serialVersionUID = 1L;

	private QueryType queryType;
	private String name;
	private String value;
	private String fromValue;
	private String toValue;
	
	public QueryType getQueryType() {
		return queryType;
	}
	public void setQueryType(QueryType queryType) {
		this.queryType = queryType;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public String getFromValue() {
		return fromValue;
	}
	public void setFromValue(String fromValue) {
		this.fromValue = fromValue;
	}
	public String getToValue() {
		return toValue;
	}
	public void setToValue(String toValue) {
		this.toValue = toValue;
	}
}

package edu.buffalo.cse562.operators;


public interface Operator {

	public void reset();
	public Object[] readOneTuple();
	
	public String getTableName();
}

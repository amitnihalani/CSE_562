package edu.buffalo.cse562.operators;

import net.sf.jsqlparser.schema.Table;


public interface Operator {

	public void reset();
	public Object[] readOneTuple();
	
	public Table getTable();
}

package edu.buffalo.cse562.operators;

public class RelationOperator extends Operator {

	private String table = null;
	
	public RelationOperator(String table){
		this.setTable(table);
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}
}

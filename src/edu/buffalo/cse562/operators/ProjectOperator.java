package edu.buffalo.cse562.operators;

import java.util.ArrayList;

import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import edu.buffalo.cse562.table.Column;

public class ProjectOperator implements Operator{

	ArrayList<Column> columns;
	Operator op;
	Object[] tuple;
	ArrayList<SelectExpressionItem> project;
	
	ProjectOperator(ArrayList<Column> col, Operator op, ArrayList<SelectExpressionItem> p){
		this.columns = col;
		this.op = op;
		this.tuple = null;
		this.project = p;
	}
	
	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object[] readOneTuple() {
		// TODO Auto-generated method stub
		Object[] temp = op.readOneTuple();
		ArrayList<Object> tempList = new ArrayList<Object>();
		for(Object col: temp){
			
		}
		
		return tuple;
	}

}

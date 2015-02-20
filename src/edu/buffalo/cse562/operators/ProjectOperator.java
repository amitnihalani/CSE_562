package edu.buffalo.cse562.operators;

import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import edu.buffalo.cse562.utility.Utility;

public class ProjectOperator implements Operator{

	Operator op;
	Object[] tuple;
	ArrayList<SelectExpressionItem> toProject;
	String tableName;
	HashMap<String, Integer> schema;
	
	ProjectOperator(Operator op, ArrayList<SelectExpressionItem> p, String table){

		this.op = op;
		this.tuple = null;
		this.toProject = p;
		this.tableName = table;
		this.schema = Utility.tables.get(table);
	}
	
	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object[] readOneTuple() {
		// TODO Auto-generated method stub
		Object[] temp = op.readOneTuple();
		if(temp == null)
			return null;
		ArrayList<Object> tempList = new ArrayList<Object>();
		for(SelectExpressionItem e: toProject){
			int colID = schema.get(e.toString());
			tempList.add(temp[colID]);
		}
		tuple = tempList.toArray();
		return tuple;
	}

}

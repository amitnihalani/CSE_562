package edu.buffalo.cse562.operators;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import edu.buffalo.cse562.evaluate.Evaluator;
import edu.buffalo.cse562.utility.Utility;

public class ProjectOperator implements Operator{

	Operator op;
	Object[] tuple;
	ArrayList<SelectExpressionItem> toProject;
	String tableName;
	HashMap<String, Integer> schema;
	
	ProjectOperator(Operator op, ArrayList<SelectExpressionItem> p, String table){

		this.op = op;
		this.tuple = new Object[p.size()];
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
		Evaluator eval = new Evaluator(schema,temp);
		
		int index = 0;
		if(temp == null)
			return null;
		for(SelectExpressionItem e: toProject){
			try {
				tuple[index] = eval.eval(e.getExpression());
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			index++;
		}
		return tuple;
	}

}

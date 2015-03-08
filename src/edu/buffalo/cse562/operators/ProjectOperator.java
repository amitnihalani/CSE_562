package edu.buffalo.cse562.operators;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import edu.buffalo.cse562.evaluate.Evaluator;
import edu.buffalo.cse562.utility.Utility;

public class ProjectOperator implements Operator{

	Operator op;
	Object[] tuple;
	ArrayList<SelectExpressionItem> toProject;
	Table table;
	HashMap<String, Integer> schema;
	boolean allColumns;
	
	ProjectOperator(Operator op, ArrayList<SelectExpressionItem> p, Table table, boolean allColumns){

		this.op = op;
		this.tuple = new Object[p.size()];
		this.toProject = p;
		this.table = table;
		this.schema = Utility.tables.get(table.getAlias());
		this.allColumns=allColumns;
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
		if(allColumns)
			return temp;
		for(SelectExpressionItem e: toProject){
			try {
				if(e.getExpression() instanceof Function){
					Expression x = new Column(null, e.getExpression().toString());
					tuple[index] = eval.eval(x);
				}else{
					tuple[index] = eval.eval(e.getExpression());
				}
			} catch (SQLException e1) {
				System.out.println("Exception in ProjectOperator.readOneTuple()");
			}
			index++;
		}
		return tuple;
	}


	@Override
	public Table getTable() {
		// TODO Auto-generated method stub
		return table;
	}

}

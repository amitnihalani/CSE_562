/**
 * 
 */
package edu.buffalo.cse562.operators;


import java.sql.SQLException;
import java.util.HashMap;

import edu.buffalo.cse562.evaluate.Evaluator;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;



public class SelectionOperator implements Operator {

	Operator op;
	HashMap<String, Integer> schema;
	Expression condition;

	public SelectionOperator(Operator input, HashMap<String, Integer> schema, Expression condition) {

		this.op=input;
		this.schema=schema;
		this.condition=condition;

	}
	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.operators.Operator#reset()
	 */
	@Override
	public void reset() {
		op.reset();
	}

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.operators.Operator#readOneTuple()
	 */
	@Override
	public Object[] readOneTuple() {
		// TODO Auto-generated method stub
		Object[] tuple=  null;
		tuple=op.readOneTuple();
		if(tuple == null)
		{
			return null;
		}
		Evaluator eval= new Evaluator(schema,tuple);
		try {
			BooleanValue b= (BooleanValue)eval.eval(condition);
			if(b.getValue())
			{
				return tuple;
			}
			else
			{
				tuple = readOneTuple();
				if(tuple == null)
				{
					return null;
				}
				return tuple;
			}
		} catch (SQLException e) {

			System.out.println("Exception occured in SelectionOperator.readOneTuple()");
		}
		return null;
	}

	@Override
	public String getTableName() {
		// TODO Auto-generated method stub
		return null;
	}
}

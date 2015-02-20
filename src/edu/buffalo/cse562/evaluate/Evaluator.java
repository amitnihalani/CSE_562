package edu.buffalo.cse562.evaluate;

import java.sql.SQLException;
import java.util.HashMap;

import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.Eval;

public class Evaluator extends Eval{

	private HashMap<String, Integer> schema;
	private Object[] tuple;
	public Evaluator(HashMap<String, Integer> table, Object[] tuple)
	{
		this.schema=table;
		this.tuple=tuple;
	}
	public LeafValue eval(Column c) throws SQLException {
		
		int columnId = schema.get(c.getColumnName());
		return (LeafValue) tuple[columnId];
		
			
	}
	
}

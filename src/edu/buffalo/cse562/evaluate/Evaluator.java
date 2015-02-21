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
		String t = "";
		int columnID = 0;
		if(c.getTable().getName() != null)
			t = c.getTable().getName();
		else{
			for(String key: schema.keySet()){
				String[] x = key.split("\\.");
				if(x[1].equals(c.getColumnName())){
					columnID = schema.get(key);
					return (LeafValue) tuple[columnID];
				}
			}
		}
		columnID = schema.get(t+"."+c.getColumnName());
		return (LeafValue) tuple[columnID];
		
			
	}
	
}

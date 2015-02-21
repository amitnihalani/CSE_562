package edu.buffalo.cse562.operators;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import edu.buffalo.cse562.utility.Utility;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

public class OperatorTest {

	public static void executeSelect(File file, String tableName, Expression condition,ArrayList<SelectExpressionItem> list){
		Operator oper = new ReadOperator(file, tableName);
		oper= new SelectionOperator(oper, Utility.tables.get(tableName), condition);
		oper = new ProjectOperator(oper, list, tableName);
		dump(oper);
	}
	
	public static void executeUnion(ArrayList<ArrayList<Object>> selectStatementsParameters){
		ArrayList<Operator> oper = new ArrayList<Operator>();
		
		for(ArrayList<Object> statementParameters : selectStatementsParameters){
			Operator o = new ReadOperator(new File((String)statementParameters.get(0)), (String)statementParameters.get(1));
			if((Expression)statementParameters.get(2) != null){
				o = new SelectionOperator(o, Utility.tables.get((String)statementParameters.get(1)), (Expression)statementParameters.get(2));
			}
			
			o = new ProjectOperator(o, (ArrayList<SelectExpressionItem>)statementParameters.get(3), (String)statementParameters.get(1));
		    oper.add(o);
		}
		dumpUnion(oper);
	}
	
	public static void dumpUnion(ArrayList<Operator> oper){
		HashSet<String> unionTuples = new HashSet<String>();
		for(Operator op : oper){
			Object[] row = op.readOneTuple();
			while(row != null){
				String tuple = "";
				for(Object col: row){
					tuple += (col.toString()+" | ");
				}
				if(!unionTuples.contains(tuple)){
					unionTuples.add(tuple);
					System.out.println(tuple);
				}
				row = op.readOneTuple();
			}
		}
	}
	
	public static void dump(Operator input){
		Object[] row = input.readOneTuple();
		while(row != null){
			for(Object col: row){
				System.out.print(col.toString()+" | ");
			}
			System.out.println();
			row = input.readOneTuple();
		}
	}
}

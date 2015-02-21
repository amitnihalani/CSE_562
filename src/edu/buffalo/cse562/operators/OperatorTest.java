package edu.buffalo.cse562.operators;

import java.io.File;
import java.util.ArrayList;

import edu.buffalo.cse562.utility.Utility;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

public class OperatorTest {

	public static void execute(File file, String tableName, Expression condition,
			ArrayList<SelectExpressionItem> list, ArrayList<Join> joins){
		Operator oper = new ReadOperator(file, tableName);
		if(joins != null)
//			oper = new JoinOperator(oper, joins, tableName);
			oper = new CrossProductOperator(oper, joins, tableName);
		if(condition != null)
			oper= new SelectionOperator(oper, Utility.tables.get(tableName), condition);
		oper = new ProjectOperator(oper, list, tableName);
		dump(oper);
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

package edu.buffalo.cse562.operators;

import java.io.File;
import java.util.ArrayList;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

public class OperatorTest {

	public static void execute(File file, String tableName, ArrayList<SelectExpressionItem> list){
		Operator oper = new ReadOperator(file, tableName);
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

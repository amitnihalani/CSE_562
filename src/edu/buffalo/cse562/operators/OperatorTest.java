package edu.buffalo.cse562.operators;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;

import edu.buffalo.cse562.utility.Utility;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

public class OperatorTest {

	public static void executeSelect(File file, String tableName, Expression condition,ArrayList<SelectExpressionItem> list, ArrayList<Join> joins){
		Operator oper = new ReadOperator(file, tableName);
		if(joins != null){
			oper = new CrossProductOperator(oper, joins, tableName);
			tableName = oper.getTableName();
		}
		if(condition != null)
			oper= new SelectionOperator(oper, Utility.tables.get(tableName), condition);
		
		if(list.get(0).getExpression() instanceof Function)
		{
			Function f=(Function) list.get(0).getExpression();
			System.out.println(f.getName());
			System.out.println("All columns"+ f.isAllColumns());
			if(f.isAllColumns())
				oper=new AggregateOperator(oper, Utility.tables.get(tableName), f.getName());
			else
			oper=new AggregateOperator(oper, Utility.tables.get(tableName), (Expression) f.getParameters().getExpressions().get(0),f.getName());
		}
		
		oper = new ProjectOperator(oper, list, tableName);
		dump(oper);
	}
	
	@SuppressWarnings("unchecked")
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

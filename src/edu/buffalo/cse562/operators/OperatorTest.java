package edu.buffalo.cse562.operators;

import java.io.File;
import java.util.ArrayList;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import edu.buffalo.cse562.utility.Utility;

public class OperatorTest {

	static boolean isAggregate;
	static boolean isJoin;

	public static Operator executeSelect(Operator op, String tableName, Expression condition,ArrayList<SelectExpressionItem> list, ArrayList<Join> joins, ArrayList<Expression> grpByColumnRef, Expression having, boolean allColumns, Limit limit){
		isAggregate = false;
		isJoin = false;
		ArrayList<String> joinTables = new ArrayList<String>();
//		Operator oper = new ReadOperator(file, tableName);
		Operator oper = op;
		ArrayList<Function> functions=null;
		Expression onExpression = null;

		if(joins != null){
			if(joins.get(0).toString().contains("JOIN")){
				isJoin = true;
				joinTables.add(joins.get(0).getRightItem().toString());
				onExpression = joins.get(0).getOnExpression();
			}else{
				for(int i = 0; i<joins.size(); i++){
					Table t = (Table) joins.get(i).getRightItem();
					joinTables.add(t.getName());
				}
			}
			oper = new CrossProductOperator(oper, joinTables, tableName.toUpperCase());
			tableName = oper.getTableName();
		}
		if(onExpression != null){
			oper = new SelectionOperator(oper, Utility.tables.get(tableName.toUpperCase()), onExpression);
		}
		if(condition != null)
			oper= new SelectionOperator(oper, Utility.tables.get(tableName.toUpperCase()), condition);		
		if(!allColumns)
		{		
			functions= new ArrayList<Function>();

			for(int i=0; i<list.size(); i++){
				if(list.get(i).getExpression() instanceof Function){
					functions.add((Function) list.get(i).getExpression());
					isAggregate = true;
				}				
			}
		}

		if(grpByColumnRef!=null){
			oper = new GroupByOperator(oper, tableName.toUpperCase(), list, functions, grpByColumnRef, having);
			oper = new ProjectOperator(oper, list, oper.getTableName().toUpperCase(),allColumns);
		}
		else if (isAggregate)
			oper=new AggregateOperator(oper, Utility.tables.get(tableName.toUpperCase()), functions);
		else
			oper = new ProjectOperator(oper, list, tableName.toUpperCase(),allColumns);
		return oper;
	}

	@SuppressWarnings("unchecked")
	public static void executeUnion(ArrayList<ArrayList<Object>> selectStatementsParameters){
		ArrayList<Operator> oper = new ArrayList<Operator>();

		for(ArrayList<Object> statementParameters : selectStatementsParameters){
			Operator o = new ReadOperator(new File((String)statementParameters.get(0)), (String)statementParameters.get(1));
			if((Expression)statementParameters.get(2) != null){
				o = new SelectionOperator(o, Utility.tables.get((String)statementParameters.get(1)), (Expression)statementParameters.get(2));
			}

			o = new ProjectOperator(o, (ArrayList<SelectExpressionItem>)statementParameters.get(3), (String)statementParameters.get(1), false);
			oper.add(o);
		}
		dumpUnion(oper);
	}

	public static void dumpUnion(ArrayList<Operator> oper){
		ArrayList<String> unionTuples = new ArrayList<String>();
		for(Operator op : oper){
			Object[] row = op.readOneTuple();
			while(row != null){
				String tuple = "";
				int i = 0;
				for(i = 0; i<row.length-1; i++){
					tuple += row[i]+"|";
				}
				tuple = tuple+row[i];
				unionTuples.add(tuple);
				System.out.println(tuple);
				row = op.readOneTuple();
			}
		}
	}

	public static void dump(Operator input,Limit limit){
		if(input instanceof AggregateOperator){
			Object[] row = input.readOneTuple();
			int i = 0;
			for(i=0; i<row.length-1; i++){
				System.out.print(row[i].toString()+"|");
			}
			System.out.print(row[i].toString());
		}
		else{
			if(limit!=null)
			{

				Object[] row = input.readOneTuple();
				long rowsToPrint=1;
				while(row != null && rowsToPrint<=limit.getRowCount()){
					int i = 0;
					for(i=0; i<row.length-1; i++){
						if(row[i] instanceof StringValue)
							System.out.print(((StringValue)row[i]).getNotExcapedValue() + "|");
						else
							System.out.print(row[i]+"|");
					}
					System.out.print(row[i].toString());
					System.out.println();
					rowsToPrint++;
					row = input.readOneTuple();
				}
			}
			else
			{
				Object[] row = input.readOneTuple();
				while(row != null){
					int i = 0;
					for(i=0; i<row.length-1; i++){
						if(row[i] instanceof StringValue)
							System.out.print(((StringValue)row[i]).getNotExcapedValue() + "|");
						else
							System.out.print(row[i]+"|");
					}
					System.out.print(row[i].toString());
					System.out.println();
					row = input.readOneTuple();
				}
			}

		}
	}
}

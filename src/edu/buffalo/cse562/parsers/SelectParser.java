package edu.buffalo.cse562.parsers;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;
import edu.buffalo.cse562.operators.Operator;
import edu.buffalo.cse562.operators.OperatorTest;
import edu.buffalo.cse562.operators.ReadOperator;
import edu.buffalo.cse562.utility.Utility;

public class SelectParser {

	@SuppressWarnings("unchecked")
	/**
	 * Parse a Select statement, and execute it.
	 * @param statement
	 */
	public static void parseStatement(Statement statement){
		SelectBody body = ((Select) statement).getSelectBody();		

		if(body instanceof PlainSelect){
			Operator oper = getOperator((PlainSelect)body);
			OperatorTest.dump(oper, ((PlainSelect) body).getLimit());
//			OperatorTest.executeSelect(new File((String)parameters.get(0)), 
//					(String)parameters.get(1), 
//					(Expression)parameters.get(2),
//					(ArrayList<SelectExpressionItem>)parameters.get(3),
//					(ArrayList<Join>) parameters.get(4),					
//					(ArrayList<Expression>)parameters.get(5),
//					(Expression)parameters.get(6),
//					(boolean) parameters.get(7),
//					(Limit)parameters.get(8));
		}
		else if(body instanceof Union){
			ArrayList<PlainSelect> plainSelects = new ArrayList<PlainSelect>(((Union) body).getPlainSelects());
			ArrayList<ArrayList<Object>> selectStatementsParameters = new ArrayList<ArrayList<Object>>(); 

			for(PlainSelect p : plainSelects){
				selectStatementsParameters.add(getParameters(p));
				SelectParser.populateAliases(p);
			}
			OperatorTest.executeUnion(selectStatementsParameters);
		}
	}

	public static String returnFromItem(SelectBody body){
		return ((PlainSelect) body).getFromItem().toString();
	}

	public static Expression returnConditionItem(SelectBody body){
		return ((PlainSelect) body).getWhere();
	}



	@SuppressWarnings("unchecked")
	public static ArrayList<Object> getParameters(PlainSelect body){
		//list of parameters in the sequence - From Item, Condition Item,  Select Items, Joins, GroupByColumnReference, Having, allColumns,Limit
		Table t = null;
		Operator op = null;
		ArrayList<Object> parameters = new ArrayList<Object>();
		boolean isSubSelect=false;
		if(body.getFromItem() instanceof SubSelect){
			ArrayList<Object> params = getParameters((PlainSelect) ((SubSelect) body.getFromItem()).getSelectBody());
			isSubSelect=true;
			Operator oper = new ReadOperator(new File((String)params.get(0)), (String) params.get(1));
			op = OperatorTest.executeSelect(oper,
					(String)params.get(1), 
					(Expression)params.get(2),
					(ArrayList<SelectExpressionItem>)params.get(3),
					(ArrayList<Join>) params.get(4),					
					(ArrayList<Expression>)params.get(5),
					(Expression)params.get(6),
					(boolean) params.get(7),
					(Limit)params.get(8));
			System.out.println(op.getClass());
			params = new ArrayList<Object>();
			params.add(op);
			params.add(op.getTableName());
			params.add(body.getWhere());
			params.add((ArrayList<SelectExpressionItem>)(body).getSelectItems());
			params.add(body.getJoins());
			params.add(body.getGroupByColumnReferences());
			params.add(body.getHaving());
			if(((PlainSelect) body).getSelectItems().get(0) instanceof AllColumns)
				params.add(true);
			else
				params.add(false);
			params.add(body.getLimit());
			op = OperatorTest.executeSelect((Operator) params.get(0),
					(String)params.get(1), 
					(Expression)params.get(2),
					(ArrayList<SelectExpressionItem>)params.get(3),
					(ArrayList<Join>) params.get(4),					
					(ArrayList<Expression>)params.get(5),
					(Expression)params.get(6),
					(boolean) params.get(7),
					(Limit)params.get(8));
			System.out.println(op.getClass());
			OperatorTest.dump(op, (Limit) params.get(8));
		}
		if(body.getFromItem() instanceof Table){
			t = (Table) body.getFromItem();
			if(t.getAlias() != null){
				Utility.tableAlias.put(t.getAlias(), t);
			}
			parameters.add(Utility.dataDir.toString() + File.separator + t.getName() + ".dat");
			parameters.add(t.getName());
			parameters.add(body.getWhere());
			parameters.add((ArrayList<SelectExpressionItem>)(body).getSelectItems());
			parameters.add(body.getJoins());
			parameters.add(body.getGroupByColumnReferences());
			parameters.add(body.getHaving());

			if(((PlainSelect) body).getSelectItems().get(0) instanceof AllColumns)
				parameters.add(true);
			else
				parameters.add(false);

			parameters.add(body.getLimit());
			return parameters;
		}
		return parameters;

	}


	public static void populateAliases(SelectBody body)
	{
		@SuppressWarnings("unchecked")
		ArrayList<SelectExpressionItem> selectItems=(ArrayList<SelectExpressionItem>)((PlainSelect) body).getSelectItems();
		Utility.alias=new HashMap<String, Expression>();
		if(((PlainSelect) body).getSelectItems().get(0) instanceof AllColumns)
			return;	
		for(SelectExpressionItem s : selectItems)
		{
			String alias=s.getAlias();
			/*if(alias == null)
			{
				s.setAlias(s.getExpression().toString());
				System.out.println(s.getAlias());
			}*/
			if(alias!=null)
				Utility.alias.put(s.getAlias(), s.getExpression());
		}
	}
	
	@SuppressWarnings("unchecked")
	public static Operator getOperator(PlainSelect body){
		Table t = null;
		Operator op = null;
		boolean allCol = false;
		if(body.getFromItem() instanceof SubSelect){
			op = getOperator((PlainSelect) ((SubSelect) body.getFromItem()).getSelectBody());
			op = OperatorTest.executeSelect(op,
				op.getTableName(),
				body.getWhere(),
				(ArrayList<SelectExpressionItem>)body.getSelectItems(),
				(ArrayList<Join>)body.getJoins(),
				(ArrayList<Expression>)body.getGroupByColumnReferences(),
				body.getHaving(),
				allCol,
				body.getLimit());
			return op;
		}
		else{
			SelectParser.populateAliases(body);
			t = (Table) body.getFromItem();
			if(t.getAlias() != null){
				Utility.tableAlias.put(t.getAlias(), t);
			}
			if(((PlainSelect) body).getSelectItems().get(0) instanceof AllColumns)
					allCol = true;
				else
					allCol = false;

			String tableFile = Utility.dataDir.toString() + File.separator + t.getName() + ".dat";
			Operator readOp = new ReadOperator(new File(tableFile), t.getName());
			op = OperatorTest.executeSelect(readOp,
				t.getName(),
				body.getWhere(),
				(ArrayList<SelectExpressionItem>)body.getSelectItems(),
				(ArrayList<Join>)body.getJoins(),
				(ArrayList<Expression>)body.getGroupByColumnReferences(),
				body.getHaving(),
				allCol,
				body.getLimit());
			return op;
		}
	}
}
package edu.buffalo.cse562.parsers;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.Union;
import edu.buffalo.cse562.operators.OperatorTest;
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
			ArrayList<Object> parameters = getParameters((PlainSelect)body);
			SelectParser.populateAliases(body);
			OperatorTest.executeSelect(new File((String)parameters.get(0)), 
					(String)parameters.get(1), 
					(Expression)parameters.get(2),
					(ArrayList<SelectExpressionItem>)parameters.get(3),
					(ArrayList<Join>) parameters.get(4),					
					(ArrayList<Expression>)parameters.get(5),
					(Expression)parameters.get(6),
					(boolean) parameters.get(7),
					(Limit)parameters.get(8));
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
		//list of parameters in the sequence - From Item, Condition Item,  Select Items, DataFileName, Joins, GroupByColumnReference, Having, allColumns,Limit
		boolean allColumns=false;
		
		ArrayList<Object> parameters = new ArrayList<Object>();
		parameters.add(Utility.dataDir.toString() + File.separator + (body).getFromItem().toString() + ".dat");
		parameters.add((body).getFromItem().toString());
		parameters.add((body).getWhere());
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
	
}
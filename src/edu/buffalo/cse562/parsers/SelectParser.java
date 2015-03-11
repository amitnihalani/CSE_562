package edu.buffalo.cse562.parsers;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.Join;
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

	/**
	 * Parse a Select statement, and execute it.
	 * @param statement
	 */
	public static void parseStatement(Statement statement){
		SelectBody body = ((Select) statement).getSelectBody();		

		if(body instanceof PlainSelect){
			Operator oper = getOperator((PlainSelect)body);
			OperatorTest.dump(oper, ((PlainSelect) body).getLimit());
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

	@SuppressWarnings("unchecked")
	public static ArrayList<Object> getParameters(PlainSelect body){
		//list of parameters in the sequence - From Item, Condition Item,  Select Items, Joins, GroupByColumnReference, Having, allColumns,Limit
		Table t = null;
		ArrayList<Object> parameters = new ArrayList<Object>();
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
			if(alias == null)
			{
				s.setAlias(s.getExpression().toString());
				Utility.alias.put(s.getAlias(), s.getExpression());
			}
			else if(alias!=null)
				Utility.alias.put(s.getAlias(), s.getExpression());
		}
	}

	@SuppressWarnings("unchecked")
	public static Operator getOperator(PlainSelect body){
		Table t = null;
		Operator op = null;
		boolean allCol = false;
		if(body.getFromItem() instanceof SubSelect){
			t = new Table();
			if(body.getFromItem().getAlias() == null){
				t.setName("SubQuery");
				t.setAlias("SubQuery");
				
			}
			else{
				t.setName(body.getFromItem().getAlias());
				t.setAlias(body.getFromItem().getAlias());
			}
			createSchema((ArrayList<SelectExpressionItem>) ((PlainSelect) ((SubSelect) body.getFromItem()).getSelectBody()).getSelectItems(), t);
			op = getOperator((PlainSelect) ((SubSelect) body.getFromItem()).getSelectBody());
			op = OperatorTest.executeSelect(op,
					t,
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
			checkTableAlias(t);
			if(((PlainSelect) body).getSelectItems().get(0) instanceof AllColumns)
				allCol = true;
			else
				allCol = false;

			String tableFile = Utility.dataDir.toString() + File.separator + t.getName() + ".dat";
			Operator readOp = new ReadOperator(new File(tableFile), t);
			op = OperatorTest.executeSelect(readOp,
					t,
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

	public static void checkTableAlias(Table t){
		if(t.getAlias() == null){
			t.setAlias(t.getName());
		}
		Utility.tableAlias.put(t.getAlias(), t);

		if(!Utility.tables.containsKey(t.getAlias())){
			HashMap<String, Integer> tempSchema = Utility.tables.get(t.getName());
			HashMap<String, Integer> newSchema = new HashMap<String, Integer>();
			for(String key: tempSchema.keySet()){
				String[] temp = key.split("\\.");
				newSchema.put(t.getAlias()+"."+temp[1], tempSchema.get(key));
			}
			Utility.tables.put(t.getAlias(), newSchema);
		}
	}

	public static void createSchema(ArrayList<SelectExpressionItem> selectItems, Table t){
		HashMap<String, Integer> schema = new HashMap<String, Integer>();
		for(int i = 0; i < selectItems.size(); i++){
			schema.put(selectItems.get(i).getExpression().toString(), i);
		}
		Utility.tables.put(t.getAlias(), schema);
	}
}
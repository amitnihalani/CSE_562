package edu.buffalo.cse562.parsers;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import edu.buffalo.cse562.operators.RelationOperator;
import edu.buffalo.cse562.operators.SelectOperator;

public class SelectParser {

	@SuppressWarnings("unchecked")
	public static void parseStatement(Statement statement){
		SelectBody body = ((Select) statement).getSelectBody();
		
		if(body instanceof PlainSelect){
			String from = ((PlainSelect) body).getFromItem().toString();
			Expression where = ((PlainSelect) body).getWhere();
			ArrayList<SelectExpressionItem> list = (ArrayList<SelectExpressionItem>)
					((PlainSelect) body).getSelectItems();
			createTree(from, where, list);
		}
	}

	private static void createTree(String from, Expression where,
			ArrayList<SelectExpressionItem> list) {
		// TODO Auto-generated method stub
		RelationOperator relation = new RelationOperator(from);
		SelectOperator select = new SelectOperator(where);
		relation.setParent(select);
		select.setLeft(relation);
	}
}
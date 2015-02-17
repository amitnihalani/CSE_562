package edu.buffalo.cse562.parsers;

import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;

public class SelectParser {

	public static void parseStatement(Statement statement){
		SelectBody body = ((Select) statement).getSelectBody();
		if(body instanceof PlainSelect){
			System.out.println(((PlainSelect) body).getFromItem());
			System.out.println(((PlainSelect) body).getWhere());
			System.out.println(((PlainSelect) body).getSelectItems());
		}
	}
}

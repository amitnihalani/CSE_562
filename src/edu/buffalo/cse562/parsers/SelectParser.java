package edu.buffalo.cse562.parsers;

import java.io.File;
import java.util.ArrayList;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import edu.buffalo.cse562.Main;
import edu.buffalo.cse562.operators.OperatorTest;

public class SelectParser {

	@SuppressWarnings("unchecked")
	public static void parseStatement(Statement statement){
		SelectBody body = ((Select) statement).getSelectBody();
		
		if(body instanceof PlainSelect){
			String from = ((PlainSelect) body).getFromItem().toString();
			Expression where = ((PlainSelect) body).getWhere();
			ArrayList<SelectExpressionItem> list = (ArrayList<SelectExpressionItem>)
					((PlainSelect) body).getSelectItems();
			String dataFileName = from.toLowerCase() + ".dat";
			dataFileName = Main.dataDir.toString() + File.separator + dataFileName;
			System.out.println(dataFileName);
			OperatorTest.execute(new File(dataFileName));
		}
	}
}
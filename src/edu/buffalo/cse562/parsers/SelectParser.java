package edu.buffalo.cse562.parsers;

import java.io.File;
import java.util.ArrayList;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
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
			String from = ((PlainSelect) body).getFromItem().toString();
			ArrayList<SelectExpressionItem> list = (ArrayList<SelectExpressionItem>)
					((PlainSelect) body).getSelectItems();
			String dataFileName = from.toLowerCase() + ".dat";
			dataFileName = Utility.dataDir.toString() + File.separator + dataFileName;
			
			OperatorTest.execute(new File(dataFileName), from, list);
		}
	}
}
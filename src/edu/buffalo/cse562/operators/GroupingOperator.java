package edu.buffalo.cse562.operators;

import net.sf.jsqlparser.expression.Expression;

public class GroupingOperator extends Operator{

	public GroupingOperator(Expression e){
		this.setExpression(e);
	}
}

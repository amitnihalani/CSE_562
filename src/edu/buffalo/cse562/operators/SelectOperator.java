package edu.buffalo.cse562.operators;

import net.sf.jsqlparser.expression.Expression;

public class SelectOperator extends Operator {
	
	public SelectOperator(Expression e){
		this.setExpression(e);
	}
}

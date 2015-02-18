package edu.buffalo.cse562.operators;

import net.sf.jsqlparser.expression.Expression;

public class ProjectOperator extends Operator {

	public ProjectOperator(Expression e){
		this.setExpression(e);
	}
}

package edu.buffalo.cse562.operators;

import net.sf.jsqlparser.expression.Expression;


public abstract class Operator {

	private Expression expression;
	private Operator left;
	private Operator right;
	private Operator parent;
	

	Operator(){
		this.expression = null;
		this.left = null;
		this.right = null;
		this.parent = null;
	}
	
	public Operator getLeft() {
		return left;
	}
	public void setLeft(Operator left) {
		this.left = left;
	}
	public Operator getRight() {
		return right;
	}
	public void setRight(Operator right) {
		this.right = right;
	}
	public Operator getParent() {
		return parent;
	}
	public void setParent(Operator parent) {
		this.parent = parent;
	}
	public Expression getExpression() {
		return expression;
	}
	public void setExpression(Expression e) {
		this.expression = e;
	}
	
}

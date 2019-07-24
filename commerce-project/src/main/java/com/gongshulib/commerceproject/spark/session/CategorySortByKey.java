package com.gongshulib.commerceproject.spark.session;


import java.io.Serializable;

import scala.math.Ordered;

/**
 * 自定义key二次排序
 * 实现Ordered接口，重写Ordered接口的方法
 * 只有compareTo方法有效，其他的方法不需要实现也可？
 * 测试结果无误
 * @author Administrator
 *
 */
public class CategorySortByKey implements Ordered<CategorySortByKey>, Serializable{
	
	private static final long serialVersionUID = 8612567242806500113L;

	private long clickCount;
	private long orderCount;
	private long payCount;
	
	public CategorySortByKey(long clickCount, long orderCount, long payCount) {
		super();
		this.clickCount = clickCount;
		this.orderCount = orderCount;
		this.payCount = payCount;
	}

	public long getClickCount() {
		return clickCount;
	}

	public void setClickCount(long clickCount) {
		this.clickCount = clickCount;
	}

	public long getOrderCount() {
		return orderCount;
	}

	public void setOrderCount(long orderCount) {
		this.orderCount = orderCount;
	}

	public long getPayCount() {
		return payCount;
	}

	public void setPayCount(long payCount) {
		this.payCount = payCount;
	}

	@Override
	public boolean $greater(CategorySortByKey other) {
		if(clickCount > other.getClickCount()){
			return true;
		}else if(clickCount == other.getClickCount() &&
				orderCount > other.getOrderCount()){
			return true;
		}else if(clickCount == other.getClickCount() &&
				orderCount == other.getOrderCount() &&
				payCount > other.getPayCount()){
			return true;
		}
		return false;
	}

	@Override
	public boolean $greater$eq(CategorySortByKey other) {
		if($greater(other)){
			return true;
		}else if(clickCount == other.getClickCount() &&
				orderCount == other.getOrderCount() && 
				payCount == other.getPayCount()){
			return true;
		}
		return false;
	}

	@Override
	public boolean $less(CategorySortByKey other) {
		if(clickCount < other.getClickCount()){
			return true;
		}else if(clickCount == other.getClickCount() &&
				orderCount < other.getOrderCount()){
			return true;
		}else if(clickCount == other.getClickCount() &&
				orderCount == other.getOrderCount() &&
				payCount < other.getPayCount()){
			return true;
		}
		return false;
	}

	@Override
	public boolean $less$eq(CategorySortByKey other) {
		if($less(other)){
			return true;
		}else if(clickCount == other.getClickCount() &&
				orderCount == other.getOrderCount() && 
				payCount == other.getPayCount()){
			return true;
		}
		return false;
	}

	@Override
	public int compare(CategorySortByKey other) {
		if(clickCount - other.getClickCount() != 0){
			return (int) (clickCount - other.getClickCount());
		}else if(orderCount - other.getOrderCount() != 0){
			return (int) (orderCount - other.getOrderCount());
		}else if(payCount - other.getPayCount() != 0){
			return (int) (payCount - other.getPayCount());
		}
		return 0;
	}

	@Override
	public int compareTo(CategorySortByKey other) {
		if(clickCount - other.getClickCount() != 0){
			return (int) (clickCount - other.getClickCount());
		}else if(orderCount - other.getOrderCount() != 0){
			return (int) (orderCount - other.getOrderCount());
		}else if(payCount - other.getPayCount() != 0){
			return (int) (payCount - other.getPayCount());
		}
		return 0;
	}
	
}

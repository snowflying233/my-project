package com.gongshulib.commerceproject.domain;

/**
 * 热门商品top10
 * @author Administrator
 *
 */
public class Top10Caregory {
	
	private long taskId;
	private long categoryId;
	private long clickCount;
	private long orderCount;
	private long payCount;
	
	public long getTaskId() {
		return taskId;
	}
	public void setTaskId(long taskId) {
		this.taskId = taskId;
	}
	public long getCategoryId() {
		return categoryId;
	}
	public void setCategoryId(long categoryId) {
		this.categoryId = categoryId;
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
	
	
}

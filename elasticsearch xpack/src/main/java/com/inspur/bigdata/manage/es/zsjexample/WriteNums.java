package com.inspur.bigdata.manage.es.zsjexample;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WriteNums {

	private int ketiCount;

	private int qlrCount;

	private int qlCount;

	private ReadWriteLock KetiRwl = new ReentrantReadWriteLock();
	private ReadWriteLock QlrRwl = new ReentrantReadWriteLock();
	private ReadWriteLock QlRwl = new ReentrantReadWriteLock();

	public void addKetiCount() {
		KetiRwl.writeLock().lock();
		try {
			ketiCount++;
		} finally {
			KetiRwl.writeLock().unlock();
		}
	}

	public int getKetiCount() {
		KetiRwl.readLock().lock();
		try {
			return this.ketiCount;
		} finally {
			KetiRwl.readLock().unlock();
		}
	}

	public void addQlrCount() {
		QlrRwl.writeLock().lock();
		try {
			qlrCount++;
		} finally {
			QlrRwl.writeLock().unlock();
		}
	}

	public int getQlrCount() {
		QlrRwl.readLock().lock();
		try {
			return this.qlrCount;
		} finally {
			QlrRwl.readLock().unlock();
		}
	}

	public void addQlCount() {
		QlRwl.writeLock().lock();
		try {
			qlCount++;
		} finally {
			QlRwl.writeLock().unlock();
		}
	}

	public int getQlCount() {
		QlRwl.readLock().lock();
		try {
			return this.qlCount;
		} finally {
			QlRwl.readLock().unlock();
		}
	}

}

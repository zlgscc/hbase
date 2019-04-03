package com.amc.hbase.test;

import java.io.IOException;
import java.util.HashSet;

import com.amc.hbase.util.HbaseConnection;

public class HbaseTest {

	
	public static void main(String[] args) {
		HbaseConnection conn = new HbaseConnection();
		try {
			conn.init();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		HashSet<String> columnDescSet = new HashSet<String>();
		String tableName = "machinelist";
		columnDescSet.add("machine");
		columnDescSet.add("slavemachine");
		conn.createTable(tableName, columnDescSet);
		conn.putDataToTable(tableName,"yk7332at-1");
		conn.putDataToTable(tableName,"yk7332at-2");
		
	}
}

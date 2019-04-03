package com.amc.hbase.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.mortbay.log.Log;

public class HbaseConnection {

	private static Configuration conf = null;
	
	private Connection conn = null;
	
	
	//初始化链接
	public void init() throws IOException {
		System.setProperty("hadoop.home.dir","D:\\hadoop-2.7.3");
		conf = HBaseConfiguration.create();
		conf.set("hbase.rootdir", "hdfs://master:8020/hbase");
		conf.set("hbase.zookeeper.quorum", "master:2181,slave1:2181,slave2:2181"); // hbase 服务地址
		conf.set("hbase.zookeeper.property.clientPort", "2181"); // 端口号
		this.conn = ConnectionFactory.createConnection(conf);
	}
	
	/**
	 * 创建表
	 * 输入表明和列族
	 * @param name,columnDesc
	 */
	public void createTable(String name,HashSet<String> columnDescSet) {
		
		try {
			// 创建表管理类
			Admin admin = conn.getAdmin();
			if(admin.tableExists(TableName.valueOf(name))) {
				Log.info("表已存在，请勿重复建表");
				return ;
				
			}
			// 创建表描述类
			TableName tableName = TableName.valueOf(name); // 表名称
			HTableDescriptor desc = new HTableDescriptor(tableName);
			// 创建列族的描述类
			for(String columnDesc:columnDescSet) {
				HColumnDescriptor family = new HColumnDescriptor(columnDesc); // 列族
				// 将列族添加到表中
				desc.addFamily(family);
			}
			// 创建表
			admin.createTable(desc); // 创建表
			System.out.println("创建表成功！");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/**
	 * 通过tablename扫描所有数据
	 * @param tableName
	 * @return
	 */
	public List<String> scanDataByTableName(String tableName){
		
		try {
			HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return null;
		
		
	}
	
	/**
	 *  添加数据到表中
	 * @param name
	 * @param rowKey
	 */
	public void putDataToTable(String name,String rowKey) {

		try {
			TableName tableName = TableName.valueOf(name);
			HTable table = (HTable) conn.getTable(tableName);
			List<Put> batput = new ArrayList<Put>();
			//实例化put对象，传入行键
            Put put = new Put(Bytes.toBytes(rowKey));
            //调用addcolum方法，向列簇中添加字段
            put.addColumn(Bytes.toBytes("machine"), Bytes.toBytes("name"),Bytes.toBytes("yk7332at"));
            put.addColumn(Bytes.toBytes("machine"), Bytes.toBytes("location"),Bytes.toBytes("陕西"));
            put.addColumn(Bytes.toBytes("machine"), Bytes.toBytes("runtime"),Bytes.toBytes("3year"));
            //将测试数据添加到list中
            batput.add(put);
            table.put(batput);
            Log.info("数据插入完成！");
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		/*BasicConfigurator.configure();*/ //自动快速地使用缺省Log4j环境
		System.setProperty("hadoop.home.dir","D:\\hadoop-2.7.3");
		try {
			Configuration conf = HBaseConfiguration.create();
			conf.set("hbase.rootdir", "hdfs://master:8020/hbase");
			conf.set("hbase.zookeeper.quorum", "master:2181,slave1:2181,slave2:2181"); // hbase 服务地址
			conf.set("hbase.zookeeper.property.clientPort", "2181"); // 端口号
			Connection conn = ConnectionFactory.createConnection(conf);
			Admin admin = conn.getAdmin();
			HTable table = (HTable) conn.getTable(TableName.valueOf("email_user"));
			Scan scan = new Scan();
			System.out.println("查询到的内容如下：");
			// 打印结果集
			ResultScanner scanner = table.getScanner(scan);
			for (Result result : scanner) {
				if (result.getValue(Bytes.toBytes("info"), Bytes.toBytes("f_pid")) == null) {
					for (KeyValue kv : result.raw()) {
						System.out.print(new String(kv.getRow()) + " ");
						System.out.print(new String(kv.getFamily()) + ":");
						System.out.print(new String(kv.getQualifier()) + " = ");
						System.out.print(new String(kv.getValue()));
						System.out.print(" timestamp = " + kv.getTimestamp() + "\n");
					}
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

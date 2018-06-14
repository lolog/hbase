package adj.felix.hbase.ch03.crud.read.table;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 核查数据是否存在
 * @author adolf felix
 */
public class CheckData {
	public static void main(String[] args) throws IOException {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "master,slave1,slave2");
		config.set("hbase.zookeeper.property.clientPort", "2181");

		// 创建HBase链接
		Connection connection = ConnectionFactory.createConnection(config);

		// TableName对象
		TableName tableName = TableName.valueOf("hb", "test");
		// 获取hb命名空间下的test表
		Table hbTestTable = connection.getTable(tableName);
		
		Get get = new Get(Bytes.toBytes("row01"));
		get.addFamily(Bytes.toBytes("facc"));
		
		System.out.println("exists = " + hbTestTable.exists(get));
		
		// 关闭流, 以及释放链接此到的资源
		connection.close();
	}
}

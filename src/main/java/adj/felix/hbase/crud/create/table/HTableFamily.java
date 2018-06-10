package adj.felix.hbase.crud.create.table;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 * HTable Family操作
 * @author adolf felix
 */
public class HTableFamily {
	public static void main(String[] args) throws IOException {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "master,slave1,slave2");
		config.set("hbase.zookeeper.property.clientPort", "2181");

		// 创建HBase链接
		Connection connection = ConnectionFactory.createConnection(config);
		// Admin可用于创建，删除，列出，启用和禁用表格，添加和删除表格列族和其他管理操作。
		Admin admin = connection.getAdmin();

		// hb表空间下的test表
		TableName htable = TableName.valueOf("hb", "test");
		
		// 创建HTable的详情信息, 即Describe
		HColumnDescriptor family = new HColumnDescriptor("facc");
		// 添加列族
		admin.addColumn(htable, family);
		
		// 删除Column Family
		byte[] columnName = "facc".getBytes();
		// 删除列族
		admin.deleteColumn(htable, columnName);
		
		/** 直接修改其所有family **/
		HTableDescriptor htd = new HTableDescriptor(htable);
		htd.addFamily(new HColumnDescriptor("facc"));
		htd.addFamily(new HColumnDescriptor("fact"));
		admin.modifyTable(htable, htd);
		
		// 关闭流, 以及释放链接此到的资源
		admin.close();
		connection.close();
	}
}

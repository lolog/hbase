package adj.felix.hbase.ch03.crud.delete.table;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class DropNamespace {
	public static void main(String[] args) throws IOException {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "master,slave1,slave2");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		
		// 创建HBase链接
		Connection connection = ConnectionFactory.createConnection(config);
		// Admin可用于创建，删除，列出，启用和禁用表格，添加和删除表格列族和其他管理操作。
		Admin admin = connection.getAdmin();
		
		// 删除namespace
		admin.deleteNamespace("hb");
		
		// 关闭流, 以及释放链接此到的资源
		admin.close();
		connection.close();
	}
}

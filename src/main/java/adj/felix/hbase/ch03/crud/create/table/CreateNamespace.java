package adj.felix.hbase.ch03.crud.create.table;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class CreateNamespace {
	public static void main(String[] args) throws IOException {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "master,slave1,slave2");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		
		// 创建HBase链接
		Connection connection = ConnectionFactory.createConnection(config);
		// Admin可用于创建，删除，列出，启用和禁用表格，添加和删除表格列族和其他管理操作。
		Admin admin = connection.getAdmin();
		
		// 创建namespace
		// 调用NamespaceDescriptor类, 会自动创建2个默认的namespace, hbase和default
		NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create("hb").build();
		// 创建namespace
		admin.createNamespace(namespaceDescriptor);
		
		// 关闭流, 以及释放链接此到的资源
		admin.close();
		connection.close();
	}
}

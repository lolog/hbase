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
 * <pre>
 * ~~~~ <b>创建表</b> ~~~~
 * 01. 步骤
 *     (1) 配置
 *     (2) 链接
 *     (3) 获取Admin
 *     (4) 判断表名是否存在, 不存在创建
 * 02. HTableName的valueOf方法
 *     valueOf(String namespaceAsString, String qualifierAsString)
 *     valueOf(byte[] fullName)
 *     valueOf(String name)
 * </pre>
 * @author adolf felix
 */
public class CreateHTable {
	public static void main(String[] args) throws IOException {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "master,slave1,slave2");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		
		// 创建HBase链接
		Connection connection = ConnectionFactory.createConnection(config);
		// Admin可用于创建，删除，列出，启用和禁用表格，添加和删除表格列族和其他管理操作。
		Admin admin = connection.getAdmin();
		
		// TableName对象
		// 其中, hb   - 表空间
		//      test - 表名
		TableName htable = TableName.valueOf("hb", "test");
		// 如果表不存在
		if (admin.tableExists(htable) == false) {
			// 创建HTable的详情信息, 即Describe
			HTableDescriptor tableDescriptor = new HTableDescriptor(htable);
			// 添加Family
			tableDescriptor.addFamily(new HColumnDescriptor("family"));
			
			// 创建表
			admin.createTable(tableDescriptor);
		}
		
		// 关闭流, 以及释放链接此到的资源
		admin.close();
		connection.close();
	}
}

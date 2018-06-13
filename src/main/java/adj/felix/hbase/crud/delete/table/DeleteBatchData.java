package adj.felix.hbase.crud.delete.table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * ~~~~ <b>批量删除数据</b> ~~~~
 * @author adolf felix
 */
public class DeleteBatchData {
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
		
		List<Delete> deletes = new ArrayList<Delete>();
		Delete delete02 = new Delete(Bytes.toBytes("row02"));
		delete02.addFamily(Bytes.toBytes("facc"));
		// 不存在的列删除抛异常, 不影响前面数据的删除
		Delete delete03 = new Delete(Bytes.toBytes("row04"));
		delete03.addFamily(Bytes.toBytes("no"));
		
		deletes.add(delete02);
		deletes.add(delete03);
		
		// 删除数据
		hbTestTable.delete(deletes);
		
		// 关闭流, 以及释放链接此到的资源
		connection.close();
	}
}

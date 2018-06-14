package adj.felix.hbase.ch03.crud.create.table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * <pre>
 * ~~~~ <b>Batch Puts</b> ~~~~
 * 01. 批量提交操作
 * 02. 方法
 * </pre>
 * @author adolf felix
 */
public class AddBatchPutsData {
	public static void main(String[] args) throws IOException, InterruptedException {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "master,slave1,slave2");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		
		// 创建HBase链接
		Connection connection = ConnectionFactory.createConnection(config);
		
		// 获取表
		TableName tableName = TableName.valueOf("hb", "test");
		Table hbTestTable = connection.getTable(tableName);
		
		// 批量数据
		List<Put> puts = new ArrayList<Put>();
		
		// 列族
		byte[] faccFamily = Bytes.toBytes("facc");
		byte[] noFamily = Bytes.toBytes("no");
		// 列
		byte[] quaccQuaifier = Bytes.toBytes("quacc");
		
		// 数据
		Put put04 = new Put(Bytes.toBytes("row04"));
		put04.addColumn(faccFamily, quaccQuaifier, Bytes.toBytes("value0401"));
		// 批量提交的时候, 不存在的Family会抛异常, 本条记录不会影响其他数据的插入
		Put put05 = new Put(Bytes.toBytes("row05"));
		put05.addColumn(noFamily, quaccQuaifier, Bytes.toBytes("value0501"));
		// 没有列, 抛出异常
		Put nullPut = new Put(Bytes.toBytes("row06"));
		
		// 加入批量
		puts.add(put04);
		puts.add(put05);
		puts.add(nullPut);
		
		// 发送一个RPC到服务器
		hbTestTable.put(puts);
		
		// 关闭流, 以及释放链接此到的资源
		connection.close();
	}
}

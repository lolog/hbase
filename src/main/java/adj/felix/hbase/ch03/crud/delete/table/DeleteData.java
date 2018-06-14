package adj.felix.hbase.ch03.crud.delete.table;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * <pre>
 * ~~~~ <b>删除数据</b> ~~~~
  * 01. Delete构造函数
 *     Delete(byte [] row)
 *     Delete(byte [] row, long timestamp)
 *     Delete(byte [] row, int rowOffset, int rowLength)
 *     Delete(byte [] row, int rowOffset, int rowLength, long ts)
 *     注意：row - 键, timestamp - 时间戳, rowOffset - 行键开始位置, rowLength - 行键长度, ts - 时间戳
 * 02. Delete方法
 *     addFamily(byte[] family)
 *     addFamily(byte[] family, long timestamp)
 *     addColumns(byte[] family, byte[] qualifier)           - 删除特定列的所有版本数据
 *     addColumns(byte[] family, byte[] qualifier, long ts)  - 删除特定列的所有版本数据, 但是其时间戳必须大于等于ts
 *     addColumn(byte[] family, byte[] qualifier)            - 删除最新时间戳的列数据
 *     addColumn(byte[] family, byte[] qualifier, long ts)
 *     setTimestamp(long timestamp)
 * 03. Delete的其他方法
 *     getRow()           - 获取需要删除的键值
 *     isEmpty()          - 判断需要删除的数据条件是否为空
 *     getFamilyMap()     - 获取需要删除数据的family结构
 * </pre> 
 * @author adolf felix
 */
public class DeleteData {
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
		
		Delete delete = new Delete(Bytes.toBytes("row01"));
		delete.addFamily(Bytes.toBytes("facc"));
		System.out.println("isEmpty = " + delete.isEmpty());
		
		// 删除数据
		hbTestTable.delete(delete);
		
		/** 原子操作删除数据 **/
		Delete compareAndDelete = new Delete(Bytes.toBytes("row05"));
		compareAndDelete.addColumns(Bytes.toBytes("facc"),  Bytes.toBytes("no"));
		// 检查键值是否一样, 一样则删除
		boolean success = hbTestTable.checkAndDelete(Bytes.toBytes("row05"), Bytes.toBytes("facc"), Bytes.toBytes("no"), Bytes.toBytes("value06"), compareAndDelete);
		System.out.println("Compare And Delete = " + success);
		
		
		// 关闭流, 以及释放链接此到的资源
		connection.close();
	}
}

package adj.felix.hbase.ch03.crud.create.table;

import java.io.IOException;

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
 * ~~~~ <b>Add Put Data</b> ~~~~
 * 01. Bytes类提供的方法
 *     static byte[] toBytes(ByteBuffer b);
 *     static byte[] toBytes(String s);
 *     static byte[] toBytes(boolean s);
 *     static byte[] toBytes(long s);
 *     static byte[] toBytes(float s);
 *     ...
 * 02. Put的构造函数
 *     Put(byte[] row)
 *     Put(byte[] row, long ts)
 *     Put(byte [] rowArray, int rowOffset, int rowLength)
 *     ...
 *     注释：row - 行键; ts - 时间戳; rowOffset - 行键起始位置; rowLength - 行键长度。
 * 03. Put的add方法
 *     add(byte[] family, byte[] qulifier, byte[] value)
 *     add(byte[] family, byte[] qulifier, long ts, byte[] value)
 *     add(KeyValue kv)
 *     ...
 * 04. Put的has方法
 *     has(byte[] family, byte[] qualifier)
 *     has(byte[] family, byte[] qualifier, long ts)
 *     has(byte[] family, byte[] qualifier, byte[] value)
 *     has(byte[] family, byte[] qualifier, long ts, byte[] value)
 *     ...
 * 05. Put的其他方法
 *     getRow()       - Put实例指定的行键
 *     getTimeStamp() - Put实例的时间戳
 *     isEmpty()      - 是否由任何KeyValue实例
 *     numFamilies()  - 返回FamilyMap的大小, 即所有的KeyValue实例中列族的个数
 *     size()         - 返回本次Put会添加9的KeyValue实例的数量
 * 06. 附加
 *     (01) 默认情况下, HBase只会保留3个版本的数据。scan和get只会返回最新版本的数据。
 * </pre>
 * @author adolf felix
 */
public class AddPutData {
	public static void main(String[] args) throws IOException {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "master,slave1,slave2");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		
		// 创建HBase链接
		Connection connection = ConnectionFactory.createConnection(config);
		// 获取hb表空间下的表名
		TableName tableName = TableName.valueOf("hb", "test");
		Table testTable = connection.getTable(tableName);
		
		// 列族
		byte[] faccFamily = Bytes.toBytes("facc");
		byte[] factFamily = Bytes.toBytes("fact");
		// 列
		byte[] quaccQuaifier = Bytes.toBytes("quacc");
		
		//  `row` - `family` - `qualifier`
		//  row01 -   facc   -    quacc
		//            fact   -    quacc
		Put put = new Put(Bytes.toBytes("row01"));
		put.addColumn(faccFamily, quaccQuaifier, Bytes.toBytes("hb--row01-facc-quacc-value01"));
		put.addColumn(factFamily, quaccQuaifier, Bytes.toBytes("hb--row01-fact-quacc-value01"));
		
		// 添加数据
		testTable.put(put);
		
		// 刷写表所有客户端的写操作,
		testTable.close();
		
		// 关闭流, 以及释放链接此到的资源
		connection.close();
	}
}

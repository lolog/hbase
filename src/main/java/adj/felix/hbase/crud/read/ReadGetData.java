package adj.felix.hbase.crud.read;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * <pre>
 * ~~~~ <b>Get获取单条数据</b> ~~~~
 * 01. Get构造函数
 *     Get(byte [] row)
 * 02. Get的方法
 *     addFamily(byte[] family)                    - 限定get请求只能获取一个指定的列族， 如果需要获取多个列族需要多次调用该方法
 *     addColumn(byte[] family, byte[] qualifier)  - 列族和列
 *     setTimeRange(long minStamp, long maxStamp)  - 仅在指定的时间戳范围内获取列的版本，[minStamp, maxStamp)
 *     setTimeStamp(long timestamp)                - 仅在指定的时间戳获取列的版本
 *     setMaxVersions()                            - 默认情况下, 版本数为1。版本数设置为Integer.MAX_VALUE
 *     setMaxVersions(int maxVersions)             - 设置版本数为maxVersions
 * 03. Get其他方法
 *     getRow()                     - 返回创建Get实例时指定的行键
 *     numFamilies()                - 返回指定的列族数
 *     getTimeRange()               - 返回时间戳范围
 *     hasFamilies()                - 返回是否指定列列族
 *     familySet() / getFamilyMap() - 返回family的集合
 *     setFilter(Filter filter)     - Get实例的过滤器
 * 04. Bytes类方法
 *     toString(byte[] b)           - byte[]转换为String
 *     toBoolean(byte[] b)
 *     toLong(byte[] b)
 *     toFloat(byte[] b)
 *     toInt(byte[] b)
 *     ... 
 * 05. Result的方法
 *     byte[] getRow()                           - 行键
 *     Cell[] rawCells()                         - 返回Cell数组
 *     int size()                                - Cell大小
 *     isEmpty()                                 - 数据是否为空
 *     getColumnLatestCell(family, qualifier)    - 返回指定的列的Cell
 * </pre>
 * @author adolf felix
 */
public class ReadGetData {
	public static void main(String[] args) throws IOException {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "master,slave1,slave2");
		config.set("hbase.zookeeper.property.clientPort", "2181");

		// 创建HBase链接
		Connection connection = ConnectionFactory.createConnection(config);
		
		// TableName对象
		TableName tableName = TableName.valueOf("hb", "test");
		// 获取hb命名空间下的test表
		Table hbTestTable   = connection.getTable(tableName);
		
		Get get = new Get(Bytes.toBytes("row01"));
		get.addFamily(Bytes.toBytes("facc"));
		
		// 返回查询的结果
		Result result = hbTestTable.get(get);
		
		// 读取数据
		List<Cell> cells = result.listCells();
		for(Cell cell: cells) {
			String family = Bytes.toString(CellUtil.cloneFamily(cell));
			String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
			String value = Bytes.toString(CellUtil.cloneValue(cell));
			System.out.println("family = " + family + ", qualifier=" + qualifier + ", value=" + value);
		}
		
		// 获取指定的数据
		byte[] vals = result.getValue(Bytes.toBytes("facc"), Bytes.toBytes("quacc"));
		System.out.println(Bytes.toString(vals));
		
		// 关闭流, 以及释放链接此到的资源
		connection.close();
	}
}

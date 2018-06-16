package adj.felix.hbase.ch03.scan;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * <pre>
 * ~~~~ <b>Scan</b> ~~~~
 * 01. Scan构造器
 *     Scan()
 *     Scan(Get get)
 *     Scan(Scan scan)
 *     Scan(byte [] startRow)
 *     Scan(byte [] startRow, Filter filter)
 *     Scan(byte [] startRow, byte [] stopRow)  [startRow, stopRow)
 * 02. Scan方法
 *     addFamily(byte [] family)                            - 添加列族
 *     addColumn(byte [] family, byte [] qualifier)         - 添加列
 *     setTimeRange(long minStamp, long maxStamp)           - 设置时间戳范围
 *     setTimeStamp(long timestamp)                         - 设置时间戳
 *     setMaxVersions()                                     - 所有版本
 *     setMaxVersions(int maxVersions)                      - 最大的版本
 *     
 *     setStartRow(byte [] startRow)                        - 设置起始的行键
 *     setStopRow(byte [] stopRow)                          - 设置结束的行键
 *     setFilter(Filter filter)                             - 设置过滤器
 *     hasFilter()                                          - 判断是否由过滤器
 *     
 *     setCacheBlocks(boolean cacheBlocks)                  - 是否启动HBase的region缓存数据, 默认为true, 应避免全表扫描
 *     hasFamilies()                                        - 检查是否有添加过列族和列
 *     setFamilyMap(Map<byte[], NavigableSet<byte[]>> fm)   - 设置FamilyMap
 * 03. Scan Table数据
 *     getScanner(Scan scan)
 *     getScanner(byte [] family)
 *     getScanner(byte [] family, byte [] qualifier)
 * 04. ResultScanner
 *     next()                                                - 获取Result
 *     next(int nbRows)                                      - 获取nbRows条Result
 *     close()                                               - 释放资源
 * </pre> 
 * @author adolf felix
 */
public class HTableScan {
	public static void main(String[] args) throws IOException {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "master,slave1,slave2");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		
		// 创建HBase链接
		Connection connection = ConnectionFactory.createConnection(config);
		
		// 获取表
		TableName tableName = TableName.valueOf("hb", "test");
		Table hbTestTable = connection.getTable(tableName);
		
		byte[] fact = Bytes.toBytes("fact");
		byte[] quacc = Bytes.toBytes("quacc");
		
		// 扫描的条件
		Scan scan = new Scan();
		scan.addColumn(fact, quacc);
		
		// 开始扫描
		ResultScanner result = hbTestTable.getScanner(scan);
		for(Result rs: result) {
			System.out.println(rs);
		}
		
		// 释放资源
		result.close();
		hbTestTable.close();
		connection.close();
	}
}
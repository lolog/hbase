package adj.felix.hbase.ch03.scan;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
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
 * ~~~~ <b>Batch Scan</b> ~~~~
 * 01. Scan方法
 *     setCaching(int caching)                  - 设置scan的数量，批量加载caching条行数据, 默认值HConstants#HBASE_CLIENT_SCANNER_CACHING, 或者修改hbase配置项hbase.client.scanner.caching
 *                                              - 批量获取行数, 指定范围内next()不会发送RPC请求。
 *                                              - 需要获取大量数据到客户端, next()会占用大量时间,客户端可能会抛出OutOfMemoryException异常。 
 *     getCaching()                             - 获取scan的数量
 *     
 *     setBatch(int batch)                      - 每次scan批量加载batch列数据
 * 02. 注意
 *     当传输和数据处理的时间, 超过配置的扫描器租约时间, 将会抛出ScannerTimeoutException异常
 * </pre> 
 * @author adolf felix
 */
public class HTableBatchScan {
	public static void main(String[] args) throws IOException, InterruptedException {
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
		byte[] quact = Bytes.toBytes("quact");
		
		// 扫描的条件
		Scan scan = new Scan();
		scan.addColumn(fact, quacc);
		scan.addColumn(fact, quact);
		// 每次批量获取10条数据
		scan.setCaching(10);
		// 每次批量加载1列数据, 那么每次只能取回一列的数据
		scan.setBatch(1);
		
		// 批量开始扫描
		ResultScanner batchResultScanner = hbTestTable.getScanner(scan);
		// 单次读取扫描器
		ResultScanner resultScanner = hbTestTable.getScanner(Bytes.toBytes("facc"), quacc);
		
		// 设置超过租约时间,那么如果next()再发送RPC请求将抛出ScannerTimeoutException异常
		// Thread.sleep(config.getLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, -1) + 10000);
		
		try {
			// 批量数据处理
			for(Result rs: batchResultScanner) {
				System.out.println(rs);
			}
			
			// 单词数据处理, 再次发送RPC将抛出ScannerTimeoutException异常. 测试未抛出异常
			while (true) {
				Result rs = resultScanner.next();
				if(rs == null) break;
				System.out.println(rs);
			}
		} finally {
			// 释放资源
			batchResultScanner.close();
			resultScanner.close();
			hbTestTable.close();
			connection.close();
		}
	}
}
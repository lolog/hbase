package adj.felix.hbase.ch04.filter;

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
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * <pre>
 * ~~~~ <b>行过滤器</b> ~~~~
 * 01. 比较运算符
 *     LESS             小于
 *     LESS_OR_EQUAL    小于等于
 *     EQUAL            等于
 *     NOT_EQUAL        不等于
 *     GREATER_OR_EQUAL 大于等于
 *     GREATER          大于
 *     NO_OP            排除一切的值
 * 02. 比较器
 *     BinaryComparator       使用Bytes.compareTo()比较当前值与阈值
 *     BinaryPrefixComparetor 从左端开始前缀匹配
 *     NullComparator         NULL匹配
 *     BitComparator          位比较
 *     RegexStringComparator  正贼表达式比较
 *     SubstringComparator    是否包含子字符串比较
 * </pre>
 * @author adolf felix
 */
public class RowFilterData {
	public static void main(String[] args) throws IOException {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "master,slave1,slave2");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		
		// 创建HBase链接
		Connection connection = ConnectionFactory.createConnection(config);
		
		// 获取表
		TableName tableName = TableName.valueOf("hb", "test");
		Table hbTestTable = connection.getTable(tableName);
		
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes("fact"), Bytes.toBytes("quacc"));
		
		Filter lessOrEqualfilter = new RowFilter(CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("row02")));
		scan.setFilter(lessOrEqualfilter);
		ResultScanner lessOrEqualScanner = hbTestTable.getScanner(scan);
		System.out.println("++++++++++++++++ LESS_OR_EQUAL - BinaryComparator +++++++++++++++");
		for (Result result: lessOrEqualScanner) {
			System.out.println(result);
		}
		lessOrEqualScanner.close();
		
		Filter equalFilter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(".*2"));
		scan.setFilter(equalFilter);
		ResultScanner equalScanner = hbTestTable.getScanner(scan);
		System.out.println("\n++++++++++++++++ EQUAL - RegexStringComparator +++++++++++++++");
		for (Result result: equalScanner) {
			System.out.println(result);
		}
		equalScanner.close();
		
		Filter equalSubFilter = new RowFilter(CompareOp.EQUAL, new SubstringComparator("02"));
		scan.setFilter(equalSubFilter);
		ResultScanner equalSubScanner = hbTestTable.getScanner(scan);
		System.out.println("\n++++++++++++++++ EQUAL - SubstringComparator +++++++++++++++");
		for (Result result: equalSubScanner) {
			System.out.println(result);
		}
		equalScanner.close();
		
		// 释放资源
		hbTestTable.close();
		connection.close();
	}
}

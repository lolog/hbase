package adj.felix.hbase.crud.read.table;

import java.io.IOException;
import java.util.ArrayList;
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
 * 批量读取数据
 * @author adolf felix
 *
 */
public class ReadGetBatchData {
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
		
		List<Get> gets = new ArrayList<Get>();
		Get get01 = new Get(Bytes.toBytes("row01"));
		get01.addFamily(Bytes.toBytes("facc"));
		Get get02 = new Get(Bytes.toBytes("row02"));
		get02.addFamily(Bytes.toBytes("fact"));
		
		gets.add(get01);
		gets.add(get02);
		
		// 注意：如果读取一个不存在的列族，那么代码将会抛出异常
		
		// 返回查询的结果
		Result[] results = hbTestTable.get(gets);
		
		// 读取数据
		for (Result result: results) {
			List<Cell> cells = result.listCells();
			for(Cell cell: cells) {
				String family = Bytes.toString(CellUtil.cloneFamily(cell));
				String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
				String value = Bytes.toString(CellUtil.cloneValue(cell));
				System.out.println("family = " + family + ", qualifier=" + qualifier + ", value=" + value);
			}
		}
		
		// 关闭流, 以及释放链接此到的资源
		connection.close();
	}
}

package adj.felix.hbase.crud.create.table;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * <pre>
 * ~~~~ <b> Add Put KeyValue Data</b> ~~~~
 * 01. Put的add方法
 *     add(KeyValue kv)
 * 02. KeyValue提供的Comparator比较器
 *     KeyComparator     - Key
 *     KVComparator      - KeyValue实例
 *     RowComparator     - KeyValue实例的行键, 即getRow()值
 *     MetaKeyComparator - 字节数组格式表示的.META.条目的行键
 *     MetaComparator    - 比较.META.目录表中的条目, MetaKeyComparator的封装
 *     RootKeyComparator - 字节数组格式表示的-ROOT-条目的行键
 *     RootKeyComparator - 字节数组格式表示的-ROOT-目录表中的条目
 * </pre>
 * @author adolf felix
 */
public class AddPutKeyValueData {
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
		Put put = new Put(Bytes.toBytes("row03"));
		// 注意：KeyValue的rowKey必须和Put的rouKey相同, 不然会抛异常, 导致该Put记录都不能插入
		Cell kv01 = new KeyValue(Bytes.toBytes("row02"), faccFamily, quaccQuaifier, Bytes.toBytes("hb--row02-facc-quacc-value0201"));
		Cell kv02 = new KeyValue(Bytes.toBytes("row02"), factFamily, quaccQuaifier, Bytes.toBytes("hb--row02-fact-quacc-value0202"));
		put.add(kv01);
		put.add(kv02);
		
		// 添加数据
		testTable.put(put);
		
		// 关闭流, 以及释放链接此到的资源
		connection.close();
	}
}

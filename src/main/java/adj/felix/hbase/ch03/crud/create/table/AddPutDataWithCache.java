package adj.felix.hbase.ch03.crud.create.table;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * <pre>
 * ~~~~ <b>Write Cache</b> ~~~~
 * 01. 每个Put对应一个RPC操作，一般情况下, 在LAN网络中, 一个Put的RPC操作, 耗时为1ms, 那么1m只能进行100次RPC操作。
 *     那么, 如果通过网络发送的数据量较大, 那么需要请求返回的次数相对减少, 因为时间大都花费在数据传输上。
 * 02. HBase准备列一个客户端缓冲区, 缓冲Put, 通过
 * </pre>
 * @author adolf felix
 */
public class AddPutDataWithCache {
	public static void main(String[] args) throws IOException, InterruptedException {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "master,slave1,slave2");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		
		// 创建HBase链接
		Connection connection = ConnectionFactory.createConnection(config);
		
		// 获取表
		TableName tableName = TableName.valueOf("hb", "test");
		Table hbTestTable = connection.getTable(tableName);
		
		// 获取HBase表的数据缓冲区
		BufferedMutator cacheMutator = connection.getBufferedMutator(tableName);
		
		// 列族
		byte[] faccFamily = Bytes.toBytes("facc");
		byte[] noFamily = Bytes.toBytes("no");
		// 列
		byte[] quaccQuaifier = Bytes.toBytes("quacc");
		
		// 数据
		Put put04 = new Put(Bytes.toBytes("row04"));
		put04.addColumn(faccFamily, quaccQuaifier, Bytes.toBytes("value04001"));
		// 批量提交的时候, 不存在的Family会抛异常, 本条记录不会影响其他数据的插入
		Put put05 = new Put(Bytes.toBytes("row05"));
		put05.addColumn(noFamily, quaccQuaifier, Bytes.toBytes("value005"));
		
		// 将数据加入到缓冲区，可以根据自己的需要控制缓冲区的大小，实现批量提交数据
		cacheMutator.mutate(put04);
		cacheMutator.mutate(put05);
		
		// 在未发送刷新缓存之前, 检测是否将数据插入到HBase
		Get get = new Get(Bytes.toBytes("row04"));
		Result resultBefore = hbTestTable.get(get);
		System.out.println("刷新缓冲区之前检测, 查询数据库的结果 = " + resultBefore);
		
		// 刷新缓冲区, 向HBase发送RPC，批量提交数据， 该过程是异步发送的
		cacheMutator.flush();
		// 通过close()方法也能提交RPC，但是下次必须重新获取相应表的BufferedMutator缓冲区对象
		// 或 cacheMutator.close();
		
		// 刷写表所有客户端的写操作,
		hbTestTable.close();
		
		// 等待提交带HBase成功
		Thread.sleep(1000);
		
		// 提交之后，再次查询
		Result resultAfter = hbTestTable.get(get);
		System.out.println("刷新缓冲区之后检测, 查询数据库的结果 = " + resultAfter);
		
		// 关闭流, 以及释放链接此到的资源
		connection.close();
	}
}

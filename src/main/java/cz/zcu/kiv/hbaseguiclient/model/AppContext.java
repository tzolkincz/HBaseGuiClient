package cz.zcu.kiv.hbaseguiclient.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

public class AppContext {

	ConcurrentHashMap<String, Pair<HConnection, HBaseAdmin>> clusterMap = new ConcurrentHashMap<>();
	Map<String, Map<String, List<String>>> clusterTables = new HashMap<>();

	public void createConnection(String zk, String clusterName) throws IOException {
		Configuration conf = new Configuration();
		conf.set(HConstants.ZOOKEEPER_QUORUM, zk);
//		conf.set(HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT, "600");
//		conf.set(HConstants.DEFAULT_HBASE_RPC_TIMEOUT, "600");

		//as of HBase 0.99 deprecated (but this should be for hbase 98)
		HConnection connection = HConnectionManager.createConnection(conf);
		HBaseAdmin admin = new HBaseAdmin(conf);

		//determine cluster name if not present
		if (clusterName == null) {
			if (zk.length() > 20) {
				clusterName = zk.substring(0, 18).concat("...");
			} else {
				clusterName = zk;
			}
		}

		clusterMap.put(clusterName, new Pair<>(connection, admin));
		refreshTables(clusterName);
	}

	public void refreshTables(String clusterName) throws IOException {
		//@TODO table namespace (HBase 1.0+)
		Map<String, List<String>> namespaceMap = new HashMap<>();
		List<String> defaultNamespaceTables = new ArrayList<>();

		HTableDescriptor[] tablesDescriptors = clusterMap.get(clusterName).getValue().listTables();
		for (HTableDescriptor desc : tablesDescriptors) {
			defaultNamespaceTables.add(desc.getNameAsString());
		}
		namespaceMap.put("default", defaultNamespaceTables);
		clusterTables.put(clusterName, namespaceMap);
	}

	public ConcurrentHashMap<String, Pair<HConnection, HBaseAdmin>> getClusterMap() {
		return clusterMap;
	}

	public Map<String, Map<String, List<String>>> getClusterTables() {
		return clusterTables;
	}

}

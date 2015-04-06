package cz.zcu.kiv.hbaseguiclient.model;

import com.google.common.base.Throwables;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import javafx.application.Platform;
import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

public class AppContext {

	ConcurrentHashMap<String, Pair<HConnection, HBaseAdmin>> clusterMap = new ConcurrentHashMap<>();
	Map<String, Map<String, List<String>>> clusterTables = new HashMap<>();

	public void createConnection(final String zk, final String clusterName, BiConsumer<String, String> callback) {
		if (zk.isEmpty()) {
			callback.accept("Empty zookeeper connection string", null);
			return;
		}

		Platform.runLater(() -> {
			Configuration conf = new Configuration();
			conf.set("hbase.zookeeper.quorum", zk);
			conf.set("zookeeper.session.timeout", "600");
			conf.set("hbase.client.operation.timeout", "600");
			conf.set("hbase.rpc.timeout", "600");
			conf.set("hbase.client.retries.number", "1");
			conf.set("zookeeper.recovery.retry", "1");

			try {
				//as of HBase 0.99 deprecated (but this should be for hbase 98)
				HConnection connection = HConnectionManager.createConnection(conf);
				HBaseAdmin admin = new HBaseAdmin(conf);

				//determine cluster name if not present
				String clusterAlias;
				if (clusterName == null) {
					if (zk.length() > 20) {
						clusterAlias = zk.substring(0, 18).concat("...");
					} else {
						clusterAlias = zk;
					}
				} else {
					clusterAlias = clusterName;
				}

				clusterMap.put(clusterAlias, new Pair<>(connection, admin));
				callback.accept(null, clusterAlias);
			} catch (IOException e) {
				e.printStackTrace();
				callback.accept(Throwables.getStackTraceAsString(e), null);
			}
		});
	}

	public void refreshTables(String clusterName, Consumer<Boolean> callback) {
		Platform.runLater(() -> {
			try {
				//@TODO table namespace (HBase 1.0+)
				Map<String, List<String>> namespaceMap = new HashMap<>();
				List<String> defaultNamespaceTables = new ArrayList<>();

				HTableDescriptor[] tablesDescriptors = clusterMap.get(clusterName).getValue().listTables();
				for (HTableDescriptor desc : tablesDescriptors) {
					defaultNamespaceTables.add(desc.getNameAsString());
				}
				namespaceMap.put("default", defaultNamespaceTables);
				clusterTables.put(clusterName, namespaceMap);
				callback.accept(true);
			} catch (IOException ex) {
				callback.accept(false);
			}
		});
	}

	public ConcurrentHashMap<String, Pair<HConnection, HBaseAdmin>> getClusterMap() {
		return clusterMap;
	}

	public Map<String, Map<String, List<String>>> getClusterTables() {
		return clusterTables;
	}

}

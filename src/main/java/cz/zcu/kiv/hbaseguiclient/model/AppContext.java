package cz.zcu.kiv.hbaseguiclient.model;

import com.google.common.base.Throwables;
import static cz.zcu.kiv.hbaseguiclient.MainApp.CLUSTER_CONFIG_NAME;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import javafx.application.Platform;
import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

public class AppContext {

	static ConcurrentHashMap<String, Pair<HConnection, HBaseAdmin>> clusterMap = new ConcurrentHashMap<>();
	static Map<String, Map<String, List<String>>> clusterTables = new HashMap<>();

	public void createConnection(final String zk, final String clusterName, BiConsumer<String, String> callback) {
		if (zk.isEmpty()) {
			callback.accept("Empty zookeeper connection string", null);
			return;
		}

		new Thread(() -> {
			Configuration conf = new Configuration();
			conf.set("hbase.zookeeper.quorum", zk);
			conf.set("zookeeper.session.timeout", "6000");
			conf.set("hbase.client.operation.timeout", "2000");
			conf.set("hbase.rpc.timeout", "2000");
			conf.set("hbase.client.retries.number", "1");
			conf.set("zookeeper.recovery.retry", "1");

			try {
				//as of HBase 0.99 deprecated (but this should be for hbase 98)
				HConnection connection = HConnectionManager.createConnection(conf);
				HBaseAdmin admin = new HBaseAdmin(conf);

				String clusterAlias = getUnifiedClusterAlias(clusterName, zk);
				clusterMap.put(clusterAlias, new Pair<>(connection, admin));
				callback.accept(null, clusterAlias);
			} catch (IOException e) {
				clusterMap.remove(getUnifiedClusterAlias(clusterName, zk));
				callback.accept(Throwables.getStackTraceAsString(e), null);
			}
		}).start();
	}

	private String getUnifiedClusterAlias(String clusterName, String zk) {
		if (clusterName == null || clusterName.isEmpty()) {
			if (zk.length() > 20) {
				return zk.substring(0, 18).concat("...");
			} else {
				return zk;
			}
		}
		return clusterName;
	}

	public void refreshTables(String clusterName, BiConsumer<Boolean, Exception> callback) {
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
				callback.accept(true, null);
			} catch (IOException ex) {
				callback.accept(false, ex);
			}
		});
	}

	public ConcurrentHashMap<String, Pair<HConnection, HBaseAdmin>> getClusterMap() {
		return clusterMap;
	}

	public Map<String, Map<String, List<String>>> getClusterTables() {
		return clusterTables;
	}

	public static void addClusterToConfigFileIfNotPresent(String newAlias, String newZk) {
		try {
			java.nio.file.Path configPath = Paths.get(System.getProperty("user.dir"), CLUSTER_CONFIG_NAME);
			try {
				Files.createFile(configPath);
			} catch (FileAlreadyExistsException e) {
				//do nothing
			}

			AtomicBoolean add = new AtomicBoolean(true);
			java.nio.file.Files.lines(configPath).forEach(line -> {
				String[] aliasAndZk = line.split("\t");
				String alias = aliasAndZk[0];
				String zk = aliasAndZk[1];

				if (alias.equals(newAlias) && zk.equals(newZk)) {
					add.set(false);
				}
			});
			if (add.get() == true) {
				ArrayList<String> newLine = new ArrayList<>();
				newLine.add(newAlias + "\t" + newZk);
				java.nio.file.Files.write(configPath, newLine, StandardOpenOption.APPEND);
			}
		} catch (IOException ex) {
			System.out.println("Something went wrong:");
			ex.printStackTrace();
		}
	}

	public void createTable(String cluster, HTableDescriptor descriptor, byte[][] presplits, Consumer<String> callback) {
		new Thread(() -> {
			HBaseAdmin admin = getClusterMap().get(cluster).getValue();
			try {
				admin.createTable(descriptor, presplits);
			} catch (IOException ex) {
				callback.accept(ex.toString());
			}
			callback.accept(null);
		}).start();
	}
}

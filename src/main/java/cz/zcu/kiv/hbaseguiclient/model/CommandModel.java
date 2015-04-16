package cz.zcu.kiv.hbaseguiclient.model;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class CommandModel {

	//@TODO gramatics
	static Pattern scanPattern = Pattern.compile("\\s*scan\\s+(?<table>[\\w:.]+)(\\s+start\\s+(?<start>[\\w\\s]+))(\\s+skip\\s+(?<skip>\\d+))(\\s+limit\\s+(?<limit>\\d+))?\\s*",
			Pattern.CASE_INSENSITIVE);

	public static String submitQuery(String text) throws IOException {

		try {
			Thread.sleep(2000L);
		} catch (InterruptedException ex) {
			Logger.getLogger(CommandModel.class.getName()).log(Level.SEVERE, null, ex);
		}

		Matcher m = scanPattern.matcher(text);

		if (m.matches()) {
			String fullTableIdentifier = m.group("table");
			String skip = m.group("skip");
			String limit = m.group("limit");
			String start = m.group("start");

//			AppContext.clusterTables.
			String[] tableLocators = fullTableIdentifier.split(":");

			String cluster;
			String table;
			if (tableLocators.length > 1) {
				cluster = tableLocators[0];
				table = tableLocators[1];
			} else {
				cluster = getFirstClusterAlias();
				table = tableLocators[0];
			}

			if (cluster == null) {
				return "Cluster not specified";
			}

			if (table == null) {
				return "Table not specified";
			}

			if (!AppContext.clusterTables.get(cluster).get("default").contains(table)) {
				return "Table not found at cluster " + cluster;
			}

			createScan(cluster, table, start, skip, limit);

			//@TODO table namespace (HBase v1.0+)
			return null;
		} else {
			return "No table found. Exec command like \"scan Table limit 10\"";
		}
	}

	public static String getFirstClusterAlias() {
		for (String cluster : AppContext.clusterTables.keySet()) {
			return cluster;
		}
		return null;
	}

	private static void createScan(String cluster, String table, String start, String skip, String limit) throws IOException {
		Scan scan = new Scan();
		if (start != null) {
			scan.setStartRow(Bytes.toBytes(start));
		}

		if (limit == null) {
			limit = "40";
		}
		scan.setMaxResultsPerColumnFamily(Integer.parseInt(limit));
		if (skip != null) {
			scan.setRowOffsetPerColumnFamily(Integer.parseInt(skip));
		}

		HConnection connection = AppContext.clusterMap.get(cluster).getKey();
		HTableInterface tableInterface = connection.getTable(table);

		ResultScanner resultScanner = tableInterface.getScanner(scan);
		resultScanner.iterator().forEachRemaining( res -> {
			res.getNoVersionMap().forEach( (family, kv) -> {
				
			});
		});
	}

}

package cz.zcu.kiv.hbaseguiclient.model;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class CommandModel {

	static Map<String, Map<String, String>> queryResult;
	static Set<String> columns;

	public static Map<String, Map<String, String>> getQueryResult() {
		return queryResult;
	}

	public static Set<String> getColumns() {
		return columns;
	}

	//@TODO gramatics
	static Pattern scanPattern = Pattern.compile("\\s*scan\\s+(?<table>[\\w:.]+)(\\s+start\\s+(?<start>[\\w\\s]+))?(\\s+skip\\s+(?<skip>\\d+))?(\\s+limit\\s+(?<limit>\\d+))?\\s*",
			Pattern.CASE_INSENSITIVE);

	public static String submitQuery(String text) throws IOException {

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

			execScan(cluster, table, start, skip, limit);

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

	private static void execScan(String cluster, String table, String start, String skip, String limit) throws IOException {
		Scan scan = new Scan();
//		scan.setCaching(10);
//		scan.setBatch(100);
		scan.setMaxResultSize(1 * 1024 * 1024); //1MB

		if (start != null) {
			scan.setStartRow(Bytes.toBytes(start));
		}

		if (limit == null) {
			limit = "40";
		}
		scan.setFilter(new PageFilter(Integer.parseInt(limit)));

		if (skip != null) {
			scan.setRowOffsetPerColumnFamily(Integer.parseInt(skip));
		}

		HConnection connection = AppContext.clusterMap.get(cluster).getKey();
		HTableInterface tableInterface = connection.getTable(table);

		//rowKey cf:cq	value
		queryResult = new TreeMap<>();
		columns = new TreeSet<>(); //store all columns from result (some could be null in first line)

		System.out.println("Scan settings: " + scan.toString());
		ResultScanner resultScanner = tableInterface.getScanner(scan);
		System.out.println("scan done");

		resultScanner.iterator().forEachRemaining(res -> {

			Map<String, String> columnValues = new HashMap<>();

			res.getNoVersionMap().forEach((familyBytes, qualifierValue) -> {
				String family = Bytes.toString(familyBytes);
				qualifierValue.forEach((qualifierBytes, valueBytes) -> {

					String qualifier = Bytes.toString(qualifierBytes);
					String value = Bytes.toString(valueBytes);

					columns.add(family + ":" + qualifier);
					columnValues.put(family + ":" + qualifier, value);
				});
			});

			String rk = Bytes.toString(res.getRow());
			queryResult.put(rk, columnValues);
		});
	}

}

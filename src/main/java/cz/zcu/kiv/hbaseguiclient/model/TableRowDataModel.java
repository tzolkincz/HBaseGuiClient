package cz.zcu.kiv.hbaseguiclient.model;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class TableRowDataModel {

	String rowKey;
	Map<String, String> kvs;

	public TableRowDataModel(String rowKey, Map<String, String> row) {
		this.rowKey = rowKey;
		this.kvs = row;
	}

	public String getRowKey() {
		return rowKey;
	}

	public String getValue(String columnName) {
		return kvs.get(columnName);
	}

	public void setValue(String columnName, String newValue, Consumer<String> done) {
		Put put = new Put(Bytes.toBytes(rowKey));

		String[] cfAndCq = columnName.split(":");

		put.add(Bytes.toBytes(cfAndCq[0]), Bytes.toBytes(cfAndCq[1]), Bytes.toBytes(newValue));

		new Thread(() -> {
			try {
				CommandModel.currentTableInterface.put(put);
				done.accept(null);
			} catch (IOException ex) {
				done.accept(ex.getMessage());
				Logger.getLogger(TableRowDataModel.class.getName()).log(Level.SEVERE, null, ex);
			}
		}).start();
	}

}

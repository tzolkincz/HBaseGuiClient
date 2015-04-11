package cz.zcu.kiv.hbaseguiclient.model;

import java.util.ArrayList;
import java.util.List;

public class TableRowDataModel {

	List<String> values = new ArrayList<>();

	public TableRowDataModel() {
		for (int j = 0; j < 10; j++) {
			values.add(String.valueOf(Math.random() * 100));
		}
	}

	public List<String> getValues() {
		return values;
	}

}

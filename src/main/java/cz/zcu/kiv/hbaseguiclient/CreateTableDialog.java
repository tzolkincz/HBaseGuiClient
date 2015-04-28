package cz.zcu.kiv.hbaseguiclient;

import cz.zcu.kiv.hbaseguiclient.model.AppContext;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import javafx.application.Platform;
import javafx.beans.property.ReadOnlyBooleanProperty;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.scene.Node;
import javafx.scene.control.ButtonBar;
import javafx.scene.control.ButtonType;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ChoiceBox;
import javafx.scene.control.Dialog;
import javafx.scene.control.Label;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.TextField;
import javafx.scene.layout.Border;
import javafx.scene.layout.BorderStroke;
import javafx.scene.layout.BorderStrokeStyle;
import javafx.scene.layout.BorderWidths;
import javafx.scene.layout.CornerRadii;
import javafx.scene.layout.GridPane;
import javafx.scene.paint.Color;
import javax.xml.bind.DatatypeConverter;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Triple;

public class CreateTableDialog {

	final static AtomicInteger countOfCFs = new AtomicInteger(0);
	static List<TextField> cfNamesList = new ArrayList<>();
	static int cfPropertiesHeight = 4;
	static int cfPropertiesOffset = 3;
	static List<GridPane> cfPanes;
	private final AppContext appContext;
	private final MainApp mainGui;
	private ObservableList<Node> cfDynamicNodes;

	CreateTableDialog(MainApp mainGui, AppContext appContext) {
		this.appContext = appContext;
		this.mainGui = mainGui;
		cfPanes = new ArrayList<>();
	}

	private GridPane cfPropertiesFactory(GridPane grid) {
		GridPane cfGrid = new GridPane();
		cfGrid.setHgap(5);
		cfGrid.setVgap(10);
		cfGrid.setPadding(new Insets(20, 150, 10, 10));
		cfGrid.setBorder(new Border(new BorderStroke(
				Color.GRAY, BorderStrokeStyle.SOLID, new CornerRadii(15), BorderWidths.DEFAULT)));

		TextField cf = new TextField();
		cf.setPromptText("cf");
		cfGrid.add(new Label("Column family:"), 0, 0);
		cfGrid.add(cf, 1, 0);
		cfNamesList.add(cf);

		ChoiceBox<String> dataBlockEncodingChoiceBox = new ChoiceBox();
		dataBlockEncodingChoiceBox.getItems().addAll(
				"NONE",
				"DIFF",
				"FAST_DIFF",
				"PREFIX",
				"PREFIX_TREE");
		dataBlockEncodingChoiceBox.setValue("FAST_DIFF");

		cf.focusedProperty().addListener(focus -> {
			if (((ReadOnlyBooleanProperty) focus).get() == false) {
				//determine if add new cf
				boolean emptyExists = false;
				for (TextField cfField : cfNamesList) {
					if (cfField.getText().isEmpty()) {
						emptyExists = true;
					}
				}
				if (emptyExists == false) {
					grid.add(cfPropertiesFactory(grid), 0,
							countOfCFs.incrementAndGet() * cfPropertiesHeight + cfPropertiesOffset, 4, 3);
				}
			}
		});

		cfGrid.add(new Label("Data block encoding:"), 0, 1);
		cfGrid.add(dataBlockEncodingChoiceBox, 1, 1);

		NumFieldFX versions = new NumFieldFX();
		versions.setText("3");
		cfGrid.add(new Label("Versions:"), 2, 0);
		cfGrid.add(versions, 3, 0);

		NumFieldFX minVersions = new NumFieldFX();
		minVersions.setPromptText("1");
		cfGrid.add(new Label("Min versions:"), 2, 1);
		cfGrid.add(minVersions, 3, 1);

		NumFieldFX ttl = new NumFieldFX();
		ttl.setText(Integer.toString(Integer.MAX_VALUE));
		cfGrid.add(new Label("TTL (sec):"), 0, 3);
		cfGrid.add(ttl, 1, 3);

		ChoiceBox<String> bloomFilterChoiceBox = new ChoiceBox();
		bloomFilterChoiceBox.getItems().addAll(
				"NONE",
				"ROW",
				"ROWCOL");
		bloomFilterChoiceBox.setValue("NONE");
		cfGrid.add(new Label("Bloom filter"), 0, 4);
		cfGrid.add(bloomFilterChoiceBox, 1, 4);

		ChoiceBox<String> compressionChoiceBox = new ChoiceBox();
		compressionChoiceBox.getItems().addAll(
				"NONE",
				"SNAPPY",
				"LZO",
				"LZ4",
				"GZ");
		compressionChoiceBox.setValue("SNAPPY");
		cfGrid.add(new Label("Compression"), 2, 3);
		cfGrid.add(compressionChoiceBox, 3, 3);

		CheckBox inMemoryCheckBox = new CheckBox();
		cfGrid.add(new Label("In memory"), 2, 4);
		cfGrid.add(inMemoryCheckBox, 3, 4);

		cfPanes.add(cfGrid);
		return cfGrid;
	}

	public void showCreatePopUp() {
		Dialog<Triple<String, HTableDescriptor, byte[][]>> dialog = new Dialog<>();
		dialog.setTitle("Create new table");
		GridPane grid = new GridPane();
		grid.setHgap(10);
		grid.setVgap(10);
		grid.setPadding(new Insets(20, 150, 10, 10));

		TextField name = new TextField();
		name.setPromptText("my_table");
		grid.add(new Label("Table name:*"), 0, 0);
		grid.add(name, 1, 0);

		TextField namespace = new TextField();
		namespace.setPromptText("namespace");
		namespace.setDisable(true);
		grid.add(new Label("Table namespace:"), 2, 0);
		grid.add(namespace, 3, 0);

		ChoiceBox<String> atCluster = new ChoiceBox();
		appContext.getClusterMap().forEach((k, v) -> {
			atCluster.getItems().add(k);
			atCluster.setValue(k);
		});

		grid.add(new Label("Select cluster:"), 0, 1);
		grid.add(atCluster, 1, 1);

		NumFieldFX presplits = new NumFieldFX();
		presplits.setPromptText("64");
		grid.add(new Label("Presplits:"), 2, 1);
		grid.add(presplits, 3, 1);

		TextField startKey = new TextField();
		startKey.setPromptText("8000");
		grid.add(new Label("Start key (hex):"), 0, 2);
		grid.add(startKey, 1, 2);

		TextField endKey = new TextField();
		endKey.setPromptText("80FF");
		grid.add(new Label("End key (hex):"), 2, 2);
		grid.add(endKey, 3, 2);

		grid.add(cfPropertiesFactory(grid), 0, cfPropertiesOffset, 4, 3);

		ButtonType loginButtonType = new ButtonType("Create", ButtonBar.ButtonData.OK_DONE);
		dialog.getDialogPane().getButtonTypes().addAll(loginButtonType, ButtonType.CANCEL);

		ScrollPane scrollPane = new ScrollPane(grid);
		dialog.getDialogPane().setContent(scrollPane);

		dialog.setResultConverter(dialogButton -> {
			if (dialogButton.getButtonData().isCancelButton()) {
				return null;
			}

			//dealing with presplits
			String presplitsCount = presplits.getText();
			byte[][] presplitsBytes = null;
			if (!presplitsCount.isEmpty()) {
				try {
					int presplitsCountInt = Integer.parseInt(presplitsCount) - 1; //one presplit is allready there
					BigInteger start = new BigInteger(-1, DatatypeConverter.parseHexBinary(startKey.getText()));
					BigInteger end = new BigInteger(-1, DatatypeConverter.parseHexBinary(endKey.getText()));

					if (end.compareTo(start) > 0) {
						mainGui.errorDialogFactory("Presplit format exception", "Presplits error", "End key must be above start key");
						return null;
					} else {
						presplitsBytes = new byte[presplitsCountInt][];
						BigInteger step = (start.subtract(end)).divide(new BigInteger(presplitsCount)).add(new BigInteger("1"));
						BigInteger iterating = start;
						for (int i = 0; i < presplitsCountInt; i++) {
							iterating = iterating.add(step);
							if (iterating.toByteArray()[0] == 0) { //first bit is only for sign and hence is added whole byte
								presplitsBytes[i] = Arrays.copyOfRange(iterating.toByteArray(), 1, iterating.toByteArray().length);
							} else {
								presplitsBytes[i] = iterating.toByteArray();
							}
						}
					}
				} catch (IllegalArgumentException e) {
					mainGui.errorDialogFactory("Presplit format exception", "Presplits error", "Presplits can be only valid HEX numbers");
					return null;
				}
			}

			String tableNameString = name.getText();
			if (tableNameString.isEmpty()) {
				mainGui.errorDialogFactory("Table creation exception", "Empty table name", "Table name must not be empty");
				return null;
			}

			HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableNameString));

			for (GridPane cfGrid : cfPanes) {
				ObservableList<Node> cfNodes = cfGrid.getChildren();
				setDynamicCFNodesList(cfNodes);

				String cfName = getCFOption(1);
				if (cfName.isEmpty()) {
					continue;
				}

				HColumnDescriptor cf = new HColumnDescriptor(cfName);
				cf.setDataBlockEncoding(DataBlockEncoding.valueOf(((ChoiceBox<String>) cfNodes.get(3)).getValue()));
				cf.setBloomFilterType(BloomType.valueOf(((ChoiceBox<String>) cfNodes.get(11)).getValue()));
				cf.setCompressionType(Compression.Algorithm.valueOf(((ChoiceBox<String>) cfNodes.get(13)).getValue()));
				cf.setInMemory(((CheckBox) cfNodes.get(15)).isSelected());

				//set ioptional fields
				if (!getCFOption(5).isEmpty()) {
					cf.setMaxVersions(Integer.parseInt(getCFOption(5)));
				}
				if (!getCFOption(7).isEmpty()) {
					cf.setMinVersions(Integer.parseInt(getCFOption(7)));
				}
				if (!getCFOption(9).isEmpty()) {
					cf.setTimeToLive(Integer.parseInt(getCFOption(9)));
				}
				descriptor.addFamily(cf);
			}
			if (descriptor.getFamilies().isEmpty()) {
				mainGui.errorDialogFactory("Table creation exception", "No column family specified", "Table must contain at least one cf");
				return null;
			}

			return new Triple<>(atCluster.getValue(), descriptor, presplitsBytes);
		});

		Optional<Triple<String, HTableDescriptor, byte[][]>> pureResult = dialog.showAndWait();
		if (pureResult.isPresent()) {
			Triple<String, HTableDescriptor, byte[][]> res = pureResult.get();

			appContext.createTable(res.getFirst(), res.getSecond(), res.getThird(), err -> {
				Platform.runLater(() -> {
					if (err != null) {
						mainGui.errorDialogFactory("Error, cant create table", "Table cannot be created", err);
					} else {
						appContext.refreshTables(res.getFirst(), (suc, ex) -> {
							mainGui.createClustersTreeView();
						});
					}
				});
			});
		}
	}

	private void setDynamicCFNodesList(ObservableList<Node> cfNodes) {
		this.cfDynamicNodes = cfNodes;
	}

	private String getCFOption(int index) {
		return ((TextField) cfDynamicNodes.get(index)).getText();
	}

}

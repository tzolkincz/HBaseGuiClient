package cz.zcu.kiv.hbaseguiclient;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import javafx.beans.property.ReadOnlyBooleanProperty;
import javafx.geometry.Insets;
import javafx.scene.control.ButtonBar;
import javafx.scene.control.ButtonType;
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

public class CreateTableDialog {

	final static AtomicInteger countOfCFs = new AtomicInteger(0);
	static List<TextField> cfNamesList = new ArrayList<>();
	static int cfPropertiesHeight = 3;
	static int cfPropertiesOffset = 3;

	private static GridPane cfPropertiesFactory(GridPane grid) {
		GridPane cfGrid = new GridPane();
		cfGrid.setHgap(5);
		cfGrid.setVgap(10);
		cfGrid.setPadding(new Insets(20, 150, 10, 10));
		cfGrid.setBorder(new Border(
				new BorderStroke(Color.GRAY, BorderStrokeStyle.SOLID, new CornerRadii(15), BorderWidths.DEFAULT)));


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

		/*
		*
		* @TODO bloomfilter
		* @TODO compression
		*
		*
		*/
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

		return cfGrid;
	}

	public static void showCreatePopUp() {
		Dialog<List<String>> dialog = new Dialog<>();
		dialog.setTitle("Create new table");
		GridPane grid = new GridPane();
		grid.setHgap(10);
		grid.setVgap(10);
		grid.setPadding(new Insets(20, 150, 10, 10));

		TextField name = new TextField();
		name.setPromptText("my_table");
		grid.add(new Label("Table name:"), 0, 0);
		grid.add(name, 1, 0);

		TextField namespace = new TextField();
		namespace.setPromptText("namespace");
		grid.add(new Label("Table namespace:"), 2, 0);
		grid.add(namespace, 3, 0);

		ChoiceBox<String> atCluster = new ChoiceBox();
		atCluster.getItems().addAll(
				"Sencha",
				"Piskovisko",
				"Produkce");
		atCluster.setValue("Sencha");
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
			List<String> ret = new ArrayList<>();
			ret.add(name.getText());
			return ret;
		});

		Optional<List<String>> result = dialog.showAndWait();
		result.ifPresent(System.out::println);
	}

}

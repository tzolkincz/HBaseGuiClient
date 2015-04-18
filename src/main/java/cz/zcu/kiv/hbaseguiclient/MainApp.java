package cz.zcu.kiv.hbaseguiclient;

import com.google.common.base.Throwables;
import cz.zcu.kiv.hbaseguiclient.model.AppContext;
import cz.zcu.kiv.hbaseguiclient.model.CommandModel;
import cz.zcu.kiv.hbaseguiclient.model.TableRowDataModel;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Optional;
import javafx.application.Application;
import static javafx.application.Application.launch;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.concurrent.Task;
import javafx.event.EventHandler;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.control.Alert.AlertType;
import javafx.scene.control.TableColumn.CellEditEvent;
import javafx.scene.layout.BorderPane;
import javafx.stage.Stage;
import javafx.scene.control.cell.TextFieldTableCell;
import javafx.scene.input.KeyCode;
import javafx.scene.input.KeyCodeCombination;
import javafx.scene.input.KeyCombination;
import javafx.scene.input.MouseEvent;
import javafx.scene.layout.GridPane;
import javafx.scene.text.Text;
import javafx.util.Pair;

public class MainApp extends Application {

	BorderPane root;
	Scene scene;
	AppContext appContext;
	TableView commandTableView = getCommandTableView();
	public static final String CLUSTER_CONFIG_NAME = "known_cluster.cfg";

	/**
	 * failover for true java fx apps
	 *
	 * @param args
	 */
	public static void main(String[] args) {
		launch(args);
	}

	@Override
	public void start(Stage stage) throws Exception {

		appContext = new AppContext();

		createGui(stage);
		connectToKnownClusters();
		createHeader();
		createClustersTreeView();
		createCli();
	}

	private void createGui(Stage stage) {
		root = new BorderPane();
		scene = new Scene(root);

		stage.setTitle("HBase Client");
		stage.setScene(scene);
		stage.show();
	}

	private void createHeader() {
		MenuBar mainMenu = new MenuBar();

		CreateTableDialog createTableDialog = new CreateTableDialog(this, appContext);

		mainMenu.getMenus().addAll(
				clickableMenuItemFactory("Connect", e -> showConnectPopUp()),
				clickableMenuItemFactory("Create", e -> createTableDialog.showCreatePopUp()),
				clickableMenuItemFactory("Copy", e -> showConnectPopUp()),
				clickableMenuItemFactory("Exit", e -> Platform.exit())
		);
		root.setTop(mainMenu);
	}

	private Menu clickableMenuItemFactory(String text, EventHandler<? super MouseEvent> clickHandler) {
		Menu menu = new Menu();
		Text menuText = new Text(text);
		menu.setGraphic(menuText);
		menuText.setOnMouseClicked(clickHandler);
		return menu;
	}

	private void showConnectPopUp() {
		Dialog<Pair<String, String>> dialog = new Dialog<>();
		dialog.setTitle("Connect to HBase cluster");

		GridPane grid = new GridPane();
		grid.setHgap(10);
		grid.setVgap(10);
		grid.setPadding(new Insets(20, 150, 10, 10));

		TextField name = new TextField();
		name.setPromptText("Production");
		grid.add(new Label("Alias:"), 0, 0);
		grid.add(name, 1, 0);

		TextField zk = new TextField();
		zk.setPromptText("localhost:2181");
		grid.add(new Label("Zookeeper connection:"), 0, 1);
		grid.add(zk, 1, 1);

		ButtonType connectButtonType = new ButtonType("Connect", ButtonBar.ButtonData.OK_DONE);
		dialog.getDialogPane().getButtonTypes().addAll(connectButtonType, ButtonType.CANCEL);

		dialog.getDialogPane().setContent(grid);

		dialog.setResultConverter(dialogButton -> {
			if (dialogButton.getButtonData().isCancelButton()) {
				return null;
			}
			return new Pair<>(zk.getText(), name.getText());
		});

		Node currentLeft = root.getLeft();

		Optional<Pair<String, String>> connectionDialogRes = dialog.showAndWait();
		if (!connectionDialogRes.isPresent()) {
			return;
		}
		Pair<String, String> result = connectionDialogRes.get();
		ProgressIndicator p1 = new ProgressIndicator();
		root.setLeft(p1);
		appContext.createConnection(result.getKey(), result.getValue(), (err, res) -> {
			if (err != null) {
				errorDialogFactory("Error, cant connect", "Cannot connect to cluster", err);
				root.setLeft(currentLeft);
			} else {
				AppContext.addClusterToConfigFileIfNotPresent(res, result.getKey());
				appContext.refreshTables(res, (suc, ex) -> {
					if (suc == true) {
						createClustersTreeView();
					} else {
						errorDialogFactory("Cant get list of tables", ex.getMessage(), Throwables.getStackTraceAsString(ex));
					}
				});
			}
		});
	}

	public void errorDialogFactory(String title, String header, String msg) {
		Alert alert = new Alert(AlertType.ERROR);
		alert.setTitle(title);
		alert.setHeaderText(header);
		alert.setContentText(msg);
		alert.showAndWait();
	}

	public void createClustersTreeView() {
		Platform.runLater(() -> {
			TreeView clustersTreeView = new TreeView();
			TreeItem<String> rootTreeItem = new TreeItem<>("unshowable root");

			appContext.getClusterTables().forEach((cluster, namespaceTablesMap) -> {
				TreeItem<String> clusterTreeItem = new TreeItem(cluster);

				namespaceTablesMap.forEach((namespace, tableList) -> {
					TreeItem<String> namespaceTreeItem = new TreeItem<>(namespace);
					namespaceTreeItem.setExpanded(true);

					tableList.forEach(tableName -> {
						TreeItem<String> tableTreeItem = new TreeItem<>(tableName);
						tableTreeItem.setExpanded(true);
						namespaceTreeItem.getChildren().add(tableTreeItem);
					});

					clusterTreeItem.getChildren().add(namespaceTreeItem);
				});

				clusterTreeItem.setExpanded(true);
				rootTreeItem.getChildren().add(clusterTreeItem);
			});

			clustersTreeView.setRoot(rootTreeItem);
			clustersTreeView.setShowRoot(false);
			root.setLeft(clustersTreeView);
		});
	}

	private void createCli() {
		GridPane cliGrid = new GridPane();

		TextArea commandTextArea = new TextArea("scan table");
		cliGrid.add(commandTextArea, 0, 0, 5, 2);

		CheckBox hexConverstionCheckBox = new CheckBox("Result as HEX");
		cliGrid.add(hexConverstionCheckBox, 0, 2, 4, 1);

		cliGrid.add(commandTableView, 0, 3, 5, 5);

		Button submitCommandButton = new Button("Execute command");
		submitCommandButton.setAlignment(Pos.CENTER_RIGHT);
		scene.getAccelerators().put(
				new KeyCodeCombination(KeyCode.ENTER, KeyCombination.CONTROL_DOWN), () -> {
					submitCommandButton.fire();
				});

		submitCommandButton.setOnAction(a -> {
			ProgressIndicator pi = new ProgressIndicator();
			cliGrid.add(pi, 0, 3, 5, 5);

			final Task<String> t = new Task<String>() {

				@Override
				protected String call() throws Exception {
					String res = CommandModel.submitQuery(commandTextArea.getText(), hexConverstionCheckBox.isSelected());
					Platform.runLater(() -> {
						if (res != null) {
							errorDialogFactory("Error submitting command", "There is some issue with command", res);
						}
						cliGrid.getChildren().remove(pi);
						fillCommandTableViewContent();
					});
					return "hovnoto";
				}
			};

			submitCommandButton.disableProperty().bind(t.runningProperty());

			Thread tr = new Thread(t);
			tr.setDaemon(true);
			tr.start();

		});
		cliGrid.add(submitCommandButton, 4, 2);

		cliGrid.setGridLinesVisible(true);

		root.setCenter(cliGrid);
	}

	private TableView<TableRowDataModel> getCommandTableView() {
		TableView<TableRowDataModel> table = new TableView<>();
		table.setEditable(true);
		return table;
	}

	private void fillCommandTableViewContent() {
		if (CommandModel.getColumns() == null) {
			return;
		}

		//remove all columns
		commandTableView.getColumns().clear();

		//add rowKey column
		TableColumn<TableRowDataModel, String> rowKeyColumn = new TableColumn<>("Row key");
		rowKeyColumn.setCellValueFactory(c -> {
			return new SimpleStringProperty(c.getValue().getRowKey());
		});
		commandTableView.getColumns().add(rowKeyColumn);

		//add data columns
		CommandModel.getColumns().forEach(columnName -> {
			TableColumn<TableRowDataModel, String> tableColumn = new TableColumn<>(columnName);

			tableColumn.setCellValueFactory(c -> {
				return new SimpleStringProperty(c.getValue().getValue(columnName));
			});

			tableColumn.setCellFactory(TextFieldTableCell.<TableRowDataModel>forTableColumn());
			tableColumn.setOnEditCommit(
					(CellEditEvent<TableRowDataModel, String> t) -> {
						((TableRowDataModel) t.getTableView().getItems().get(t.getTablePosition().getRow()))
						.setValue(columnName, t.getNewValue());
					});
			commandTableView.getColumns().add(tableColumn);

		});

		//set data
		ObservableList<TableRowDataModel> tableData = FXCollections.observableArrayList();
		CommandModel.getQueryResult().forEach((rowKey, kv) -> {
			tableData.add(new TableRowDataModel(rowKey, kv));
		});

		commandTableView.setItems(tableData);
	}

	private void connectToKnownClusters() {
		ProgressIndicator pi = new ProgressIndicator();
		root.setLeft(pi);

		try {
			java.nio.file.Files.lines(Paths.get(System.getProperty("user.dir"), CLUSTER_CONFIG_NAME)).forEach(line -> {
				String[] aliasAndZk = line.split("\t");
				String alias = aliasAndZk[0];
				String zk = aliasAndZk[1];

				appContext.createConnection(zk, alias, (err, finalAlias) -> {
					appContext.refreshTables(finalAlias, (suc, ex) -> {
						if (suc == true) {
							createClustersTreeView();
						}
					});
				});
			});
		} catch (IOException ex) {
			createClustersTreeView();
			System.out.println("clusters config file not found or err: " + ex);
		}
	}

}

package cz.zcu.kiv.hbaseguiclient;

import com.google.common.base.Throwables;
import cz.zcu.kiv.hbaseguiclient.model.AppContext;
import cz.zcu.kiv.hbaseguiclient.model.CommandModel;
import cz.zcu.kiv.hbaseguiclient.model.TableRowDataModel;
import java.util.stream.IntStream;
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
import javafx.scene.control.Alert;
import javafx.scene.control.Alert.AlertType;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonBar;
import javafx.scene.control.ButtonType;
import javafx.scene.control.CheckBox;
import javafx.scene.control.Dialog;
import javafx.scene.control.Label;
import javafx.scene.control.Menu;
import javafx.scene.control.MenuBar;
import javafx.scene.control.ProgressIndicator;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableColumn.CellEditEvent;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.layout.BorderPane;
import javafx.stage.Stage;
import javafx.scene.control.TreeItem;
import javafx.scene.control.TreeView;
import javafx.scene.control.cell.TextFieldTableCell;
import javafx.scene.input.MouseEvent;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.text.Text;
import javafx.util.Pair;

public class MainApp extends Application {

	BorderPane root;
	Scene scene;
	AppContext appContext;
	TableView commandTableView = getCommandTableView();

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
		mainMenu.getMenus().addAll(
				clickableMenuItemFactory("Connect", e -> showConnectPopUp()),
				clickableMenuItemFactory("Create", e -> CreateTableDialog.showCreatePopUp(appContext)),
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

		ButtonType loginButtonType = new ButtonType("Connect", ButtonBar.ButtonData.OK_DONE);
		dialog.getDialogPane().getButtonTypes().addAll(loginButtonType, ButtonType.CANCEL);

		dialog.getDialogPane().setContent(grid);

		dialog.setResultConverter(dialogButton -> {
			return new Pair<>(zk.getText(), name.getText());
		});

		Node currentLeft = root.getLeft();
		Pair<String, String> result = dialog.showAndWait().get();
		ProgressIndicator p1 = new ProgressIndicator();
		root.setLeft(p1);
		appContext.createConnection(result.getKey(), result.getValue(), (err, res) -> {
			if (err != null) {
				errorDialogFactory("Error, cant connect", "Cannot connect to cluster", err);
				root.setLeft(currentLeft);
			} else {
				System.out.println("done");
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

	private void errorDialogFactory(String title, String header, String msg) {
		Alert alert = new Alert(AlertType.ERROR);
		alert.setTitle(title);
		alert.setHeaderText(header);
		alert.setContentText(msg);
		alert.showAndWait();
	}

	private void createClustersTreeView() {
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
	}

	private void createCli() {
		GridPane cliGrid = new GridPane();

		TextArea commandTextArea = new TextArea("scan table");
		cliGrid.add(commandTextArea, 0, 0, 5, 2);

		CheckBox phoenixDataTypeCheckBox = new CheckBox("Phoenix data types converison");
		cliGrid.add(phoenixDataTypeCheckBox, 0, 2, 4, 1);

		cliGrid.add(commandTableView, 0, 3, 5, 5);

		Button submitCommandButton = new Button("Execute command");
		submitCommandButton.setAlignment(Pos.CENTER_RIGHT);
		submitCommandButton.setOnAction(a -> {
			ProgressIndicator pi = new ProgressIndicator();
			cliGrid.add(pi, 0, 3, 5, 5);

			final Task<String> t = new Task<String>() {

				@Override
				protected String call() throws Exception {

					String res = CommandModel.submitQuery(commandTextArea.getText());
					Platform.runLater(() -> {
						if (res != null) {
							errorDialogFactory("Error submitting command", "There is some issue with command", res);
						}
						System.out.println("test");
						cliGrid.getChildren().remove(pi);
						fillCommandTableViewContent();
					});
					return "hovnoto";
				}
			};

			submitCommandButton.disableProperty().bind(t.runningProperty());
			t.stateProperty().addListener(state -> {
				System.out.println("state changed: " + state);
			});

			Thread tr = new Thread(t);
			tr.setDaemon(true);
			tr.start();

		});
		cliGrid.add(submitCommandButton, 4, 2);

		cliGrid.setGridLinesVisible(true);

		root.setCenter(cliGrid);
	}

	private TableView<TableRowDataModel> getCommandTableView() {
		TableView<TableRowDataModel> commandTableView = new TableView<>();

		commandTableView.setEditable(true);

		return commandTableView;
	}

	private void fillCommandTableViewContent() {

		TableColumn<TableRowDataModel, String>[] tableColumns = new TableColumn[10];


		IntStream.range(0, tableColumns.length).forEach(i -> {
			tableColumns[i] = new TableColumn<>(String.valueOf(i));
			tableColumns[i].setCellValueFactory(c -> {
				return new SimpleStringProperty(c.getValue().getValues().get(i));
			});

			if (i != 0) { //row key is not modifiable
				tableColumns[i].setCellFactory(TextFieldTableCell.<TableRowDataModel>forTableColumn());
				tableColumns[i].setOnEditCommit(
						(CellEditEvent<TableRowDataModel, String> t) -> {
							((TableRowDataModel) t.getTableView().getItems().get(t.getTablePosition().getRow()))
							.getValues().set(i, t.getNewValue());
						});
			}
		});

		commandTableView.getColumns().addAll(tableColumns);

		ObservableList<TableRowDataModel> tableData = FXCollections.observableArrayList();

		tableData.add(new TableRowDataModel());
		tableData.add(new TableRowDataModel());
		tableData.add(new TableRowDataModel());

		commandTableView.setItems(tableData);
	}

}

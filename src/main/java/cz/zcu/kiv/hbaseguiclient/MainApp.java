package cz.zcu.kiv.hbaseguiclient;

import cz.zcu.kiv.hbaseguiclient.model.AppContext;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javafx.application.Application;
import static javafx.application.Application.launch;
import javafx.application.Platform;
import javafx.event.EventHandler;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.control.Alert;
import javafx.scene.control.Alert.AlertType;
import javafx.scene.control.ButtonBar;
import javafx.scene.control.ButtonType;
import javafx.scene.control.Dialog;
import javafx.scene.control.Label;
import javafx.scene.control.Menu;
import javafx.scene.control.MenuBar;
import javafx.scene.control.ProgressIndicator;
import javafx.scene.control.TextField;
import javafx.scene.layout.BorderPane;
import javafx.stage.Stage;
import javafx.scene.control.TreeItem;
import javafx.scene.control.TreeView;
import javafx.scene.input.MouseEvent;
import javafx.scene.layout.GridPane;
import javafx.scene.text.Text;
import javafx.util.Pair;

public class MainApp extends Application {

	BorderPane root;
	Scene scene;
	AppContext appContext;

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
				clickableMenuItemFactory("Create", e -> CreateTableDialog.showCreatePopUp()),
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
			return new Pair<>(name.getText(), zk.getText());
		});

		Pair<String, String> result = dialog.showAndWait().get();
		try {
			ProgressIndicator p1 = new ProgressIndicator();
			//root.getChildren().add(p1);
			root.setLeft(p1);


			new Thread( () -> {
				try {
					Thread.sleep(10000);
				} catch (InterruptedException ex) {
					Logger.getLogger(MainApp.class.getName()).log(Level.SEVERE, null, ex);
				}
			}).start();

			Thread.sleep(0, 1);


//			appContext.createConnection(result.getKey(), result.getValue());
//		} catch (IOException ex) {
//			//open err dialog
//			Alert alert = new Alert(AlertType.ERROR);
//			alert.setTitle("Connection Error");
//			alert.setHeaderText("Unable to connect to cluster");
//			alert.setContentText(ex.getMessage());
//			ex.printStackTrace();
//			alert.showAndWait();
		} catch (InterruptedException ex) {
			Logger.getLogger(MainApp.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	private void createClustersTreeView() {
		TreeView clustersTreeView = new TreeView();

		appContext.getClusterTables().forEach((cluster, namespaceTablesMap) -> {
			TreeItem<String> clusterTreeItem = new TreeItem(cluster);

			namespaceTablesMap.forEach((namespace, tableList) -> {
				TreeItem<String> namespaceTreeItem = new TreeItem<>(namespace);

				tableList.forEach(tableName -> {
					TreeItem<String> tableTreeItem = new TreeItem<>(tableName);
					namespaceTreeItem.getChildren().add(tableTreeItem);
				});

				clusterTreeItem.getChildren().add(namespaceTreeItem);
			});

			clustersTreeView.setRoot(clusterTreeItem);
		});
		root.setLeft(clustersTreeView);
	}

}

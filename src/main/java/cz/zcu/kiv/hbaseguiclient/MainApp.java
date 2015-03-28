package cz.zcu.kiv.hbaseguiclient;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import javafx.application.Application;
import static javafx.application.Application.launch;
import javafx.application.Platform;
import javafx.beans.property.ReadOnlyBooleanProperty;
import javafx.event.EventHandler;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.control.ButtonBar.ButtonData;
import javafx.scene.control.ButtonType;
import javafx.scene.control.ChoiceBox;
import javafx.scene.control.Dialog;
import javafx.scene.control.Label;
import javafx.scene.control.Menu;
import javafx.scene.control.MenuBar;
import javafx.scene.control.TextField;
import javafx.scene.layout.BorderPane;
import javafx.stage.Stage;
import javafx.scene.control.TextInputDialog;
import javafx.scene.input.KeyEvent;
import javafx.scene.input.MouseEvent;
import javafx.scene.layout.Border;
import javafx.scene.layout.BorderStroke;
import javafx.scene.layout.BorderStrokeStyle;
import javafx.scene.layout.BorderWidths;
import javafx.scene.layout.CornerRadii;
import javafx.scene.layout.GridPane;
import javafx.scene.text.Text;

public class MainApp extends Application {

	BorderPane root;
	Scene scene;

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
		createGui(stage);
		createHeader();
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
		TextInputDialog dialog = new TextInputDialog("localhost:2181");
		dialog.setTitle("Connect to cluster");
		dialog.setContentText("Enter the Connection string to Zookeeper:");

		Optional<String> result = dialog.showAndWait();
		result.ifPresent(name -> System.out.println("Your name: " + name));
	}


}

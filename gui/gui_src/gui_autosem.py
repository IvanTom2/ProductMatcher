import os
import sys
import json
import pandas as pd

from pathlib import Path
from PyQt6.QtWidgets import (
    QTreeView,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QComboBox,
    QMenuBar,
    QFileDialog,
    QApplication,
    QFormLayout,
    QDialog,
    QTableView,
    QCheckBox,
)
from PyQt6.QtCore import Qt, QAbstractTableModel, QModelIndex, QVariant


GUI_DIR = Path(__file__).parent.parent
GUI_SRC = GUI_DIR / "gui_src"
PROJECT_DIR = GUI_DIR.parent
AUTOSEM_CONFIG_PATH = PROJECT_DIR / "config" / "autosem_config" / "setups"

sys.path.append(str(PROJECT_DIR))

from jsonmodel import JsonModel
from src.autosem.measures_extraction import MeasuresExtractor
from src.autosem.cross_semantic import CrosserPro, LanguageRules


class ConfigViewerDialog(QDialog):
    def __init__(
        self,
        config_path: str | Path,
        config_name: str,
        reset_configs: callable,
    ):
        super().__init__()

        self.config_path = config_path
        self.reset_configs = reset_configs

        self.json_model = JsonModel()
        self.json_model.load(self.load_data(config_name))

        config_layout = QFormLayout(self)

        tree_view = QTreeView(self)
        tree_view.setModel(self.json_model)

        button = QPushButton("Сохранить")
        button.clicked.connect(self.save_button)

        config_layout.addWidget(tree_view)
        config_layout.addWidget(button)

        self.setLayout(config_layout)
        self.setWindowTitle("Config Viewer")

    def load_data(self, config_name) -> dict:
        path = self.config_path / config_name
        with open(path, "rb") as file:
            json_data = json.load(file)
        return json_data

    def save_data(self, config_name: str, data: dict) -> None:
        if ".json" not in config_name:
            config_name += ".json"

        path = self.config_path / config_name
        with open(path, "w") as file:
            file.write(json.dumps(data, ensure_ascii=False))

        self.reset_configs()

    def save_button(self):
        data: dict = self.json_model.to_json()
        config_name = data["config_name"]
        self.save_data(config_name, data)

        self.close()


class PandasModel(QAbstractTableModel):
    def __init__(self, dataframe: pd.DataFrame = pd.DataFrame(), parent=None):
        QAbstractTableModel.__init__(self, parent)
        self._dataframe = dataframe

    def rowCount(self, parent=QModelIndex()) -> int:
        if parent == QModelIndex():
            return len(self._dataframe)
        return 0

    def columnCount(self, parent=QModelIndex()) -> int:
        if parent == QModelIndex():
            return len(self._dataframe.columns)
        return 0

    def data(self, index: QModelIndex, role=Qt.ItemDataRole):
        if not index.isValid():
            return None

        if role == Qt.ItemDataRole.DisplayRole:
            return str(self._dataframe.iloc[index.row(), index.column()])

        return None

    def headerData(
        self, section: int, orientation: Qt.Orientation, role: Qt.ItemDataRole
    ):
        if role == Qt.ItemDataRole.DisplayRole:
            if orientation == Qt.Orientation.Horizontal:
                return str(self._dataframe.columns[section])

            if orientation == Qt.Orientation.Vertical:
                return str(self._dataframe.index[section])

        return None


class AutosemProcessRunner(object):
    def __init__(
        self,
        config_path: str | Path,
        data_path: str | Path,
        column: str,
        cross_sem_langs: list[str] = ["ru", "eng"],
    ) -> None:
        self.data_path = data_path
        self.column = column

        self.extractor = MeasuresExtractor(config_path, True)

        crosser_lang_rules = self.setup_crosser_lang_rules(cross_sem_langs)
        self.crosser = CrosserPro(
            crosser_lang_rules,
            delete_rx=True,
            process_nearest=250,
        )

    def upload_data(self):
        if ".csv" in self.data_path:
            data = pd.read_csv(self.data_path)
        elif ".xlsx" in self.data_path:
            data = pd.read_excel(self.data_path)
        else:
            raise ValueError("File should be Excel or csv")
        return data

    def setup_crosser_lang_rules(
        self,
        use_languages: list[str],
    ) -> list[LanguageRules]:
        langs = []

        if "ru" in use_languages:
            langs.append(
                LanguageRules(
                    "russian",
                    check_letters=True,
                    with_numbers=True,
                    min_lenght=3,
                    stemming=True,
                    symbols="",
                )
            )

        if "eng" in use_languages:
            langs.append(
                LanguageRules(
                    "english",
                    check_letters=True,
                    with_numbers=True,
                    min_lenght=3,
                    stemming=True,
                    symbols="",
                )
            )

        return langs

    def run(self) -> None:
        data = self.upload_data()

        data = self.extractor.extract(data, self.column, concat_regex=True)
        data = self.crosser.extract(data, self.column)

        data.to_excel(PROJECT_DIR / "autosem_output.xlsx", index=False)


class AutosemWidget(QWidget):
    def __init__(
        self,
        config_path: str | Path,
    ):
        super().__init__()
        self.CONFIG_PATH = config_path

        main_layout = QVBoxLayout(self)

        self.file_path_display = self._setup_workfile_layout(main_layout)
        self.config_combobox = self._setup_config_layout(main_layout)
        self.workcol_display = self._setup_runner(main_layout)
        self.cross_sem_langs = self._setup_cross_sem(main_layout)
        self.table_view = self._setup_table_view(main_layout)

        # self._setup_menu()

    def _setup_workfile_layout(self, main_layout: QVBoxLayout) -> QLineEdit:
        file_layout = QHBoxLayout()

        file_path_label = QLabel("Файл для обработки:")
        self.file_path_display = QLineEdit(self)

        self.browse_button = QPushButton("Обзор", self)
        self.browse_button.clicked.connect(self.browse_file)

        file_layout.addWidget(file_path_label)
        file_layout.addWidget(self.file_path_display)
        file_layout.addWidget(self.browse_button)

        main_layout.addLayout(file_layout)

        return self.file_path_display

    def _setup_config_layout(self, main_layout: QVBoxLayout) -> QComboBox:
        config_layout = QHBoxLayout()

        config_label = QLabel("Конфигурация:")
        self.config_combobox = QComboBox(self)

        change_config_button = QPushButton("Редактировать")
        change_config_button.clicked.connect(self.change_config)

        config_layout.addWidget(config_label)
        config_layout.addWidget(self.config_combobox)
        config_layout.addWidget(change_config_button)

        main_layout.addLayout(config_layout)
        self.update_config_combobox()

        return self.config_combobox

    def _setup_table_view(self, main_layout: QVBoxLayout) -> QTableView:
        table_layout = QHBoxLayout()
        self.table_view = QTableView()
        model = PandasModel()

        self.table_view.setModel(model)
        table_layout.addWidget(self.table_view)

        main_layout.addLayout(table_layout)
        return self.table_view

    def _setup_runner(self, main_layout: QVBoxLayout) -> None:
        runner_layout = QHBoxLayout()

        workcol_label = QLabel("Столбец для обработки")
        self.workcol_display = QLineEdit("Название клиента")

        run_button = QPushButton("Начать обработку")
        run_button.clicked.connect(self.run)

        runner_layout.addWidget(workcol_label)
        runner_layout.addWidget(self.workcol_display)
        runner_layout.addWidget(run_button)

        main_layout.addLayout(runner_layout)

        return self.workcol_display

    def _setup_menu(self):
        menu_bar = QMenuBar(self)

        file_menu = menu_bar.addMenu("File")
        edit_menu = menu_bar.addMenu("Edit")

        self.layout().setMenuBar(menu_bar)

    def _setup_cross_sem(self, main_layout: QVBoxLayout) -> list[QCheckBox]:
        cross_sem_langs = []
        cross_sem_layout = QHBoxLayout()

        label = QLabel("Языки для кросс-семантики: ")

        ru_cross_sem = QCheckBox("ru")
        ru_cross_sem.setChecked(True)
        cross_sem_langs.append(ru_cross_sem)

        eng_cross_sem = QCheckBox("eng")
        eng_cross_sem.setChecked(True)
        cross_sem_langs.append(eng_cross_sem)

        cross_sem_layout.addWidget(label)
        cross_sem_layout.addWidget(ru_cross_sem)
        cross_sem_layout.addWidget(eng_cross_sem)

        main_layout.addLayout(cross_sem_layout)
        return cross_sem_langs

    def update_config_combobox(self):
        if self.CONFIG_PATH:
            config_files = [
                file for file in os.listdir(self.CONFIG_PATH) if file.endswith(".json")
            ]

            self.config_combobox.clear()
            self.config_combobox.addItems(config_files)

    def browse_file(self) -> None:
        file_dialog = QFileDialog()
        file_path, _ = file_dialog.getOpenFileName(
            self, "Выберите файл для обработки", "", "Excel (*.xlsx *.csv)"
        )
        if file_path:
            self.file_path_display.setText(file_path)
            self.upload_file_data(file_path)

    def upload_file_data(self, file_path: str) -> None:
        if ".xlsx" in file_path:
            data = pd.read_excel(file_path, nrows=5)
        elif ".csv" in file_path:
            data = pd.read_excel(file_path, nrows=5)
        else:
            raise ValueError("File should be Excel or csv")

        self.table_view.setModel(PandasModel(data))

    def change_config(self):
        selected_config = self.config_combobox.currentText()

        config_viewer_dialog = ConfigViewerDialog(
            self.CONFIG_PATH,
            selected_config,
            self.update_config_combobox,
        )
        config_viewer_dialog.exec()

    def run(self):
        config_path = self.CONFIG_PATH / self.config_combobox.currentText()

        cross_sem_langs = []
        for lang in self.cross_sem_langs:
            if lang.isChecked():
                cross_sem_langs.append(lang.text())

        extractor = AutosemProcessRunner(
            config_path,
            self.file_path_display.text(),
            self.workcol_display.text(),
            cross_sem_langs,
        )

        extractor.run()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = AutosemWidget(AUTOSEM_CONFIG_PATH)
    window.show()

    sys.exit(app.exec())

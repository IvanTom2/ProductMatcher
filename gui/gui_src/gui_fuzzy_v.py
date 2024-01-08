import sys
import pandas as pd
from pathlib import Path
from PyQt6.QtWidgets import (
    QVBoxLayout,
    QApplication,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QLineEdit,
)

GUI_DIR = Path(__file__).parent.parent
GUI_SRC = GUI_DIR / "gui_src"
PROJECT_DIR = GUI_DIR.parent
CONFIG_PATH = PROJECT_DIR / "config" / "fuzzy_config" / "setups"

sys.path.append(str(PROJECT_DIR))

from gui_common import CommonGUI
from src.fuzzy_v.jakkar import FuzzyJakkarValidator, setup_jakkar_validator


class FuzzyValidatorProcessRunner(object):
    def __init__(
        self,
        config: str | Path,
        data_path: str | Path,
        client_column: str,
        source_column: str,
        fuzzy_threshold: int,
        validation_threshold: 1,
    ) -> None:
        self.client_column = client_column
        self.source_column = source_column

        self.data_path = data_path
        self.fuzzy_validator = setup_jakkar_validator(
            config,
            float(fuzzy_threshold),
            float(validation_threshold),
        )

    def upload_data(self):
        if ".csv" in self.data_path:
            data = pd.read_csv(self.data_path)
        elif ".xlsx" in self.data_path:
            data = pd.read_excel(self.data_path)
        else:
            raise ValueError("File should be Excel or csv")
        return data

    def run(self):
        data = self.upload_data()
        data: pd.DataFrame = self.fuzzy_validator.validate(
            data,
            self.client_column,
            self.source_column,
        )

        data.to_excel(PROJECT_DIR / "fuzzy_v_output.xlsx", index=False)


class FuzzyValidatorWidget(CommonGUI):
    CONFIG_PATH = CONFIG_PATH

    def __init__(self):
        super().__init__()

        main_layout = QVBoxLayout(self)

        self.workfile_lay = self._setup_workfile_layout(main_layout)
        self.config_lay = self._setup_config_layout(main_layout)
        self._setup_runner(main_layout)

        self.table_lay = self._setup_table_view(main_layout)

    def _setup_runner(self, main_layout: QVBoxLayout) -> None:
        runner_layout = QVBoxLayout()

        fuzzy_threshold_box = QHBoxLayout()
        fuzzy_threshold_label = QLabel("Порог изменения токена")
        self.fuzzy_threshold_display = QLineEdit("0.75")
        fuzzy_threshold_box.addWidget(fuzzy_threshold_label)
        fuzzy_threshold_box.addWidget(self.fuzzy_threshold_display)

        validation_threshold_box = QHBoxLayout()
        validation_threshold_label = QLabel("Порог валидации")
        self.validation_threshold_display = QLineEdit("0.5")
        validation_threshold_box.addWidget(validation_threshold_label)
        validation_threshold_box.addWidget(self.validation_threshold_display)

        params_box = QVBoxLayout()
        params_box.addLayout(fuzzy_threshold_box)
        params_box.addLayout(validation_threshold_box)

        client_box = QHBoxLayout()
        client_col_label = QLabel("Столбец названий клиента")
        self.client_col_display = QLineEdit("Название товара клиента")
        client_box.addWidget(client_col_label)
        client_box.addWidget(self.client_col_display)

        source_box = QHBoxLayout()
        source_col_label = QLabel("Столбец названий источника")
        self.source_col_display = QLineEdit("Строка валидации")
        source_box.addWidget(source_col_label)
        source_box.addWidget(self.source_col_display)

        run_button = QPushButton("Начать обработку")
        run_button.clicked.connect(self.run)

        runner_layout.addLayout(params_box)
        runner_layout.addLayout(client_box)
        runner_layout.addLayout(source_box)

        runner_layout.addWidget(run_button)
        main_layout.addLayout(runner_layout)

    def run(self):
        config_path = self.CONFIG_PATH / self.config_combobox.currentText()
        config = self.read_config(config_path)

        validator = FuzzyValidatorProcessRunner(
            config,
            self.file_path_display.text(),
            self.client_col_display.text(),
            self.source_col_display.text(),
            self.fuzzy_threshold_display.text(),
            self.validation_threshold_display.text(),
        )

        validator.run()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = FuzzyValidatorWidget()
    window.show()

    sys.exit(app.exec())

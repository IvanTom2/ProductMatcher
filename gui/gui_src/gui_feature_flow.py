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
CONFIG_PATH = PROJECT_DIR / "config" / "measures_config" / "setups"

sys.path.append(str(PROJECT_DIR))

from gui_common import CommonGUI
from src.feature_flow.main import FeatureGenerator, FeatureFlow

OUTPUT_FILENAME = "FeatureFlow_output.xlsx"
FEATURE_FLOW_CLIENT_COL = "Название товара"
FEATURE_FLOW_SOURCE_COL = "Сырые данные"


class FeatureFlowProcessRunner(object):
    def __init__(
        self,
        config: dict,
        data_path: str | Path,
        client_column: str,
        source_column: str,
    ) -> None:
        self.data_path = data_path

        self.feature_generator = FeatureGenerator()
        features = self.feature_generator.generate(config)

        self.validator = FeatureFlow(
            client_column,
            source_column,
            features,
        )

    def upload_data(self):
        if ".csv" in self.data_path:
            data = pd.read_csv(self.data_path)
        elif ".xlsx" in self.data_path:
            data = pd.read_excel(self.data_path)
        else:
            raise ValueError("File should be Excel or csv")
        return data

    def run(self, process_pool=None) -> None:
        data = self.upload_data()
        data = self.validator.validate(data, process_pool)
        data.to_excel(PROJECT_DIR / OUTPUT_FILENAME, index=False)


class FeatureFlowWidget(CommonGUI):
    CONFIG_PATH = CONFIG_PATH

    def __init__(self, process_pool=None):
        super().__init__()
        self._process_pool = process_pool

        main_layout = QVBoxLayout(self)

        self.workfile_lay = self._setup_workfile_layout(main_layout)
        self.config_lay = self._setup_config_layout(main_layout)
        self._setup_runner(main_layout)

        self.table_lay = self._setup_table_view(main_layout)

    def _setup_runner(self, main_layout: QVBoxLayout) -> None:
        runner_layout = QVBoxLayout()

        client_box = QHBoxLayout()
        client_col_label = QLabel("Столбец названий клиента")
        self.client_col_display = QLineEdit(FEATURE_FLOW_CLIENT_COL)
        client_box.addWidget(client_col_label)
        client_box.addWidget(self.client_col_display)

        source_box = QHBoxLayout()
        source_col_label = QLabel("Столбец названий источника")
        self.source_col_display = QLineEdit(FEATURE_FLOW_SOURCE_COL)
        source_box.addWidget(source_col_label)
        source_box.addWidget(self.source_col_display)

        run_button = QPushButton("Начать обработку")
        run_button.clicked.connect(self.run)

        runner_layout.addLayout(client_box)
        runner_layout.addLayout(source_box)

        runner_layout.addWidget(run_button)
        main_layout.addLayout(runner_layout)

    def run(self):
        config_path = self.CONFIG_PATH / self.config_combobox.currentText()
        config = self.read_config(config_path)

        validator = FeatureFlowProcessRunner(
            config,
            self.file_path_display.text(),
            self.client_col_display.text(),
            self.source_col_display.text(),
        )

        validator.run(self._process_pool)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = FeatureFlowWidget()
    window.show()

    sys.exit(app.exec())

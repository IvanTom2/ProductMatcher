import sys
import time
import pandas as pd

from pathlib import Path
from PyQt6.QtWidgets import (
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QApplication,
    QCheckBox,
)

GUI_DIR = Path(__file__).parent.parent
GUI_SRC = GUI_DIR / "gui_src"
PROJECT_DIR = GUI_DIR.parent
CONFIG_PATH = PROJECT_DIR / "config" / "measures_config" / "setups"

sys.path.append(str(PROJECT_DIR))

from gui_common import CommonGUI
from src.autosem.measures_extraction import MeasuresExtractor
from src.autosem.cross_semantic import CrosserPro, LanguageRules


class AutosemProcessRunner(object):
    def __init__(
        self,
        config: str | Path,
        data_path: str | Path,
        column: str,
        cross_sem_langs: list[str] = ["ru", "eng"],
    ) -> None:
        self.data_path = data_path
        self.column = column

        self.extractor = MeasuresExtractor(config, True)

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


class AutosemWidget(CommonGUI):
    CONFIG_PATH = CONFIG_PATH

    def __init__(self):
        super().__init__()

        main_layout = QVBoxLayout(self)

        self.workfile_lay = self._setup_workfile_layout(main_layout)
        self.config_lay = self._setup_config_layout(main_layout)
        self.cross_sem_langs = self._setup_cross_sem(main_layout)
        self.workcol_display = self._setup_runner(main_layout)
        self.table_lay = self._setup_table_view(main_layout)

    def _setup_runner(self, main_layout: QVBoxLayout) -> None:
        workcol_layout = QHBoxLayout()
        workcol_label = QLabel("Столбец для обработки")
        self.workcol_display = QLineEdit("Название клиента")
        workcol_layout.addWidget(workcol_label)
        workcol_layout.addWidget(self.workcol_display)

        runner_layout = QHBoxLayout()
        run_button = QPushButton("Начать обработку")
        run_button.clicked.connect(self.run)
        runner_layout.addWidget(run_button)

        main_layout.addLayout(workcol_layout)
        main_layout.addLayout(runner_layout)

        self.run_button = run_button
        return self.workcol_display

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

    def run(self):
        self.run_button.setText("Обрабатывается...")

        config_path = self.CONFIG_PATH / self.config_combobox.currentText()
        config = self.read_config(config_path)

        cross_sem_langs = []
        for lang in self.cross_sem_langs:
            if lang.isChecked():
                cross_sem_langs.append(lang.text())

        extractor = AutosemProcessRunner(
            config,
            self.file_path_display.text(),
            self.workcol_display.text(),
            cross_sem_langs,
        )

        extractor.run()
        self.run_button.setText("Начать обработку")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = AutosemWidget()
    window.show()

    sys.exit(app.exec())

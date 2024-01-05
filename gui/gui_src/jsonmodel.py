import json
import sys
from pathlib import Path
from typing import Any, List, Dict, Union

from PyQt6.QtWidgets import QTreeView, QApplication, QHeaderView, QDialog, QFormLayout
from PyQt6.QtCore import QAbstractItemModel, QModelIndex, QObject, Qt, QFileInfo

from config.autosem_config.config_parser import CONFIG, MEASURE, DATA, FEATURE


class TreeItem:
    def __init__(self, parent: "TreeItem" = None):
        self._parent = parent
        self._key = ""
        self._value = ""
        self._value_type = None
        self._children = []

    def appendChild(self, item: "TreeItem"):
        self._children.append(item)

    def child(self, row: int) -> "TreeItem":
        return self._children[row]

    def parent(self) -> "TreeItem":
        return self._parent

    def childCount(self) -> int:
        return len(self._children)

    def row(self) -> int:
        return self._parent._children.index(self) if self._parent else 0

    @property
    def key(self) -> str:
        return self._key

    @key.setter
    def key(self, key: str):
        self._key = key

    @property
    def value(self) -> str:
        return self._value

    @value.setter
    def value(self, value: str):
        self._value = value

    @property
    def value_type(self):
        return self._value_type

    @value_type.setter
    def value_type(self, value):
        self._value_type = value

    def search_keyname(self, current_name, value: dict) -> str:
        key_name = ""
        if MEASURE.NAME in value:
            key_name = value[MEASURE.NAME]
        if FEATURE.NAME in value:
            key_name = value[FEATURE.NAME]

        return key_name if key_name else current_name

    def load(
        self,
        value: Union[List, Dict],
        parent: "TreeItem" = None,
    ) -> "TreeItem":
        rootItem = TreeItem(parent)
        rootItem.key = "root"

        if isinstance(value, dict):
            "Using for nested dicts"

            items = value.items()

            for key, value in items:
                child = self.load(value, rootItem)
                child.key = key
                child.value_type = type(value)
                rootItem.appendChild(child)

        elif isinstance(value, list):
            "Using for nested lists"

            for index, value in enumerate(value):
                key_name = self.search_keyname(index, value)

                child = self.load(value, rootItem)
                child.key = key_name
                child.value_type = type(value)
                rootItem.appendChild(child)

        else:
            "Using for simple records"

            rootItem.value = value
            rootItem.value_type = type(value)

        return rootItem


class JsonModel(QAbstractItemModel):
    def __init__(self, parent: QObject = None):
        super().__init__(parent)

        self._rootItem = TreeItem()
        self._headers = ("key", "value")

    def clear(self):
        self.load({})

    def load(self, document: dict):
        assert isinstance(document, (dict, list, tuple)), (
            "`document` must be of dict, list or tuple, " f"not {type(document)}"
        )

        self.beginResetModel()

        self._rootItem = self._rootItem.load(document)
        self._rootItem.value_type = type(document)

        self.endResetModel()

        return True

    def data(self, index: QModelIndex, role: Qt.ItemDataRole) -> Any:
        if not index.isValid():
            return None

        item = index.internalPointer()

        if role == Qt.ItemDataRole.DisplayRole:
            if index.column() == 0:
                return item.key

            if index.column() == 1:
                return item.value

        elif role == Qt.ItemDataRole.EditRole:
            if index.column() == 1:
                return item.value

    def setData(self, index: QModelIndex, value: Any, role: Qt.ItemDataRole):
        if role == Qt.ItemDataRole.EditRole:
            if index.column() == 1:
                item = index.internalPointer()
                item.value = str(value)

                self.dataChanged.emit(index, index, [Qt.ItemDataRole.EditRole])

                return True

        return False

    def headerData(
        self, section: int, orientation: Qt.Orientation, role: Qt.ItemDataRole
    ):
        if role != Qt.ItemDataRole.DisplayRole:
            return None

        if orientation == Qt.Orientation.Horizontal:
            return self._headers[section]

    def index(self, row: int, column: int, parent=QModelIndex()) -> QModelIndex:
        if not self.hasIndex(row, column, parent):
            return QModelIndex()

        if not parent.isValid():
            parentItem = self._rootItem
        else:
            parentItem = parent.internalPointer()

        childItem = parentItem.child(row)
        if childItem:
            return self.createIndex(row, column, childItem)
        else:
            return QModelIndex()

    def parent(self, index: QModelIndex) -> QModelIndex:
        if not index.isValid():
            return QModelIndex()

        childItem = index.internalPointer()
        parentItem = childItem.parent()

        if parentItem == self._rootItem:
            return QModelIndex()

        return self.createIndex(parentItem.row(), 0, parentItem)

    def rowCount(self, parent=QModelIndex()):
        if parent.column() > 0:
            return 0

        if not parent.isValid():
            parentItem = self._rootItem
        else:
            parentItem = parent.internalPointer()

        return parentItem.childCount()

    def columnCount(self, parent=QModelIndex()):
        return 2

    def flags(self, index: QModelIndex) -> Qt.ItemFlag:
        flags = super(JsonModel, self).flags(index)

        if index.column() == 1:
            return Qt.ItemFlag.ItemIsEditable | flags
        else:
            return flags

    def to_json(self, item=None):
        if item is None:
            item = self._rootItem

        nchild = item.childCount()

        if item.value_type is dict:
            document = {}
            for i in range(nchild):
                ch = item.child(i)
                document[ch.key] = self.to_json(ch)
            return document

        elif item.value_type == list:
            document = []
            for i in range(nchild):
                ch = item.child(i)
                document.append(self.to_json(ch))
            return document

        else:
            return item.value


if __name__ == "__main__":
    app = QApplication(sys.argv)
    view = QTreeView()
    model = JsonModel()

    view.setModel(model)

    json_path = QFileInfo(__file__).absoluteDir().filePath("example.json")

    with open(json_path) as file:
        document = json.load(file)
        model.load(document)

    view.show()
    view.header().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
    view.setAlternatingRowColors(True)
    view.resize(500, 300)
    app.exec()

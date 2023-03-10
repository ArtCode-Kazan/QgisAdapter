from typing import List, Union
from dataclasses import dataclass

from psycopg2 import connect

from qgis.core import (QgsProject, QgsVectorLayer, QgsRasterLayer,
                       QgsDataSourceUri, QgsLayerTreeGroup,
                       QgsLayerTreeLayer, QgsLayerTreeNode,
                       QgsExpressionContextUtils
                       )
from PyQt5.QtWidgets import QMessageBox

import processing


def remove_useless_styles(layer: QgsVectorLayer, need_style_name: str):
    all_styles = layer.styleManager().mapLayerStyles()
    for style_name in all_styles.keys():
        if style_name != need_style_name:
            layer.styleManager().removeStyle(style_name)


def set_style(shp_layer: QgsVectorLayer, db_layer: QgsVectorLayer):
    style_shp_name = shp_layer.styleManager().currentStyle()
    style_shp = shp_layer.styleManager().style(style_shp_name)

    style_name = f'Style-{shp_layer.name()}'
    if style_name in db_layer.styleManager().styles():
        db_layer.styleManager().removeStyle(style_name)

    db_layer.styleManager().addStyle(style_name, style_shp)
    db_layer.styleManager().setCurrentStyle(style_name)

    remove_useless_styles(layer=db_layer, need_style_name=style_name)
    return db_layer


@dataclass
class PostgresConnection:
    host: str
    db_name: str
    user: str
    password: str
    port: int = 5432
    schema: str = 'public'

    __connection = None

    @property
    def dict_view(self) -> dict:
        return {
            'HOST': self.host,
            'PORT': str(self.port),
            'USER': self.user,
            'DBNAME': self.db_name,
            'PASSWORD': self.password,
            'SCHEMA': self.schema
        }

    @property
    def connection(self):
        if not self.__connection:
            self.__connection = connect(
                host=self.host,
                port=self.port,
                dbname=self.db_name,
                user=self.user,
                password=self.password,
            )
        return self.__connection

    @staticmethod
    def get_from_env() -> 'PostgresConnection':
        project = QgsProject.instance()
        project_scope = QgsExpressionContextUtils.projectScope(project)
        host = project_scope.variable('host')
        port = int(project_scope.variable('port'))
        db_name = project_scope.variable('dbname')
        user = project_scope.variable('user')
        password = project_scope.variable('password')
        return PostgresConnection(
            host=host,
            db_name=db_name,
            user=user,
            password=password,
            port=port
        )


class QGISAdapter:
    def __init__(self, connection_params: PostgresConnection):
        self.__connection_params = connection_params

    @property
    def connection_params(self) -> PostgresConnection:
        return self.__connection_params

    @property
    def layers(self) -> List[Union[QgsVectorLayer, QgsRasterLayer]]:
        return list(QgsProject.instance().mapLayers().values())

    def format_layer_names(self):
        for layer in self.layers:
            correct_name = layer.name().replace(' ', '_').replace('-', '_')
            layer.setName(correct_name)

    def get_nonunique_layer_names(self) -> List[str]:
        names_list = []
        unique_names = set()
        for layer in self.layers:
            layer_name = layer.name()
            if layer_name not in unique_names:
                unique_names.add(layer_name)
            else:
                names_list.append(layer_name)
        return names_list

    def get_all_vector_layers(self) -> List[QgsVectorLayer]:
        vector_layers = []
        for layer in self.layers:
            if isinstance(layer, QgsVectorLayer):
                vector_layers.append(layer)
        return vector_layers

    def get_all_raster_layers(self) -> List[QgsRasterLayer]:
        raster_layers = []
        for layer in self.layers:
            if isinstance(layer, QgsRasterLayer):
                raster_layers.append(layer)
        return raster_layers

    @property
    def shp_layers(self) -> List[QgsVectorLayer]:
        shp_layers = []
        for layer in self.get_all_vector_layers():
            layer_source, is_spatial = layer.source(), layer.isSpatial()
            if 'dbname' not in layer_source and is_spatial:
                shp_layers.append(layer)
        return shp_layers

    @property
    def db_vector_layers(self) -> List[QgsVectorLayer]:
        db_vector_layers = []
        for layer in self.get_all_vector_layers():
            layer_source = layer.source()
            if 'dbname' in layer_source:
                db_vector_layers.append(layer)
        return db_vector_layers

    @property
    def local_raster_layers(self) -> List[QgsRasterLayer]:
        local_layers = []
        for layer in self.get_all_raster_layers():
            if 'dbname' not in layer.source():
                local_layers.append(layer)
        return local_layers

    @property
    def db_raster_layers(self) -> List[QgsRasterLayer]:
        local_layers = []
        for layer in self.get_all_raster_layers():
            if 'dbname' in layer.source():
                local_layers.append(layer)
        return local_layers

    def save_shp_layers_to_dbase(self):
        for layer in self.shp_layers:
            layer_dict = {
                'INPUT': layer.source(),
                'TABLE': layer.name(),
                'OVERWRITE': True,
            }

            processing.run(
                'gdal:importvectorintopostgisdatabasenewconnection',
                layer_dict | self.connection_params.dict_view
            )

    def get_duplicate_shp_from_dbase(self,
                                     layer: QgsVectorLayer) -> QgsVectorLayer:
        uri = QgsDataSourceUri()
        uri.setConnection(
            self.connection_params.host,
            str(self.connection_params.port),
            self.connection_params.db_name,
            self.connection_params.user,
            self.connection_params.password
        )
        uri.setDataSource(
            self.connection_params.schema, layer.name().lower(), 'geom'
        )

        db_layer = QgsVectorLayer(
            uri.uri(False), layer.name(), self.connection_params.user
        )
        return db_layer

    def get_layers_from_dbase(self):
        loading_layers = []
        for layer in self.shp_layers:
            loading_layers.append(
                self.get_duplicate_shp_from_dbase(layer=layer)
            )
        return loading_layers

    def is_style_table_exist(self):
        connection = self.connection_params.connection
        cursor = connection.cursor()
        cursor.execute(
            'SELECT EXISTS(SELECT FROM information_schema.tables WHERE '
            'table_schema = \'public\' AND table_name = \'layer_styles\');'
        )
        return cursor.fetchone()[0]

    def remove_style_from_db(self, style_name: str):
        if not self.is_style_table_exist():
            return
        connection = self.connection_params.connection
        cursor = connection.cursor()
        cursor.execute(
            f'DELETE FROM layer_styles WHERE stylename=\'{style_name}\''
        )
        connection.commit()
        cursor.close()

    def remove_all_local_layers_project(self):
        for layer in self.layers:
            if 'dbname' not in layer.source():
                QgsProject.instance().removeMapLayer(layer)

    def patch_layer_styles_table(self):
        if not self.is_style_table_exist():
            return

        connection = self.connection_params.connection
        cursor = connection.cursor()
        query = (
            """
            UPDATE layer_styles ls 
            set type = 
                case 
                    when (SELECT "type" FROM geometry_columns as gc WHERE gc.f_table_name=ls.f_table_name) in ('LINESTRING', 'MULTILINESTRING') then 'Line'
                    when (SELECT "type" FROM geometry_columns as gc WHERE gc.f_table_name=ls.f_table_name) in ('POINT', 'MULTIPOINT') then 'Point'
                    when (SELECT "type" FROM geometry_columns as gc WHERE gc.f_table_name=ls.f_table_name) in ('POLYGON', 'MULTIPOLYGON') then 'Polygon'
                end
            """
        )
        cursor.execute(query)
        connection.commit()
        cursor.close()

    def set_styles(self):
        db_layers = self.get_layers_from_dbase()
        for i, shp_layer in enumerate(self.shp_layers):
            db_layer = db_layers[i]
            db_layers[i] = set_style(shp_layer=shp_layer, db_layer=db_layer)
        return db_layers

    def load_styles_to_dbase(self, layers):
        for layer in layers:
            style_name = layer.styleManager().currentStyle()
            self.remove_style_from_db(style_name=style_name)
            layer.saveStyleToDatabase(style_name, '', True, '')

    def replace_shp_layers_in_group(self, group: QgsLayerTreeGroup):
        all_tree_layers = group.findLayers()
        for i, tree_layer in enumerate(all_tree_layers):
            layer = tree_layer.layer()
            if 'dbname' in layer.source():
                continue

            if 'dbname' not in layer.source() and layer.isSpatial():
                db_layer = self.get_duplicate_shp_from_dbase(
                    layer=layer
                )
                QgsProject.instance().addMapLayer(db_layer, False)
                group.insertChildNode(-1, QgsLayerTreeLayer(db_layer))

    def replace_shp_layers(self):
        root = QgsProject.instance().layerTreeRoot()
        tree_groups = set()
        for layer in self.shp_layers:
            tree_groups.add(root.findLayer(layer.id()).parent())

        for group in tree_groups:
            self.replace_shp_layers_in_group(group=group)

    def run(self):
        self.format_layer_names()
        non_unique_layer_names = self.get_nonunique_layer_names()
        message_text = (
                'You have the non-unique layers in the project:\n' +
                '\n'.join(non_unique_layer_names)
        )
        if non_unique_layer_names:
            QMessageBox.information(None, 'Warning!', message_text)
            return

        self.save_shp_layers_to_dbase()
        layers = self.set_styles()

        self.load_styles_to_dbase(layers)
        self.patch_layer_styles_table()

        self.remove_all_local_layers_project()

        if layers:
            root = QgsProject.instance().layerTreeRoot()
            new_group = root.insertGroup(0, 'Postgres-layers')
            for layer in layers:
                QgsProject.instance().addMapLayer(layer, False)
                new_group.insertChildNode(-1, QgsLayerTreeLayer(layer))


def openProject():
    pass


def saveProject():
    conn_params = PostgresConnection.get_from_env()
    adapter = QGISAdapter(connection_params=conn_params)
    adapter.run()


def closeProject():
    conn_params = PostgresConnection.get_from_env()
    adapter = QGISAdapter(connection_params=conn_params)
    adapter.run()

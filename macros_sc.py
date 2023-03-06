import qgis
import processing
import os
from qgis.core import QgsProject, QgsDataSourceUri, QgsVectorLayer

def sort_layers(layers):
    vector_list = []
    raster_list = []
    for val in layers.values():
        if isinstance(val, qgis._core.QgsVectorLayer):
            vector_list.append(val)
        else:
            raster_list.append(val)
    return vector_list, raster_list


def sort_vector_layers(vector_list):
    vector_nonpostgres = []
    vector_postgres = []
    for v_layer in vector_list:
        vlayersource = v_layer.source()
        geometry_v_layer = v_layer.isSpatial()
        if not 'dbname' in vlayersource:
            if geometry_v_layer:
                vector_nonpostgres.append(v_layer)
            else:
                continue
        else:
            vector_postgres.append(v_layer)
    return vector_nonpostgres, vector_postgres


def sort_raster_layers(raster_list):
    raster_nonpostgres = []
    raster_postgres = []
    for r_layer in raster_list:
        rlayersource = r_layer.source()
        if 'dbname' not in rlayersource:
            raster_nonpostgres.append(r_layer)
        else:
            raster_postgres.append(r_layer)
    return raster_nonpostgres, raster_postgres


def load_shp_to_pg(vector_nonpostgres):
    for vlayer in vector_nonpostgres:
        print(vlayer)
        processing.run(
            "gdal:importvectorintopostgisdatabasenewconnection",
            {
                'INPUT': vlayer.source(),
                'HOST': 'localhost',
                'PORT': '5432',
                'USER': 'postgres',
                'DBNAME': 'rastertest',
                'PASSWORD': '456789123',
                'SCHEMA': 'public',
                'TABLE': vlayer.name(),
                'OVERWRITE': False,
            }
        )
        print(f'{vlayer.name()} done')


def load_styles_to_pg (vector_postgres):
    for vlayer in vector_postgres:
        if vlayer.name() not in vlayer.listStylesInDatabase():
            vlayer.saveStyleToDatabase(vlayer.name(), '', True, '')
            print(f'{vlayer.name()} done')


def get_layers_from_pg(vector_nonpostgres):
    layer_from_postgres = []
    for vlayer in vector_nonpostgres:
        uri = QgsDataSourceUri()
        uri.setConnection("localhost", "5432", "rastertest", "postgres", "456789123")
        uri.setDataSource("public", vlayer.name().lower(), "geom")

        pgvlayer = QgsVectorLayer(uri.uri(False), vlayer.name(), "postgres")
        layer_from_postgres.append(pgvlayer)
        QgsProject.instance().addMapLayer(pgvlayer)
    return layer_from_postgres


def set_style(vector_nonpostgres, layer_from_postgres):
    for i in range(len(layer_from_postgres)):
        shp_layer = vector_nonpostgres[i]
        pg_layer = layer_from_postgres[i]
        style_shp_name = shp_layer.styleManager().currentStyle()
        style_shp = shp_layer.styleManager().style(style_shp_name)
        if 'mystyle' in pg_layer.styleManager().styles():
            pg_layer.styleManager().removeStyle('mystyle')
        pg_layer.styleManager().addStyle('mystyle', style_shp)
        pg_layer.styleManager().setCurrentStyle('mystyle')
        pg_layer.triggerRepaint()


def delete_shp_layers(vector_nonpostgres):
    for shp_layer in vector_nonpostgres:
        QgsProject.instance().removeMapLayer(shp_layer)

def openProject():
    layers = QgsProject.instance().mapLayers()
    vector_list, raster_list = sort_layers(layers)
    vector_nonpostgres, vector_postgres = sort_vector_layers(vector_list)
    layer_from_postgres = get_layers_from_pg(vector_nonpostgres)
    set_style(vector_nonpostgres, layer_from_postgres)
    delete_shp_layers(vector_nonpostgres)
    pass



def saveProject():
    layers = QgsProject.instance().mapLayers()
    vector_list, raster_list = sort_layers(layers)
    vector_nonpostgres, vector_postgres = sort_vector_layers(vector_list)
    raster_nonpostgres, raster_postgres = sort_raster_layers(raster_list)
    load_shp_to_pg(vector_nonpostgres)
    pass

def closeProject():
    layers = QgsProject.instance().mapLayers()
    vector_list, raster_list = sort_layers(layers)
    vector_nonpostgres, vector_postgres = sort_vector_layers(vector_list)
    load_styles_to_pg(vector_postgres)
    pass
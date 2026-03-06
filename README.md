# solus-stac
a STAC catalog for SOLUS data

For dataset details see:
* https://storage.googleapis.com/solus100pub/index.html
* https://agdatacommons.nal.usda.gov/articles/dataset/Data_from_Soil_Landscapes_of_the_United_States_100-meter_SOLUS100_soil_property_maps_project_repository/25033856

Metadata from https://storage.googleapis.com/solus100pub/Final_Layer_Table_20231215.csv

## Catalog Structure

Each SOLUS property/Tif is provided with three asset corresponding to a best estimate and uncertainty bounds:
* p: predicted property values
* l: prediction interval low
* h: prediction interval high
* rpi: relative prediction interval

STAC is commonly used to group collections of common items by their unique 'datetime' (e.g. acquisition dates of observations by the same satellite). So 'time' naturally becomes the 3rd dimension of a multidimensional xarray. For SOLUS however, all estimates share the same 'datetime' and are instead differentiated by 'depth' (0cm, 5cm, etc). This dictates the structure of our catalog. To easily ingest into Xarray, we need 'depth' to be a top-level property of every Item, just like 'datetime'.

We therefore organize our STAC catalog by each SOLUS estimate type so that we can easily load a mulidimensional xarray with data variables corresponding to each physical property (cec7, sandco, etc) and a 3rd dimension corresponding to the depth (0cm, 5cm, etc).

```
    Catalog
    ├── Collection: p
    │   ├── Collection: depth_0cm
    │       ├── Item: caco3
                └── Asset: caco3
    │       └── Item: sandco
                └── Asset: sandco
    │   └── ...
    ├── Collection: h
    │   ├── Collection: depth_0cm
    │       ├── Item: caco3
                └── Asset: caco3
    │       └── Item: sandco
                └── Asset: sandco
    │   └── ...
    ├── Collection: rpi
    │   └── ...
    └── ...

```

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

We want to design a logically organized STAC catalog that works well with ODC.STAC. This assumes "Items from the same collection are assumed to have the same number and names of bands, and bands are assumed to use the same data_type across the collection." (https://odc-stac.readthedocs.io/en/latest/stac-best-practice.html#assumptions)

STAC is commonly used to group collections of common items by their unique 'datetime' (e.g. acquisition dates of observations by the same satellite). So 'time' naturally becomes the 3rd dimension of a multidimensional xarray. For SOLUS however, all estimates share the same 'datetime' and are instead differentiated by 'depth' (0cm, 5cm, etc). This dictates the structure of our catalog.

We will therefore have a single Item for each depth, which has an Asset corresponding to each property (cec7, sandco, etc) and a Collection for each estimate type (p, l, h, rpi). The exception is a separate Collection for soil_thickness, where there is no 'depth'.

```
    Catalog
    ├── Collection: soil_thickness
        ├── Item: p
            ├── Asset: anylithicdpt
            └── Asset: resdept
        └── Item: rpi
            ├── Asset: anylithicdpt
            └── Asset: resdept
        └── Item: ...
    ├── Collection: prediction (p)
    │   ├── Item: depth_0cm
    │       ├── Asset: caco3 (https://storage.googleapis.com/solus100pub/caco3_0_cm_p.tif)
            ├── Asset: sandco
    │       └── ...
    │   └── Item: depth_5cm
    │       ├── Asset: caco3
            ├── Asset: sandco
    │       └── ...
    ├── Collection: low (l)
    │   ├── Item: depth_0cm
    │       ├── Asset: caco3 (https://storage.googleapis.com/solus100pub/caco3_0_cm_l.tif)
            ├── Asset: sandco
    │       └── ...
│       └── Item: depth_5cm
            └── Asset: sandco
    │   └── ...
    ├── Collection: high (h)
    │   └── ...
    └── Collection: interval (rpi)

```

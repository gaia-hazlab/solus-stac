# solus-stac
a STAC catalog for SOLUS data

For dataset details see:
* https://storage.googleapis.com/solus100pub/index.html
* https://agdatacommons.nal.usda.gov/articles/dataset/Data_from_Soil_Landscapes_of_the_United_States_100-meter_SOLUS100_soil_property_maps_project_repository/25033856


Metadata from https://storage.googleapis.com/solus100pub/Final_Layer_Table_20231215.csv

## Catalog Structure:

```
Catalog: catalog.json
    Collection: soil_thickness.json
        Item: soil_thickness.json
            Assets:
                anylithicdpt_cm_2D_h.tif
                anylithicdpt_cm_2D_l.tif
                anylithicdpt_cm_2D_p.tif
                anylithicdpt_cm_2D_rpi.tif
    Collection: depth_0cm.json
        Item: caco3.json
            Assets:
                caco3_0_cm_h.tif
                caco3_0_cm_l.tif
                caco3_0_cm_p.tif
                caco3_0_cm_rpi.tif
        Item: claytotal.json
            Assets:
                claytotal_0_cm_h.tif
                claytotal_0_cm_l.tif
                claytotal_0_cm_p.tif
        ...
    Collection: depth_5cm.json
        Item: caco3.json
            Assets:
                caco3_5_cm_h.tif
                caco3_5_cm_l.tif
                caco3_5_cm_p.tif
                caco3_5_cm_rpi.tif
        ...
    Collection: depth_150cm.json
        ...
```

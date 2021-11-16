# AutoBEM-DynamicArchetypes
Automatic Building Energy Modeling (AutoBEM, https://bit.ly/AutoBEM) has been used to create an OpenStudio and EnergyPlus building energy model of 122.9 million U.S. buildings (https://bit.ly/ModelAmerica). Simulating and analyzing a model of every building for large areas (e.g. cities) is often not feasible. This dynamic archetyping capability uses a representative building and calculates a floor-space multiplier that allows millions of buildings to be represented by less than 100 buildings.

This script (WRF_Archetypes_Parallel.py) calculates these building archetypes for each of the grid cells from a Weather Research and Forecasting (WRF) model in a parallel fashion. The script works by looping through each of the grid cells in the shapefile in parallel, spatially joining the building metadata table to each grid cell, aggregating relevant archetypes and calculating necessary statistics related to area and number of buildings in each cell. The output is a table (.csv) in which each row is an archetype building with properties about that building as well as statistics that relate that building to the total cell (such as an area multiplier).

The following inputs are required:

1. WRF zone shapefile (.shp) (wrf-grids-origin_Vegas_Select_100.geojson)
2. The projection of the shapefile ("EPSG:XXXX")
3. Input table containing building metadata for area corresponding to shapefile (.csv) (https://zenodo.org/record/4552901#.YZQEotDMJPY - ClarkCounty2.csv)
4. The number of cores that will be parallelized (integer)
5. The output file name for the archetype table (.csv)

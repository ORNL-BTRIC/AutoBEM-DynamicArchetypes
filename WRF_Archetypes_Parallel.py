# -*- coding: utf-8 -*-
"""
Created on Tue Jul 20 11:00:38 2021

@author: Brett Bass
"""

import pandas as pd
import geopandas as gpd
from argparse import ArgumentParser
from shapely.geometry import Point
import numpy as np
import time
from joblib import Parallel, delayed


def parse_inputs():
    parser = ArgumentParser(description='WRF Archetypes')

    parser.add_argument('-i', '--input_shp_file', dest="input_shp_file", metavar="shp", help='input shp file for WRF',
                        )						

    parser.add_argument('-c', '--input_csv_file', dest="input_csv_file", metavar="csv", help='input csv with building metadata',
                        )	

    parser.add_argument('-o', '--output', dest="output_file", metavar="CSV", help='Output csv name',
                        )

    parser.add_argument('-e', '--EPSG', dest="EPSG", metavar="str", help='EPSG of geospatial file, format example - "EPSG:4326" ',
                        )		

    parser.add_argument('-j', '--num_cores', dest="num_cores", metavar="num_cores", help='Number of Cores',
                        )					
						
    results = parser.parse_args()

    return results

def AggregateArchetypes(All_Zones, gdf_df):
    All_zone_buildings_All_Zones = []

    building_types = ['IECC',
                  'Apartment',
                  'Office',
                  'Retail',
                  'School',
                  'Medical',
                  'Restaurant',
                  'Hotel',
                  'Warehouse']

    vintages = ['Pre_1980',
                '1980_2004',
                'Post_2004']
    
    i = All_Zones.index[0]
    for i_in in range(len(All_Zones)):
        zone = pd.DataFrame(All_Zones.loc[i_in+i]).transpose()
        zone = gpd.GeoDataFrame(zone, crs="EPSG:4326", geometry = zone['geometry'])
        
        zone_buildings = gpd.sjoin(gdf_df, zone, how="inner", op='intersects')
        zone_buildings.drop('index_right', axis = 1, inplace = True)
        
        if len(zone_buildings) > 0:
            print(i_in)
            
            ii=0
            for build in building_types:
                for vin in vintages:
                    if vin == 'Pre_1980':
                        vin_actual = 'DOE-Ref-Pre-1980'
                    if vin == '1980_2004':
                        vin_actual = 'DOE-Ref-1980-2004'   
                    if vin == 'Post_2004':
                        vin_actual = '90.1-2004|90.1-2007|90.1-2010|90.1-2013'                       

                    if build == 'Medical':
                        build_actual = 'Outpatient|Hospital'
                    else:
                        build_actual = build
                    
                    if (zone_buildings['BuildingType'].str.contains(build_actual) & zone_buildings['Standard'].str.contains(vin_actual)).any():    
                        zone_buildings_BT_VIN = zone_buildings[zone_buildings['BuildingType'].str.contains(build_actual) & zone_buildings['Standard'].str.contains(vin_actual)]
    
                        Num_build_per_zone = len(zone_buildings_BT_VIN)
                        Total_zone_area = zone_buildings_BT_VIN['Area'].sum(axis=0)
                        
                        
                        zone_building = zone_buildings_BT_VIN.iloc[:1]
                       
                        Area_multiplier = Total_zone_area / zone_building['Area'].iloc[0]
                        
                        zone_building = zone_building.reset_index()    
                        zone_building.loc[0,'Num_build_per_zone'] = Num_build_per_zone
                        zone_building.loc[0,'Total_zone_area'] = Total_zone_area
                        zone_building.loc[0,'Area_multiplier'] =  Area_multiplier
                        zone_building.loc[0,'Vintage'] = vin
                        zone_building.loc[0,'BuildingType_general'] = build
                        
                        
                        if ii == 0:
                            All_zone_buildings = zone_building
                        if ii > 0:
                            All_zone_buildings = pd.concat((All_zone_buildings, zone_building), axis = 0) 
                        ii += 1
                        
            All_zone_buildings['Zone_ID'] = All_Zones.loc[i_in+i,'id']
        
        else:
            continue

        All_zone_buildings_All_Zones.append(All_zone_buildings)        
    
    return(All_zone_buildings_All_Zones) 

if __name__ == '__main__':
    tic = time.time()
    
    parsed_inputs = parse_inputs()
    in_shp_path = parsed_inputs.input_shp_file
    input_csv_file = parsed_inputs.input_csv_file   
    out_path = parsed_inputs.output_file
    num_cores = int(parsed_inputs.num_cores)    
    EPSG = str(parsed_inputs.EPSG)
    
    All_Zones = gpd.read_file(in_shp_path)
    All_Zones.crs  = EPSG
    All_Zones = All_Zones.to_crs("EPSG:4326") 
    All_Zones = All_Zones[['id','geometry']]
    
    df = pd.read_csv(input_csv_file)
    df[['lat', 'lon']] = df['Centroid'].str.split('/', 1, expand=True)
    df = df[['ID','Area','Centroid','CZ', 'Height', 'NumFloors','BuildingType', 'Standard', 'Footprint2D', 'WWR_surfaces','Area2D', 'lat', 'lon']]
    
    geometry = [Point(xy) for xy in zip(df['lon'].astype(float), df['lat'].astype(float))]
    gdf_df = gpd.GeoDataFrame(df, crs="EPSG:4326", geometry=geometry)
    
    splits = num_cores
    All_Zones_split = np.array_split(All_Zones, splits)
    
 
    Output = Parallel(n_jobs=num_cores, backend='multiprocessing')(delayed(AggregateArchetypes)(All_Zones_split[j], gdf_df) for j in range(splits))
    Output_list_concat =[]
    for out in Output:
        Output_list_concat.append(pd.concat(out))
    Output_df = pd.concat(Output_list_concat)
    toc = time.time()
    print(toc - tic)

    
    Output_df.to_csv(out_path, index = False)

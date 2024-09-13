import spinedb_api as api
from spinedb_api import DatabaseMapping
import datetime
import pandas as pd
import geopandas as gpd
import sys
from openpyxl import load_workbook
import numpy as np
import json

def add_entity(db_map : DatabaseMapping, class_name : str, name : str, ent_description = None) -> None:
    _, error = db_map.add_entity_item(name=name, entity_class_name=class_name, description = ent_description)
    if error is not None:
        raise RuntimeError(error)

def add_parameter_value(db_map : DatabaseMapping,class_name : str,parameter : str,alternative : str,elements : tuple,value : any) -> None:
    db_value, value_type = api.to_database(value)
    _, error = db_map.add_parameter_value_item(entity_class_name=class_name,entity_byname=elements,parameter_definition_name=parameter,alternative_name=alternative,value=db_value,type=value_type)
    if error:
        raise RuntimeError(error)

def add_relationship(db_map : DatabaseMapping,class_name : str,element_names : tuple, rel_name = None, rel_description = None) -> None:
    _, error = db_map.add_entity_item(element_name_list=element_names, entity_class_name=class_name, name=rel_name, description = rel_description)
    if error is not None:
        raise RuntimeError(error)

def add_alternative(db_map : DatabaseMapping,name_alternative : str) -> None:
    _, error = db_map.add_alternative_item(name=name_alternative)
    if error is not None:
        raise RuntimeError(error)
    
def time_index(year) -> list:
    time_list = {}
    pd_range = pd.date_range(str(int(year))+"-01-01 00:00:00",str(int(year))+"-12-31 23:00:00",freq="h")
    time_list["standard"] = [i.strftime('%Y-%m-%d %H:%M:%S') for i in pd_range]
    time_list["iso"]  = [i.isoformat() for i in pd_range]
    return time_list["standard"], time_list["iso"]


if __name__ == "__main__":

    url_db_out = sys.argv[1]

    exparent_wind_on   = pd.read_excel("capacity_wind-on-existing.xlsx",sheet_name="Country_level",index_col=0)[2025]
    exparent_solar_PV  = pd.read_excel("capacity_solar-PV-existing.xlsx",sheet_name="Country_level",index_col=0)[2025]
    existing_wind_on   = pd.read_excel("capacity_wind-on-existing.xlsx",sheet_name="Regional",index_col=0)[2025]
    existing_wind_off  = pd.read_excel("capacity_wind-off-existing.xlsx",sheet_name="Regional",index_col=0)[2025]
    existing_solar_PV  = pd.read_excel("capacity_solar-PV-existing.xlsx",sheet_name="Regional",index_col=0)[2025]
    potential_wind_on  = pd.read_excel("potential_wind-on.xlsx",index_col=0)["Greenfield_potential_GW"]
    potential_wind_off = pd.read_excel("potential_wind-off.xlsx",index_col=0)["Greenfield_potential_GW"]
    potential_solar_PV = pd.read_excel("potential_solar-PV.xlsx",index_col=0)["Greenfield_potential_GW"]
    
    poparent_wind_on  = pd.Series(0.0,index=list(set([i[:2] for i in potential_wind_on.index])))
    poparent_wind_off = pd.Series(0.0,index=list(set([i[:4] for i in potential_wind_off.index])))
    poparent_solar_PV = pd.Series(0.0,index=list(set([i[:2] for i in potential_solar_PV.index])))
    exparent_wind_off = pd.Series(0.0,index=list(set([i[:4] for i in existing_wind_off.index])))


    for i in potential_wind_on.index:
        poparent_wind_on.loc[i[:2]] += potential_wind_on.at[i]
    for i in potential_wind_off.index:
        poparent_wind_off.loc[i[:4]] += potential_wind_off.at[i]
    for i in potential_solar_PV.index:
        poparent_solar_PV.loc[i[:2]] += potential_solar_PV.at[i]
    for i in existing_wind_off.index:
        exparent_wind_off.loc[i[:4]] += existing_wind_off.at[i]

    vre_cost = pd.read_csv("VRE_costs.csv",index_col=0)

    availability = {}
    for tech in vre_cost.index:
        availability[tech] = pd.read_csv(tech+".csv",index_col=0)

    print("Data loaded")

    # map = {"type":"map","rank":1,"index_type":"str","index_name":index_name,"data":{}}
    climate_years = [1995,2008,2009]
    with DatabaseMapping(url_db_out) as db_map:
        
        ## Empty the database
        db_map.purge_items('entity')
        db_map.purge_items('parameter_value')
        db_map.purge_items('alternative')
        db_map.purge_items('scenario')
        db_map.refresh_session()

        add_alternative(db_map,"Base")

        CY_index = {"iso":[],"standard":[]}
        for CY in climate_years:
            indexes = time_index(CY)
            CY_index["iso"]      += indexes[1]
            CY_index["standard"] += indexes[0]

        add_entity(db_map,"technology_type","wind-on")
        add_entity(db_map,"technology_type","wind-off")
        add_entity(db_map,"technology_type","solar-PV")

        for tech in vre_cost.index:
            add_entity(db_map,"technology",tech)
            add_relationship(db_map,"technology_type__technology",("wind-on",tech))
            map_icost = map = {"type":"map","index_type":"str","index_name":"year","data":{year:round(vre_cost.at[tech,"capex_"+year]*1e6,1) for year in ["2030","2040","2050"]}}
            map_fcost = map = {"type":"map","index_type":"str","index_name":"year","data":{year:vre_cost.at[tech,"fom_"+year] for year in ["2030","2040","2050"]}}
            map_vcost = map = {"type":"map","index_type":"str","index_name":"year","data":{year:vre_cost.at[tech,"vom_"+year] for year in ["2030","2040","2050"]}}
            add_parameter_value(db_map,"technology","investment_cost","Base",(tech,),map_icost)
            add_parameter_value(db_map,"technology","fixed_cost","Base",(tech,),map_fcost)
            add_parameter_value(db_map,"technology","operational_cost","Base",(tech,),map_vcost)
            add_parameter_value(db_map,"technology","lifetime","Base",(tech,),vre_cost.at[tech,"lifetime"])

        ## ONSHORE EXISTING
        for poly in existing_wind_on.index:
            tech = "wind-on-existing"
            if existing_wind_on.round(2).at[poly] > 0 and poly in availability[tech].columns:

                try:
                    add_entity(db_map,"region",poly)
                    add_parameter_value(db_map,"region","type","Base",(poly,),"onshore")
                    add_parameter_value(db_map,"region","GIS_level","Base",(poly,),"PECD2")
                except:
                    pass
                try:
                    add_relationship(db_map,"technology_type__region",("wind-on",poly))
                    add_parameter_value(db_map,"technology_type__region","greenfield_potentials","Base",("wind-on",poly),round(float(potential_wind_on.at[poly])*1e3,1))
                except:
                    pass

                weight = {"type":"map","index_type":"str","index_name":"to_level","data":{"PECD1":round(existing_wind_on.at[poly]/exparent_wind_on.at[poly[:2]],3)}}
                existing = round(float(existing_wind_on.at[poly]*1e3),1)
                profile = {"type":"time_series","data": dict(zip(CY_index["iso"],availability[tech].loc[CY_index["standard"],poly].round(3).tolist()))}
                
                add_relationship(db_map,"technology__region",(tech,poly))
                add_parameter_value(db_map,"technology__region","units_existing","Base",(tech,poly),existing)              
                add_parameter_value(db_map,"technology__region","weight_profile_upper_limit","Base",(tech,poly),weight)    
                add_parameter_value(db_map,"technology__region","profile_upper_limit","Base",(tech,poly),profile)

        print("existing_wind_onshore")

        ## ONSHORE FUTURE
        technologies = ["wind-on-SP335-HH100","wind-on-SP335-HH150","wind-on-SP277-HH100","wind-on-SP277-HH150","wind-on-SP198-HH100","wind-on-SP198-HH150"]
        for tech in technologies:
            for poly in availability[tech].columns:
                if poly in potential_wind_on.index:
                    try:
                        add_entity(db_map,"region",poly)
                        add_parameter_value(db_map,"region","type","Base",(poly,),"onshore")
                        add_parameter_value(db_map,"region","GIS_level","Base",(poly,),"PECD2")
                    except:
                        pass
                    try:
                        add_relationship(db_map,"technology_type__region",("wind-on",poly))
                        add_parameter_value(db_map,"technology_type__region","greenfield_potentials","Base",("wind-on",poly),round(float(potential_wind_on.at[poly])*1e3,1))
                    except:
                        pass

                    weight = {"type":"map","index_type":"str","index_name":"to_level","data":{"PECD1":round(potential_wind_on.at[poly]/poparent_wind_on.at[poly[:2]],3)}}
                    existing = 0.0
                    profile = {"type":"time_series","data": dict(zip(CY_index["iso"],availability[tech].loc[CY_index["standard"],poly].round(3).tolist()))}
                    
                    add_relationship(db_map,"technology__region",(tech,poly))
                    add_parameter_value(db_map,"technology__region","units_existing","Base",(tech,poly),existing)              
                    add_parameter_value(db_map,"technology__region","weight_profile_upper_limit","Base",(tech,poly),weight)    
                    add_parameter_value(db_map,"technology__region","profile_upper_limit","Base",(tech,poly),profile)
        print("wind_on_future")
        ## ONSHORE SOLAR
        share = {"solar-PV-no-tracking":0.2,"solar-PV-rooftop":0.8,"solar-PV-tracking":0.0}
        technologies = ["solar-PV-no-tracking","solar-PV-rooftop","solar-PV-tracking"]
        for tech in technologies:
            for poly in existing_solar_PV.index:
                if poly in availability[tech].columns and poly in potential_solar_PV.index:

                    try:
                        add_entity(db_map,"region",poly)
                        add_parameter_value(db_map,"region","type","Base",(poly,),"onshore")
                        add_parameter_value(db_map,"region","GIS_level","Base",(poly,),"PECD2")
                    except:
                        pass
                    try:
                        add_relationship(db_map,"technology_type__region",("solar-PV",poly))
                        add_parameter_value(db_map,"technology_type__region","greenfield_potentials","Base",("solar-PV",poly),round(float(potential_wind_on.at[poly])*1e3,1))
                    except:
                        pass
                    
                    weight = {"type":"map","index_type":"str","index_name":"to_level","data":{"PECD1":round(potential_solar_PV.at[poly]/poparent_solar_PV.at[poly[:2]],3)}}
                    existing = round(float(share[tech]*existing_solar_PV.at[poly]*1e3),1)
                    profile = {"type":"time_series","data": dict(zip(CY_index["iso"],availability[tech].loc[CY_index["standard"],poly].round(3).tolist()))}

                    add_relationship(db_map,"technology__region",(tech,poly))
                    add_parameter_value(db_map,"technology__region","units_existing","Base",(tech,poly),existing)              
                    add_parameter_value(db_map,"technology__region","weight_profile_upper_limit","Base",(tech,poly),weight)    
                    add_parameter_value(db_map,"technology__region","profile_upper_limit","Base",(tech,poly),profile)
        print("Solar-PV")

        ## OFFSHORE TECHNOLOGY
        ### OFFSHORE EXISTING
        for poly in existing_wind_off.index:
            tech = "wind-off-existing"
            if existing_wind_off.round(2).at[poly] > 0 and poly in availability[tech].columns:
                try:
                    add_entity(db_map,"region",poly)
                    add_parameter_value(db_map,"region","type","Base",(poly,),"offshore")
                    add_parameter_value(db_map,"region","GIS_level","Base",(poly,),"OFF3")
                except:
                    pass
                try:
                    add_relationship(db_map,"technology_type__region",("wind-off",poly))
                    add_parameter_value(db_map,"technology_type__region","greenfield_potentials","Base",("wind-off",poly),round(float(potential_wind_off.at[poly])*1e3,1))
                except:
                    pass

                weight = {"type":"map","index_type":"str","index_name":"to_level","data":{"OFF2":round(existing_wind_off.at[poly]/exparent_wind_off.at[poly[:4]],3)}}
                existing = round(float(existing_wind_off.at[poly]*1e3),1)
                profile = {"type":"time_series","data": dict(zip(CY_index["iso"],availability[tech].loc[CY_index["standard"],poly].round(3).tolist()))}
                
                add_relationship(db_map,"technology__region",(tech,poly))
                add_parameter_value(db_map,"technology__region","units_existing","Base",(tech,poly),existing)              
                add_parameter_value(db_map,"technology__region","weight_profile_upper_limit","Base",(tech,poly),weight)    
                add_parameter_value(db_map,"technology__region","profile_upper_limit","Base",(tech,poly),profile)

        print("existing_wind_offshore")

        ### OFFSHORE FUTURE
        technologies = ["wind-off-FB-SP316-HH155","wind-off-FB-SP370-HH155"]
        for tech in technologies:
            for poly in availability[tech].columns:   
                if poly in potential_wind_off.index:
                    try:
                        add_entity(db_map,"region",poly)
                        add_parameter_value(db_map,"region","type","Base",(poly,),"offshore")
                        add_parameter_value(db_map,"region","GIS_level","Base",(poly,),"OFF3")
                    except:
                        pass
                    try:
                        add_relationship(db_map,"technology_type__region",("wind-off",poly))
                        add_parameter_value(db_map,"technology_type__region","greenfield_potentials","Base",("wind-off",poly),round(float(potential_wind_off.at[poly])*1e3,1))
                    except:
                        pass

                    weight = {"type":"map","index_type":"str","index_name":"to_level","data":{"OFF2":round(potential_wind_off.at[poly]/poparent_wind_off.at[poly[:4]],3)}}
                    existing = 0.0
                    profile = {"type":"time_series","data": dict(zip(CY_index["iso"],availability[tech].loc[CY_index["standard"],poly].round(3).tolist()))}
                    
                    add_relationship(db_map,"technology__region",(tech,poly))
                    add_parameter_value(db_map,"technology__region","units_existing","Base",(tech,poly),existing)              
                    add_parameter_value(db_map,"technology__region","weight_profile_upper_limit","Base",(tech,poly),weight)    
                    add_parameter_value(db_map,"technology__region","profile_upper_limit","Base",(tech,poly),profile)
        print("wind_off_future")

        db_map.commit_session("entities added")
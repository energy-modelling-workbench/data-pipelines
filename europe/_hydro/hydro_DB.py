import spinedb_api as api
from spinedb_api import DatabaseMapping
import sys
import json
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
import yaml
import pandas as pd
import os

def add_entity(db_map : DatabaseMapping, class_name : str, element_names : tuple) -> None:
    _, error = db_map.add_entity_item(entity_byname=element_names, entity_class_name=class_name)
    if error is not None:
        raise RuntimeError(error)

def add_parameter_value(db_map : DatabaseMapping,class_name : str,parameter : str,alternative : str,elements : tuple,value : any) -> None:
    db_value, value_type = api.to_database(value)
    _, error = db_map.add_parameter_value_item(entity_class_name=class_name,entity_byname=elements,parameter_definition_name=parameter,alternative_name=alternative,value=db_value,type=value_type)
    if error:
        raise RuntimeError(error)

def add_alternative(db_map : DatabaseMapping,name_alternative : str) -> None:
    _, error = db_map.add_alternative_item(name=name_alternative)
    if error is not None:
        raise RuntimeError(error)
    

def process_parameters(target_db, sheet):

    add_entity(target_db, "commodity", ("elec",))
    entity_name = "technology"
    entity_byname = ("hydro-turbine",)
    add_entity(target_db, entity_name, entity_byname)
    entity_name = "reservoir"
    entity_byname = ("reservoir",)
    add_entity(target_db, entity_name, entity_byname)
    for country in sheet.index:
        param_source = ["initial capacity (MWh)","maximum capacity (MWh)","minimum capacity  (MWh)","maximum discharge  (MWh)","minimum discharge  (MWh)","maximum ramping in 1 hour(MWh)","maximum ramping in 4 hours(MWh)"]
        param_target = ["initial_capacity","maximum_capacity","minimum_capacity","maximum_discharge","minimum_discharge","maximum_ramp","maximum_ramp_4"]
        params =dict(zip(param_target,param_source))
        add_entity(target_db, "region", (country,))
        
        entity_name = "technology__commodity__region"
        entity_byname = ("hydro-turbine","elec",country)
        add_entity(target_db, entity_name, entity_byname)
        for parameter in ["maximum_ramp","maximum_ramp_4"]:
            add_parameter_value(target_db, entity_name, parameter, "Base", entity_byname, float(sheet.at[country,params[parameter]]))
        
        
        entity_name = "reservoir__region"
        entity_byname = ("reservoir",country)
        add_entity(target_db, entity_name, entity_byname)
        for parameter in ["initial_capacity","minimum_capacity","maximum_capacity"]:
            add_parameter_value(target_db, entity_name, parameter, "Base", entity_byname, float(sheet.at[country,params[parameter]]))
        
        entity_name = "reservoir__technology__region"
        entity_byname = ("reservoir","hydro-turbine",country)
        add_entity(target_db, entity_name, entity_byname)
        for parameter in ["minimum_discharge","maximum_discharge"]:
            add_parameter_value(target_db, entity_name, parameter, "Base", entity_byname, float(sheet.at[country,params[parameter]]))
        
        entity_name = "reservoir__technology__commodity__region"
        entity_byname = ("reservoir","hydro-turbine","elec",country)
        add_entity(target_db, entity_name, entity_byname)
        eff1 = float(sheet.at[country,"efficiency 1"])
        eff2 = float(sheet.at[country,"efficiency 2"])
        dis1 = float(sheet.at[country,"Discharge segment 1  (MWh)"])
        dis2 = float(sheet.at[country,"Discharge segment 2  (MWh)"])
        efficiency = (eff1*dis1+eff2*dis2)/(dis1+dis2)
        add_parameter_value(target_db, entity_name, "efficiency", "Base", entity_byname, efficiency)
        efficiency_map = {"type": "map", "index_type": "str", "index_name": "MWh", "data": dict(zip([dis1,dis1+dis2],[eff1,eff2]))}
        add_parameter_value(target_db, entity_name, "efficiency_curve", "Base", entity_byname, efficiency_map)

def ror_parameters(target_db, sheet):

    entity_name = "technology"
    entity_byname = ("RoR",)
    add_entity(target_db, entity_name, entity_byname)

    time_index = [pd.Timestamp(i).isoformat() for i in sheet.index]
    for column in sheet.columns:

        country = column[:2]
        try:
            add_entity(target_db, "region", (country,))
        except:
            pass
        entity_name = "technology__commodity__region"
        entity_byname = ("RoR","elec",country)
        add_entity(target_db, entity_name, entity_byname)

        param_map = {"type": "time_series", "data": dict(zip(time_index,sheet[column].values))}
        add_parameter_value(target_db, entity_name, "profile_fix", "Base", entity_byname, param_map)


def inflow_parameters(target_db, sheet):

    time_index = [pd.Timestamp(i).isoformat() for i in sheet.index]
    for column in sheet.columns:

        country = column[:2]
        entity_name = "reservoir__region"
        entity_byname = ("reservoir",country)

        param_map = {"type": "time_series", "data": dict(zip(time_index,sheet[column].values))}
        add_parameter_value(target_db, entity_name, "inflow", "Base", entity_byname, param_map)


def main():

    # Spine Inputs
    url_db_out = sys.argv[1]
    static_params = pd.read_excel(sys.argv[2],sheet_name="WP2.3 hydro",index_col=0)
    ror_params = pd.read_csv(sys.argv[3],index_col=0,sep=";")
    inflow_params = pd.read_csv(sys.argv[4],index_col=0,sep=";")

    print("############### Filling the output DB ###############")
    with DatabaseMapping(url_db_out) as target_db:

        ## Empty the database
        target_db.purge_items('entity')
        target_db.purge_items('parameter_value')
        target_db.purge_items('alternative')
        target_db.purge_items('scenario')
        target_db.refresh_session()

        for alternative_name in ["Base"]:
            add_alternative(target_db,alternative_name)

        process_parameters(target_db,static_params)
        target_db.commit_session("static_params_added")
        print("static_params_added")
        
        ror_parameters(target_db,ror_params)
        target_db.commit_session("ror_params_added")
        print("ror_params_added")

        inflow_parameters(target_db,inflow_params)
        target_db.commit_session("inflow_params_added")
        print("inflow_params_added")


if __name__ == "__main__":
    main()
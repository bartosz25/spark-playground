from pyspark.sql import SparkSession
from pyspark.sql.types import Row

def setup_data(spark:SparkSession):
    spark.createDataFrame([
        Row(customer_id="C101", item_id="ITM-001", manufacturer="Acme Corp",
            date="2026-01-17", label="Temperature", value=98.6,
            units="Fahrenheit", units_type_entity="Sensor"),
        Row(customer_id="C102", item_id="ITM-002", manufacturer="Global Tech",
            date="2026-01-18", label="Weight", value=15.5,
            units="Kilograms", units_type_entity="Scale"),
        Row(customer_id="C103", item_id="LAPTOP_X1", manufacturer="TechNova",
            date="2026-01-19", label="Pressure", value=101.3,
            units="kPa", units_type_entity="Gauge")
    ]).coalesce(1).write.csv("../data/poland_orders", header=True, mode="overwrite")


    spark.createDataFrame([
        Row(client="FR_9921", produit="LAPTOP_X1", manufacturer="TechNova",
            date="2026-02-10", label="CPU_Temp", value=45.5,
            units="Celsius", units_type_entity="Hardware_Monitor"),
        Row(client="US_4402", produit="FLOW_VALVE_B", manufacturer="HydroLogic",
            date="2026-02-11", label="Flow_Rate", value=12.8,
            units="Liters/Min", units_type_entity="Flow_Meter"),
        Row(client="DE_1150", produit="EV_BATTERY_G3", manufacturer="VoltEdge",
            date="2026-02-12", label="Voltage", value=395.2,
            units="Volts", units_type_entity="Battery_Management_System")
    ]).coalesce(1).write.csv("../data/france_orders", header=True, mode="overwrite")



    spark.createDataFrame([
        Row(abnehmer="DACH_882", item_id="LAB_REF_01", manufacturer="BioStandard",
            date="2026-03-01", label="Purity_Level", value=99.98,
            units="Percentage", units_type_entity="Chemical_Analysis"),
        Row(abnehmer="EU_LOG_55", item_id="EV_BATTERY_G3", manufacturer="PackSafe",
            date="2026-03-02", label="Internal_Humidity", value=42.1,
            units="RH", units_type_entity="Environmental_Sensor"),
        Row(abnehmer="MUNICH_IND", item_id="EV_BATTERY_G3", manufacturer="Kuka_Dynamics",
            date="2026-03-03", label="Torque_Load", value=250.5,
            units="Newton-meters", units_type_entity="Mechanical_Actuator")
    ]).coalesce(1).write.csv("../data/germany_orders", header=True, mode="overwrite")

    spark.createDataFrame([
        Row(item_id='ITM-001', manufacturer='Acme Corp', model='GPU_accelerated', is_imported=False),
        Row(item_id='ITM-002', manufacturer='Global Tech', model='RAM_optimized', is_imported=False),
        Row(item_id="LAPTOP_X1", manufacturer="TechNova", model='GPU_accelerated', is_imported=True),
        Row(item_id='EV_BATTERY_G3', manufacturer='VoltEdge', model='universal', is_imported=False),
        Row(item_id='EV_BATTERY_G3', manufacturer='PackSafe', model='PHEV_adapted', is_imported=False),
        Row(item_id='EV_BATTERY_G3', manufacturer='Kuka_Dynamics', model='worldwide', is_imported=True),
    ]).write.format('delta').mode('overwrite').saveAsTable('product_referential')
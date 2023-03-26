from datetime import timedelta

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.sensors.external_task_sensor import ExternalTaskSensor

args = {
    'owner': 'atommych',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    dag_id='CIA_SYS_BUS_ENH_PROJ',
    default_args=args,
    catchup=False,
    schedule_interval='0 23 * * 1-5',
    dagrun_timeout=timedelta(minutes=240),
    tags=['CIA', 'BUS', 'ENH', 'PROJ']
)

CIA_SYS_BUS_ING_PROJ = ExternalTaskSensor(task_id="CIA_SYS_BUS_ING_PROJ",
                             external_dag_id='CIA_SYS_BUS_ING_PROJ',
                             external_task_id='END',
                             timeout=120*60,
                             mode='reschedule',
                             dag=dag)

KINIT=BashOperator(
    task_id='KINIT',
    bash_command='echo "kinit -kt /users/atommych/kt/atommych.keytab atommych@domain"',
    dag=dag,
)

CIA_D_ING_ATH_CONT_BUS = DummyOperator(
    task_id='CIA_D_ING_ATH_CONT_BUS',
    dag=dag,
)

CIA_D_ING_ATH_VEHICU_BUS = DummyOperator(
    task_id='CIA_D_ING_ATH_VEHICU_BUS',
    dag=dag,
)

CIA_D_ING_OSC_LEAD_BUS = DummyOperator(
    task_id='CIA_D_ING_OSC_LEAD_BUS',
    dag=dag,
)

CIA_D_ING_OSC_UTI_BUS = DummyOperator(
    task_id='CIA_D_ING_OSC_UTI_BUS',
    dag=dag,
)

CIA_M_ENH_HIERARCHY_BUS = DummyOperator(
    task_id='CIA_M_ENH_HIERARCHY_BUS',
    dag=dag,
)

CIA_D_ING_ATH_CLIENT_BUS = DummyOperator(
    task_id='CIA_D_ING_ATH_CLIENT_BUS',
    dag=dag,
)

CIA_D_ING_ATH_CTRAT_HISTO_BUS = DummyOperator(
    task_id='CIA_D_ING_ATH_CTRAT_HISTO_BUS',
    dag=dag,
)

CIA_D_ING_ATH_GENRAL_BUS = DummyOperator(
    task_id='CIA_D_ING_ATH_GENRAL_BUS',
    dag=dag,
)

CIA_D_ING_ATH_APPORT_BUS = DummyOperator(
    task_id='CIA_D_ING_ATH_APPORT_BUS',
    dag=dag,
)
CIA_D_ING_OSC_PROJET_ASSOCIE_BUS = DummyOperator(
    task_id='CIA_D_ING_OSC_PROJET_ASSOCIE_BUS',
    dag=dag,
)

CIA_D_ING_OSC_TIERS_BUS = DummyOperator(
    task_id='CIA_D_ING_OSC_TIERS_BUS',
    dag=dag,
)

START = DummyOperator(
    task_id='START',
    dag=dag,
)

CIA_SYS_BUS_ING_PROJ >> START
KINIT >> START
CIA_D_ING_ATH_CONT_BUS >> START
CIA_D_ING_ATH_VEHICU_BUS >> START
CIA_D_ING_ATH_CONT_BUS >> START
CIA_D_ING_OSC_LEAD_BUS >> START
CIA_D_ING_OSC_UTI_BUS >> START
CIA_M_ENH_HIERARCHY_BUS >> START
CIA_D_ING_ATH_CLIENT_BUS >> START
CIA_D_ING_ATH_CTRAT_HISTO_BUS >> START
CIA_D_ING_ATH_GENRAL_BUS >> START
CIA_D_ING_ATH_APPORT_BUS >> START
CIA_D_ING_OSC_PROJET_ASSOCIE_BUS >> START
CIA_D_ING_OSC_TIERS_BUS >> START

END = DummyOperator(
    task_id='END',
    dag=dag,
)

CIA_D_ENH_PROJ_DW_DIM_ATT_CO_LE = BashOperator(
    task_id= 'CIA_D_ENH_PROJ_DW_DIM_ATT_CO_LE' ,
    bash_command= 'echo {{ var.value.base_path }}/proj/{{ var.value.engine_script }} {{ var.value.base_path }}/proj/{{ var.value.enh_path }}/enhanced_cia_dim_att_co_le.yaml {{ var.value.odate }}',
    dag=dag,
)
for dependency in [START]:
    dependency >> CIA_D_ENH_PROJ_DW_DIM_ATT_CO_LE

CIA_D_ENH_PROJ_DW_DIM_CANAL = BashOperator(
    task_id= 'CIA_D_ENH_PROJ_DW_DIM_CANAL' ,
    bash_command= 'echo {{ var.value.base_path }}/proj/{{ var.value.engine_script }} {{ var.value.base_path }}/proj/{{ var.value.enh_path }}/enhanced_cia_dim_canal.yaml {{ var.value.odate }}',
    dag=dag,
)
for dependency in [START]:
    dependency >> CIA_D_ENH_PROJ_DW_DIM_CANAL

CIA_D_ENH_PROJ_DW_DIM_CAR_VEH = BashOperator(
    task_id= 'CIA_D_ENH_PROJ_DW_DIM_CAR_VEH' ,
    bash_command= 'echo {{ var.value.base_path }}/proj/{{ var.value.engine_script }} {{ var.value.base_path }}/proj/{{ var.value.enh_path }}/enhanced_cia_dim_car_veh.yaml {{ var.value.odate }}',
    dag=dag,
)
for dependency in [START]:
    dependency >> CIA_D_ENH_PROJ_DW_DIM_CAR_VEH

CIA_D_ENH_PROJ_DW_DIM_CLIENT = BashOperator(
    task_id= 'CIA_D_ENH_PROJ_DW_DIM_CLIENT' ,
    bash_command= 'echo {{ var.value.base_path }}/proj/{{ var.value.engine_script }} {{ var.value.base_path }}/proj/{{ var.value.enh_path }}/enhanced_cia_dim_client.yaml {{ var.value.odate }}',
    dag=dag,
)
for dependency in [START]:
    dependency >> CIA_D_ENH_PROJ_DW_DIM_CLIENT

CIA_D_ENH_PROJ_DW_DIM_COM_LE = BashOperator(
    task_id= 'CIA_D_ENH_PROJ_DW_DIM_COM_LE' ,
    bash_command= 'echo {{ var.value.base_path }}/proj/{{ var.value.engine_script }} {{ var.value.base_path }}/proj/{{ var.value.enh_path }}/enhanced_cia_dim_com_le.yaml {{ var.value.odate }}',
    dag=dag,
)
for dependency in [ START ]:
    dependency >> CIA_D_ENH_PROJ_DW_DIM_COM_LE

CIA_D_ENH_PROJ_DW_DIM_HIERARCHY = BashOperator(
    task_id= 'CIA_D_ENH_PROJ_DW_DIM_HIERARCHY' ,
    bash_command= 'echo {{ var.value.base_path }}/proj/{{ var.value.engine_script }} {{ var.value.base_path }}/proj/{{ var.value.enh_path }}/enhanced_cia_dim_hierarchy.yaml {{ var.value.odate }}',
    dag=dag,
)
for dependency in [START]:
    dependency >> CIA_D_ENH_PROJ_DW_DIM_HIERARCHY

CIA_D_ENH_PROJ_DW_DIM_ENERGIE = BashOperator(
    task_id= 'CIA_D_ENH_PROJ_DW_DIM_ENERGIE' ,
    bash_command= 'echo {{ var.value.base_path }}/proj/{{ var.value.engine_script }} {{ var.value.base_path }}/proj/{{ var.value.enh_path }}/enhanced_cia_dim_energie.yaml {{ var.value.odate }}',
    dag=dag,
)
for dependency in [START]:
    dependency >> CIA_D_ENH_PROJ_DW_DIM_ENERGIE

CIA_D_ENH_PROJ_DW_DIM_ETA = BashOperator(
    task_id= 'CIA_D_ENH_PROJ_DW_DIM_ETA' ,
    bash_command= 'echo {{ var.value.base_path }}/proj/{{ var.value.engine_script }} {{ var.value.base_path }}/proj/{{ var.value.enh_path }}/enhanced_cia_dim_eta.yaml {{ var.value.odate }}',
    dag=dag,
)
for dependency in [START]:
    dependency >> CIA_D_ENH_PROJ_DW_DIM_ETA

CIA_D_ENH_PROJ_DW_DIM_LEAD_STAT = BashOperator(
    task_id= 'CIA_D_ENH_PROJ_DW_DIM_LEAD_STAT' ,
    bash_command= 'echo {{ var.value.base_path }}/proj/{{ var.value.engine_script }} {{ var.value.base_path }}/proj/{{ var.value.enh_path }}/enhanced_cia_dim_lead_stat.yaml {{ var.value.odate }}',
    dag=dag,
)
for dependency in [START]:
    dependency >> CIA_D_ENH_PROJ_DW_DIM_LEAD_STAT

CIA_D_ENH_PROJ_DW_DIM_MARC = BashOperator(
    task_id= 'CIA_D_ENH_PROJ_DW_DIM_MARC' ,
    bash_command= 'echo {{ var.value.base_path }}/proj/{{ var.value.engine_script }} {{ var.value.base_path }}/proj/{{ var.value.enh_path }}/enhanced_cia_dim_marc.yaml {{ var.value.odate }}',
    dag=dag,
)
for dependency in [START]:
    dependency >> CIA_D_ENH_PROJ_DW_DIM_MARC

CIA_D_ENH_PROJ_DW_DIM_NAF = BashOperator(
    task_id= 'CIA_D_ENH_PROJ_DW_DIM_NAF' ,
    bash_command= "echo {{ var.value.base_path }}/proj/{{ var.value.engine_script }} {{ var.value.base_path }}/proj/{{ var.value.enh_path }}/enhanced_cia_dim_naf.yaml {{ var.value.odate }}",
    dag=dag,
)
for dependency in [START]:
    dependency >> CIA_D_ENH_PROJ_DW_DIM_NAF

CIA_D_ENH_PROJ_DW_DIM_REGION = BashOperator(
    task_id= 'CIA_D_ENH_PROJ_DW_DIM_REGION' ,
    bash_command= 'echo {{ var.value.base_path }}/proj/{{ var.value.engine_script }} {{ var.value.base_path }}/proj/{{ var.value.enh_path }}/enhanced_cia_dim_region.yaml {{ var.value.odate }}',
    dag=dag,
)
for dependency in [START]:
    dependency >> CIA_D_ENH_PROJ_DW_DIM_REGION

CIA_D_ENH_PROJ_DW_DIM_RES = BashOperator(
    task_id= 'CIA_D_ENH_PROJ_DW_DIM_RES' ,
    bash_command= 'echo {{ var.value.base_path }}/proj/{{ var.value.engine_script }} {{ var.value.base_path }}/proj/{{ var.value.enh_path }}/enhanced_cia_dim_res.yaml {{ var.value.odate }}',
    dag=dag,
)
for dependency in [START]:
    dependency >> CIA_D_ENH_PROJ_DW_DIM_RES

CIA_D_ENH_PROJ_DW_DIM_SEC = BashOperator(
    task_id= 'CIA_D_ENH_PROJ_DW_DIM_SEC' ,
    bash_command= 'echo {{ var.value.base_path }}/proj/{{ var.value.engine_script }} {{ var.value.base_path }}/proj/{{ var.value.enh_path }}/enhanced_cia_dim_sec.yaml {{ var.value.odate }}',
    dag=dag,
)
for dependency in [START]:
    dependency >> CIA_D_ENH_PROJ_DW_DIM_SEC

CIA_D_ENH_PROJ_DW_DIM_TYPE_CO = BashOperator(
    task_id= 'CIA_D_ENH_PROJ_DW_DIM_TYPE_CO' ,
    bash_command= 'echo {{ var.value.base_path }}/proj/{{ var.value.engine_script }} {{ var.value.base_path }}/proj/{{ var.value.enh_path }}/enhanced_cia_dim_type_co.yaml {{ var.value.odate }}',
    dag=dag,
)
for dependency in [START]:
    dependency >> CIA_D_ENH_PROJ_DW_DIM_TYPE_CO

CIA_D_ENH_PROJ_DW_DIM_TYPE_FISCAL = BashOperator(
    task_id= 'CIA_D_ENH_PROJ_DW_DIM_TYPE_FISCAL' ,
    bash_command= 'echo {{ var.value.base_path }}/proj/{{ var.value.engine_script }} {{ var.value.base_path }}/proj/{{ var.value.enh_path }}/enhanced_cia_dim_type_fiscal.yaml {{ var.value.odate }}',
    dag=dag,
)
for dependency in [START]:
    dependency >> CIA_D_ENH_PROJ_DW_DIM_TYPE_FISCAL

CIA_D_ENH_PROJ_DW_DIM_VEN_RES = BashOperator(
    task_id= 'CIA_D_ENH_PROJ_DW_DIM_VEN_RES' ,
    bash_command= 'echo {{ var.value.base_path }}/proj/{{ var.value.engine_script }} {{ var.value.base_path }}/proj/{{ var.value.enh_path }}/enhanced_cia_dim_ven_res.yaml {{ var.value.odate }}',
    dag=dag,
)
for dependency in [START]:
    dependency >> CIA_D_ENH_PROJ_DW_DIM_VEN_RES

CIA_D_ENH_PROJ_DW_FCT_CONTT = BashOperator(
    task_id= 'CIA_D_ENH_PROJ_DW_FCT_CONTT' ,
    bash_command= 'echo {{ var.value.base_path }}/proj/{{ var.value.engine_script }} {{ var.value.base_path }}/proj/{{ var.value.enh_path }}/enhanced_cia_fct_contt.yaml {{ var.value.odate }}',
    dag=dag,
)
for dependency in [START, CIA_D_ENH_PROJ_DW_DIM_ATT_CO_LE, CIA_D_ENH_PROJ_DW_DIM_CANAL,
                   CIA_D_ENH_PROJ_DW_DIM_CAR_VEH, CIA_D_ENH_PROJ_DW_DIM_COM_LE,
                   CIA_D_ENH_PROJ_DW_DIM_ENERGIE, CIA_D_ENH_PROJ_DW_DIM_ETA, CIA_D_ENH_PROJ_DW_DIM_MARC,
                   CIA_D_ENH_PROJ_DW_DIM_NAF, CIA_D_ENH_PROJ_DW_DIM_REGION, CIA_D_ENH_PROJ_DW_DIM_RES,
                   CIA_D_ENH_PROJ_DW_DIM_TYPE_CO, CIA_D_ENH_PROJ_DW_DIM_TYPE_FISCAL]:
    dependency >> CIA_D_ENH_PROJ_DW_FCT_CONTT >> END

CIA_D_ENH_PROJ_DW_FCT_OPP_COME = BashOperator(
    task_id= 'CIA_D_ENH_PROJ_DW_FCT_OPP_COME' ,
    bash_command= 'echo {{ var.value.base_path }}/proj/{{ var.value.engine_script }} {{ var.value.base_path }}/proj/{{ var.value.enh_path }}/enhanced_cia_fct_opp_come.yaml {{ var.value.odate }}',
    dag=dag,
)
for dependency in [START, CIA_D_ENH_PROJ_DW_DIM_CANAL, CIA_D_ENH_PROJ_DW_DIM_CLIENT,
                   CIA_D_ENH_PROJ_DW_DIM_COM_LE, CIA_D_ENH_PROJ_DW_DIM_ETA,
                   CIA_D_ENH_PROJ_DW_DIM_LEAD_STAT, CIA_D_ENH_PROJ_DW_DIM_MARC, CIA_D_ENH_PROJ_DW_DIM_NAF,
                   CIA_D_ENH_PROJ_DW_DIM_REGION, CIA_D_ENH_PROJ_DW_DIM_RES]:
    dependency >> CIA_D_ENH_PROJ_DW_FCT_OPP_COME >> END

if __name__=="__main__":
    dag.cli()
from datetime import timedelta

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'atommych',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    dag_id='CIA_SYS_BUS_ING_PROJ',
    default_args=args,
    catchup=False,
    schedule_interval='0 23 * * 1-5',
    dagrun_timeout=timedelta(minutes=120),
    tags=['CIA', 'BUS', 'ING', 'PROJ'],
)

START = DummyOperator(
    task_id='START',
    dag=dag,
)

END = DummyOperator(
    task_id='END',
    dag=dag,
)

KINIT=BashOperator(
    task_id='KINIT',
    bash_command='echo "kinit -kt /users/atommych/kt/atommych.keytab atommych@domain"',
    dag=dag,
)

CIA_D_ING_SYS_RAW_CONTROLE_BUS=BashOperator( 
    task_id='CIA_D_ING_SYS_RAW_CONTROLE_BUS', 
    bash_command="echo '{{ var.value.base_path }}/sys/{{ var.value.engine_script }} {{ var.value.base_path }}/sys/{{ var.value.ing_path }}/ingest-cia-generique-controle.yaml {{ var.value.odate }}'",
    dag=dag,
)
for dependency in [START]:
    dependency >> CIA_D_ING_SYS_RAW_CONTROLE_BUS

CIA_D_ING_SYS_RAW_DATA_BUS=BashOperator(
    task_id='CIA_D_ING_SYS_RAW_DATA_BUS',
    bash_command="echo 'hdfs dfs -put {{ var.value.base_path }}/sys/inputs/ingest-cia-generique-data-{{ var.value.odate }}.csv /cia_srv/bus/sys/raw/data/{{ var.value.odate }}/.'",
    dag=dag,
)
for dependency in [START]:
    dependency >> CIA_D_ING_SYS_RAW_DATA_BUS


CIA_D_ING_SYS_RAW_OBJECT_DEL_BUS=BashOperator(
    task_id='CIA_D_ING_SYS_RAW_OBJECT_DEL_BUS',
    bash_command="echo '{{ var.value.base_path }}/sys/{{ var.value.engine_script }} {{ var.value.base_path }}/sys/{{ var.value.ing_path }}/ingest-cia-sys-objectif_del.yaml {{ var.value.odate }}'",
    dag=dag,
)
for dependency in [START]:
    dependency >> CIA_D_ING_SYS_RAW_OBJECT_DEL_BUS

generic_tables_from_csv = [{"CIA_D_ING_SYS_RAW_APPO_BUS": "ingest-cia-appo.yaml"},{"CIA_D_ING_SYS_RAW_MARC_BUS": "ingest-cia-marc.yaml"},{"CIA_D_ING_SYS_RAW_NAF_BUS": "ingest-cia-naf.yaml"},{"CIA_D_ING_SYS_RAW_CANAL_BUS": "ingest-cia-sys-canal.yaml"},{"CIA_D_ING_SYS_RAW_CAT_VEH_BUS": "ingest-cia-sys-cat_veh.yaml"},{"CIA_D_ING_SYS_RAW_ENERGIE_BUS": "ingest-cia-sys-energie.yaml"},{"CIA_D_ING_SYS_RAW_GENRE_VEH_BUS": "ingest-cia-sys-genre_veh.yaml"},{"CIA_D_ING_SYS_RAW_LEAD_STAT_BUS": "ingest-cia-sys-lead_STAT.yaml"},{"CIA_D_ING_SYS_RAW_LOI_OM_BUS": "ingest-cia-sys-loi_om.yaml"},{"CIA_D_ING_SYS_RAW_OBJ_COEF_BUS": "ingest-cia-sys-obj_coef.yaml"},{"CIA_D_ING_SYS_RAW_REG_APP_BUS": "ingest-cia-sys-reg_app.yaml"},{"CIA_D_ING_SYS_RAW_REG_DEL_BUS": "ingest-cia-sys-reg_del.yaml"},{"CIA_D_ING_SYS_RAW_REGION_BUS": "ingest-cia-sys-region.yaml"},{"CIA_D_ING_SYS_RAW_TYPE_CMD_BUS": "ingest-cia-sys-type_cmd.yaml"}]
for d in generic_tables_from_csv:
    for key, value in d.items():
        key = BashOperator(
            task_id=key,
            bash_command="echo '{} {}/{} {}'".format("{{ var.value.base_path }}/sys/{{ var.value.engine_script }}", "{{ var.value.base_path }}/sys/{{ var.value.ing_path }}", value, "{{ var.value.odate }}"),
            dag=dag,
        )
        CIA_D_ING_SYS_RAW_DATA_BUS >> key
        key >> END


if __name__=="__main__":
    dag.cli()
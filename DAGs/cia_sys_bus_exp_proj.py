from datetime import timedelta

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.sensors.external_task_sensor import ExternalTaskSensor

args = {
    'owner': 'atommych',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    dag_id='CIA_SYS_BUS_EXP_PROJ',
    default_args=args,
    catchup=False,
    schedule_interval='0 23 * * 1-5',
    dagrun_timeout=timedelta(minutes=300),
    tags=['CIA', 'BUS', 'EXP', 'PROJ']
)

CIA_D_ENH_DW_PROJ = ExternalTaskSensor(task_id="CIA_D_ENH_DW_PROJ",
                             external_dag_id='CIA_SYS_BUS_ENH_PROJ',
                             external_task_id='END',
                             timeout=240*60,
                             mode='reschedule',
                             dag=dag)

KINIT=BashOperator(
    task_id='KINIT',
    bash_command='echo "kinit -kt /users/atommych/kt/atommych.keytab atommych@domain"',
    dag=dag,
)

CIA_D_EXP_PROJ_SMT_ATT_CO_LE = BashOperator(
    task_id='CIA_D_EXP_PROJ_SMT_ATT_CO_LE' ,
    bash_command= 'echo {{  var.value.base_path }}/proj/{{ var.value.engine_script }} {{ var.value.base_path }}/proj/{{ var.value.exp_path }}/exposition_CIA_dim_ATT_CO_LE.yaml {{ var.value.odate }}',
    dag=dag,
)
for dependency in [CIA_D_ENH_DW_PROJ]:
    dependency >> CIA_D_EXP_PROJ_SMT_ATT_CO_LE

CIA_D_EXP_PROJ_SMT_LEAD_STAT = BashOperator(
    task_id='CIA_D_EXP_PROJ_SMT_LEAD_STAT' ,
    bash_command= 'echo {{  var.value.base_path }}/proj/{{ var.value.engine_script }} {{ var.value.base_path }}/proj/{{ var.value.exp_path }}/exposition_CIA_dim_lead_STAT.yaml {{ var.value.odate }}',
    dag=dag,
)
for dependency in [CIA_D_ENH_DW_PROJ]:
    dependency >> CIA_D_EXP_PROJ_SMT_LEAD_STAT

CIA_D_EXP_PROJ_SMT_MARC = BashOperator(
    task_id='CIA_D_EXP_PROJ_SMT_MARC' ,
    bash_command= 'echo {{  var.value.base_path }}/proj/{{ var.value.engine_script }} {{ var.value.base_path }}/proj/{{ var.value.exp_path }}/exposition_CIA_dim_MARC.yaml {{ var.value.odate }}',
    dag=dag,
)
for dependency in [CIA_D_ENH_DW_PROJ]:
    dependency >> CIA_D_EXP_PROJ_SMT_MARC

CIA_D_EXP_PROJ_SMT_VEN_RES = BashOperator(
    task_id='CIA_D_EXP_PROJ_SMT_VEN_RES' ,
    bash_command= 'echo {{  var.value.base_path }}/proj/{{ var.value.engine_script }} {{ var.value.base_path }}/proj/{{ var.value.exp_path }}/exposition_CIA_dim_VEN_RES.yaml {{ var.value.odate }}',
    dag=dag,
)
for dependency in [CIA_D_ENH_DW_PROJ]:
    dependency >> CIA_D_EXP_PROJ_SMT_VEN_RES

CIA_D_EXP_PROJ_SMT_CONTT = BashOperator(
    task_id='CIA_D_EXP_PROJ_SMT_CONTT' ,
    bash_command= 'echo {{  var.value.base_path }}/proj/{{ var.value.engine_script }} {{ var.value.base_path }}/proj/{{ var.value.exp_path }}/exposition_CIA_fct_CONTt.yaml {{ var.value.odate }}',
    dag=dag,
)
for dependency in [CIA_D_ENH_DW_PROJ]:
    dependency >> CIA_D_EXP_PROJ_SMT_CONTT

CIA_D_EXP_PROJ_SMT_OPP_COME = BashOperator(
    task_id='CIA_D_EXP_PROJ_SMT_OPP_COME' ,
    bash_command= 'echo {{  var.value.base_path }}/proj/{{ var.value.engine_script }} {{ var.value.base_path }}/proj/{{ var.value.exp_path }}/exposition_CIA_fct_OPP_COMe.yaml {{ var.value.odate }}',
    dag=dag,
)
for dependency in [CIA_D_ENH_DW_PROJ]:
    dependency >> CIA_D_EXP_PROJ_SMT_OPP_COME



if __name__=="__main__":
    dag.cli()
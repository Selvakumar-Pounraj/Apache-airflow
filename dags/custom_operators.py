from airflow.models import BaseOperator # type: ignore
from airflow.utils.decorators import apply_defaults # type: ignore
from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore
 
class PostgresTableCountOperator(BaseOperator):
   
  template_fields = ["table_name", "postgres_conn_id"]
  @apply_defaults
  def __init__(self,
               table_name: str,
               postgres_conn_id: str = "postgres_default",
               *args, **kwargs) -> None:
    super().__init__(*args, **kwargs)
    self.table_name = table_name
    self.postgres_conn_id = postgres_conn_id
    self.count = None
 
  def execute(self, context) -> None:
    postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
    sql = f"SELECT COUNT(*) FROM {self.table_name}"
    self.count = postgres_hook.get_first(sql)[0]
    self.log.info(f"Count of rows in table {self.table_name}: {self.count}")
    return self.count
 
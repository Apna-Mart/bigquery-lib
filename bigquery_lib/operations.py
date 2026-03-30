import io
import json
import logging
from datetime import datetime, date, timedelta, timezone

from django.db.backends.base.operations import BaseDatabaseOperations
from google.cloud import bigquery
import django.apps
import django.db.models

logger = logging.getLogger(__name__)


class DatabaseOperations(BaseDatabaseOperations):
    explain_prefix = "EXPLAIN"
    cast_char_field_without_max_length = "STRING"
    compiler_module = "django.db.models.sql.compiler"

    def __init__(self, connection):
        super().__init__(connection)
        self.dataset = self.connection.settings_dict.get("NAME")
        self._table_name_cache = None

    def _get_table_name_mapping(self):
        if self._table_name_cache is None:
            mapping = {}
            for model in django.apps.apps.get_models():
                mapping[model._meta.db_table] = f"{self.dataset}.{model._meta.db_table}"
                for field in model._meta.get_fields():
                    if (
                            isinstance(field, django.db.models.ManyToManyField) and
                            field.remote_field.through and
                            field.remote_field.through._meta.auto_created
                    ):
                        m2m_model = field.remote_field.through
                        mapping[m2m_model._meta.db_table] = f"{self.dataset}.{m2m_model._meta.db_table}"
            self._table_name_cache = mapping
        return self._table_name_cache

    def quote_name(self, name):
        if "." not in name and self.dataset:
            name = self._get_table_name_mapping().get(name, name)
        return f"`{name}`"

    def date_extract_sql(self, lookup_type, sql, params):
        return f"EXTRACT({lookup_type.upper()} FROM {sql})", params

    def datetime_extract_sql(self, lookup_type, sql, params, tzname):
        return f"EXTRACT({lookup_type.upper()} FROM {sql})", params

    def time_extract_sql(self, lookup_type, sql, params):
        return self.date_extract_sql(lookup_type, sql, params)

    def date_trunc_sql(self, lookup_type, sql, params, tzname=None):
        return f"DATE_TRUNC({sql}, {lookup_type.upper()})", params

    def datetime_trunc_sql(self, lookup_type, sql, params, tzname=None):
        return f"TIMESTAMP_TRUNC({sql}, {lookup_type.upper()})", params

    def time_trunc_sql(self, lookup_type, sql, params, tzname=None):
        return f"TIME_TRUNC({sql}, {lookup_type.upper()})", params

    def datetime_cast_date_sql(self, sql, params, tzname):
        return f"DATE({sql})", params

    def datetime_cast_time_sql(self, sql, params, tzname):
        return f"TIME({sql})", params

    def deferrable_sql(self):
        return ""

    def distinct_sql(self, fields, params):
        if fields:
            raise NotImplementedError("DISTINCT ON fields not supported by BigQuery")
        return ["DISTINCT"], []

    def unification_cast_sql(self, output_field):
        return "%s"

    def fetch_returned_insert_columns(self, cursor, returning_params):
        return cursor.fetchone()

    def field_cast_sql(self, db_type, internal_type):
        return "%s"

    def force_no_ordering(self):
        return []

    def limit_offset_sql(self, low_mark, high_mark):
        limit, offset = self._get_limit_offset_params(low_mark, high_mark)
        return " ".join(
            sql for sql in (
                f"LIMIT {limit}" if limit else None,
                f"OFFSET {offset}" if offset else None,
            ) if sql
        )

    def last_executed_query(self, cursor, sql, params):
        from django.utils.encoding import force_str
        def to_string(s):
            return force_str(s, strings_only=True, errors="replace")
        if isinstance(params, (list, tuple)):
            u_params = tuple(to_string(val) for val in params)
        elif params is None:
            u_params = ()
        else:
            u_params = {to_string(k): to_string(v) for k, v in params.items()}
        return "QUERY = %r - PARAMS = %r" % (sql, u_params)

    def no_limit_value(self):
        return None

    def pk_default_value(self):
        return "NULL"

    def adapt_datefield_value(self, value):
        return f"{value.isoformat()}" if value else None

    def adapt_datetimefield_value(self, value):
        return f"{value.isoformat()}" if value else None

    def adapt_timefield_value(self, value):
        return f"{value.isoformat()}" if value else None

    def adapt_decimalfield_value(self, value, max_digits=None, decimal_places=None):
        return str(value)

    def adapt_ipaddressfield_value(self, value):
        return value or None

    def year_lookup_bounds_for_date_field(self, value, iso_year=False):
        if iso_year:
            first = date.fromisocalendar(value, 1, 1)
            second = date.fromisocalendar(value + 1, 1, 1) - timedelta(days=1)
        else:
            first = date(value, 1, 1)
            second = date(value, 12, 31)
        return [str(first), str(second)]

    def year_lookup_bounds_for_datetime_field(self, value, iso_year=False):
        if iso_year:
            first = datetime.fromisocalendar(value, 1, 1)
            second = datetime.fromisocalendar(value + 1, 1, 1) - timedelta(microseconds=1)
        else:
            first = datetime(value, 1, 1)
            second = datetime(value, 12, 31, 23, 59, 59, 999999)
        return [str(first), str(second)]

    def get_db_converters(self, expression):
        return []

    def combine_expression(self, connector, sub_expressions):
        return f" {connector} ".join(sub_expressions)

    def lookup_cast(self, lookup_type, internal_type=None):
        if lookup_type in ("iexact", "icontains", "istartswith", "iendswith"):
            return "UPPER(%s)"
        return "%s"

    def pattern_ops(self):
        return {
            "contains": "ILIKE CONCAT('%%', %s, '%%')",
            "icontains": "ILIKE CONCAT('%%', %s, '%%')",
            "startswith": "ILIKE CONCAT(%s, '%%')",
            "istartswith": "ILIKE CONCAT(%s, '%%')",
            "endswith": "ILIKE CONCAT('%%', %s)",
            "iendswith": "ILIKE CONCAT('%%', %s)",
        }

    def prep_for_like_query(self, x):
        return str(x).replace("\\", "\\\\").replace("%", r"\%")

    def regex_lookup(self, lookup_type):
        if lookup_type == "regex":
            return "REGEXP_CONTAINS(%s, %s)"
        elif lookup_type == "iregex":
            return "REGEXP_CONTAINS(LOWER(%s), LOWER(%s))"
        raise NotImplementedError(f"Unsupported regex type: {lookup_type}")

    def explain_query_prefix(self, format=None, **options):
        return self.explain_prefix

    # =========================================================================
    # BigQuery Bulk Insert Operations
    # =========================================================================

    def get_table_reference(self, model):
        """Build fully-qualified table reference: 'project.dataset.table_name'"""
        project = self.connection.settings_dict.get("PROJECT")
        return f"{project}.{self.dataset}.{model._meta.db_table}"

    def _serialize_value(self, value):
        """Serialize a single value for BigQuery."""
        if isinstance(value, datetime):
            if value.tzinfo is not None:
                value = value.astimezone(timezone.utc).replace(tzinfo=None)
            return value.strftime("%Y-%m-%d %H:%M:%S.%f")
        elif isinstance(value, date):
            return value.isoformat()
        elif value is not None:
            return value
        return None

    def prepare_rows(self, objs):
        """
        Convert model instances or dicts to BigQuery-compatible format.

        Args:
            objs: List of model instances or dicts

        Returns:
            list[dict]: Serialized rows with date/datetime as ISO strings
        """
        if not objs:
            return []

        first = objs[0]

        if isinstance(first, dict):
            prepared = []
            for row in objs:
                clean = {}
                for key, value in row.items():
                    serialized = self._serialize_value(value)
                    if serialized is not None:
                        clean[key] = serialized
                prepared.append(clean)
            return prepared
        else:
            prepared = []
            for instance in objs:
                row = {}
                for field in instance._meta.fields:
                    value = getattr(instance, field.name)
                    serialized = self._serialize_value(value)
                    if serialized is not None:
                        row[field.name] = serialized
                prepared.append(row)
            return prepared

    def bulk_insert_load_job(self, model, objs, batch_size=100_000):
        """
        Bulk insert using load_table_from_file (BigQuery Load Job API).

        This method uses Django's managed connection - no new client is created.
        Load jobs are FREE and recommended for batch/bulk operations.

        Args:
            model: Django model class
            objs: List of model instances or dicts
            batch_size: Rows per load job (default 100K, BQ allows up to 15M)

        Returns:
            dict: {'inserted': int, 'errors': list, 'jobs': int}
        """
        self.connection.ensure_connection()
        client = self.connection.connection
        table_ref = self.get_table_reference(model)
        rows = self.prepare_rows(objs)

        total_inserted = 0
        all_errors = []
        job_count = 0
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            ndjson = "\n".join(json.dumps(row) for row in batch)
            json_bytes = io.BytesIO(ndjson.encode("utf-8"))
            try:
                job = client.load_table_from_file(
                    json_bytes, table_ref, job_config=job_config
                )
                job.result()
                total_inserted += job.output_rows or len(batch)
                job_count += 1
                logger.info(f"Load job batch {job_count}: {job.output_rows or len(batch)} rows")
            except Exception as e:
                all_errors.append({
                    "batch": job_count + 1,
                    "start_index": i,
                    "error": str(e)
                })
                logger.warning(f"Load job batch {job_count + 1} failed: {e}")
        return {"inserted": total_inserted, "errors": all_errors, "jobs": job_count}

    def bulk_insert_streaming(self, model, objs, batch_size=10_000):
        """
        Bulk insert using BigQuery Streaming Insert API.

        Real-time insertion - rows available for query within seconds.
        Note: Streaming inserts are CHARGED per byte.

        Args:
            model: Django model class
            objs: List of model instances or dicts
            batch_size: Rows per API call (max 50K, recommended 10K)

        Returns:
            dict: {'inserted': int, 'errors': list}
        """
        self.connection.ensure_connection()
        client = self.connection.connection
        table_ref = self.get_table_reference(model)
        rows = self.prepare_rows(objs)
        total_inserted = 0
        all_errors = []
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            errors = client.insert_rows_json(table_ref, batch)
            if errors:
                all_errors.extend(errors)
                logger.warning(f"Streaming insert batch {i // batch_size + 1} had errors: {errors}")
            else:
                total_inserted += len(batch)
            logger.info(f"Streaming insert batch {i // batch_size + 1}: {len(batch)} rows")
        return {"inserted": total_inserted, "errors": all_errors}

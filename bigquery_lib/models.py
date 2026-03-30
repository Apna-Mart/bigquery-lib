import logging

from django.db import connections, models
from bigquery_lib.constants import STREAMING, ORM

logger = logging.getLogger("django.db.backends.models")


class BigQueryManager(models.Manager):
    """
    Custom manager for BigQuery models.

    By default uses Django ORM bulk_create. When method='load_job' or 'streaming'
    is specified, uses BigQuery API via DatabaseOperations (Django's managed connection).
    """

    def bulk_create(self, objs, method=ORM, batch_size=None, **kwargs):
        """
        Bulk create with configurable insert method.

        Args:
            objs: List of model instances or dicts to insert
            method: 'orm' (default - Django ORM), 'load_job' (free, batch),
                    or 'streaming' (real-time, paid)
            batch_size: Rows per batch (default: 100K for load_job, 10K for streaming)
            **kwargs: Passed to Django ORM (ignore_conflicts, update_conflicts, etc.)

        Returns:
            list: The objects that were passed in (mimics Django behavior)

        Raises:
            DataError: If insert encounters errors
        """
        if not objs:
            return []
        if method == ORM:
            return super().bulk_create(objs, batch_size=batch_size, **kwargs)
        connection = connections[self.model.connection]
        if method == STREAMING:
            batch_size = batch_size or 10_000
            result = connection.ops.bulk_insert_streaming(
                self.model, objs, batch_size=batch_size
            )
        else:  # LOAD_JOB
            batch_size = batch_size or 150000
            result = connection.ops.bulk_insert_load_job(
                self.model, objs, batch_size=batch_size
            )
        if result.get("errors"):
            raise Exception(
                f"BigQuery bulk insert failed: {len(result['errors'])} "
                f"error(s), {result.get('inserted', 0)} rows inserted. "
                f"Errors: {result['errors']}"
            )
        logger.info(f"bulk_create ({method}): {result.get('inserted', 0)} rows inserted")
        return list(objs)


class BaseBigQueryModel(models.Model):
    """
    Base model for BigQuery with custom methods.
    """
    bq_primary_keys = []
    connection = "bigquery"
    objects = BigQueryManager()

    class Meta:
        abstract = True
        managed = False  # This model is managed by BigQuery, not Django migrations

    def save(self, *args, **kwargs):
        """
        Override save method to use BigQuery.
        """
        connection = kwargs.get("using")
        if not connection:
            logger.info("using default connection from the model")
            connection = self.connection
        logger.info(f"using connection {connection}")
        logger.info("to use a different connection, pass the connection name in the save method: save(using=\"<connection name>\")")
        logger.debug("checking if primary key already exists in table")

        query_params = {field.name: getattr(self, field.name) for field in self._meta.fields}

        if self.bq_primary_keys:
            logger.info("custom primary keys provided")
            logger.info(self.bq_primary_keys)
            filter_params = {key: getattr(self, key) for key in self.bq_primary_keys}
        else:
            logger.info("no custom primary keys provided, using primary key declared in model")
            filter_params = {"pk": self.pk}

        if not bool(self.__class__.objects.using(connection).filter(**filter_params)):
            logger.info("primary key does not exist, inserting new record")
            # If the instance does not have a primary key, insert it
            self.__class__.objects.using(connection).bulk_create([self])
        else:
            logger.info("primary key exists, updating existing record")
            # If the instance has a primary key, update it
            self.__class__.objects.using(connection).filter(**filter_params).update(**query_params)

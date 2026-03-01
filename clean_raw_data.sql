DECLARE sql STRING;

SET sql = (
  SELECT
    'CREATE OR REPLACE TABLE `project.dataset.table_clean` AS SELECT ' ||
    STRING_AGG(
      FORMAT(
        "`%s` AS %s",
        column_name,
        REGEXP_REPLACE(
          LOWER(TRIM(column_name)),
          r'[^a-z0-9]+',
          '_'
        )
      ),
      ', '
    ) ||
    ' FROM `project.dataset.table`'
  FROM `project.dataset.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'table'
);

EXECUTE IMMEDIATE sql;

import { IBaseQueries, ContextValue } from '@sqltools/types';
import queryFactory from '@sqltools/base-driver/dist/lib/factory';

/** write your queries here go fetch desired data. This queries are just examples copied from SQLite driver */

const describeTable: IBaseQueries['describeTable'] = queryFactory`
  SELECT *
  FROM information_schema.columns
  WHERE table_catalog = '${p => p.schema.toLowerCase()}'
  AND table_schema = '${p => p.database.toLowerCase()}'
  AND table_name = '${p => p.label.toLowerCase()}'
`;

const fetchColumns: IBaseQueries['fetchColumns'] = queryFactory`
SELECT *
FROM information_schema.columns
WHERE table_catalog = '${p => p.schema.toLowerCase()}'
AND table_schema = '${p => p.database.toLowerCase()}'
AND table_name = '${p => p.label.toLowerCase()}'
`;

const fetchRecords: IBaseQueries['fetchRecords'] = queryFactory`
SELECT *
FROM ${p => mountTableName(p)}
OFFSET ${p => p.offset || 0}
LIMIT ${p => p.limit || 50};
`;

const countRecords: IBaseQueries['countRecords'] = queryFactory`
SELECT count(1) AS total
FROM ${p => mountTableName(p)};
`;

const mountTableName = (context): string => {
  return `${context.table.schema}.${context.table.database}.${context.table.label}`
}

const fetchTablesAndViews = (type: ContextValue, tableType = 'table'): IBaseQueries['fetchTables'] => queryFactory`
SELECT name AS label,
  '${type}' AS type
FROM sqlite_master
WHERE LOWER(type) LIKE '${tableType.toLowerCase()}'
  AND name NOT LIKE 'sqlite_%'
ORDER BY name
`;

const fetchTables: IBaseQueries['fetchTables'] = fetchTablesAndViews(ContextValue.TABLE);
const fetchViews: IBaseQueries['fetchTables'] = fetchTablesAndViews(ContextValue.VIEW , 'view');

const searchTables: IBaseQueries['searchTables'] = queryFactory`
SELECT name AS label,
  type
FROM sqlite_master
${p => p.search ? `WHERE LOWER(name) LIKE '%${p.search.toLowerCase()}%'` : ''}
ORDER BY name
`;
const searchColumns: IBaseQueries['searchColumns'] = queryFactory`
SELECT C.name AS label,
  T.name AS "table",
  C.type AS dataType,
  C."notnull" AS isNullable,
  C.pk AS isPk,
  '${ContextValue.COLUMN}' as type
FROM sqlite_master AS T
LEFT OUTER JOIN pragma_table_info((T.name)) AS C ON 1 = 1
WHERE 1 = 1
${p => p.tables.filter(t => !!t.label).length
  ? `AND LOWER(T.name) IN (${p.tables.filter(t => !!t.label).map(t => `'${t.label}'`.toLowerCase()).join(', ')})`
  : ''
}
${p => p.search
  ? `AND (
    LOWER(T.name || '.' || C.name) LIKE '%${p.search.toLowerCase()}%'
    OR LOWER(C.name) LIKE '%${p.search.toLowerCase()}%'
  )`
  : ''
}
ORDER BY C.name ASC,
  C.cid ASC
LIMIT ${p => p.limit || 100}
`;

export default {
  describeTable,
  countRecords,
  fetchColumns,
  fetchRecords,
  fetchTables,
  fetchViews,
  searchTables,
  searchColumns
}
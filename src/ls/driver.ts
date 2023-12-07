import AbstractDriver from '@sqltools/base-driver';
import { IConnectionDriver, MConnectionExplorer, NSDatabase, Arg0, ContextValue } from '@sqltools/types';
import queries from './queries';
import { v4 as generateId } from 'uuid';
import { Athena, S3, Credentials, Glue, SharedIniFileCredentials } from 'aws-sdk';
import { GetQueryResultsInput } from 'aws-sdk/clients/athena';

export default class AthenaDriver extends AbstractDriver<Athena, Athena.Types.ClientConfiguration> implements IConnectionDriver {

  queries = queries

  /**
   * If you driver depends on node packages, list it below on `deps` prop.
   * It will be installed automatically on first use of your driver.
   */
  public readonly deps: typeof AbstractDriver.prototype['deps'] = [{
    type: AbstractDriver.CONSTANTS.DEPENDENCY_PACKAGE,
    name: 'lodash',
    // version: 'x.x.x',
  }];

  /** if you need to require your lib in runtime and then
   * use `this.lib.methodName()` anywhere and vscode will take care of the dependencies
   * to be installed on a cache folder
   **/
  // private get lib() {
  //   return this.requireDep('node-packge-name') as DriverLib;
  // }

  public async open() {
    if (this.connection) { 
      return this.connection;
    }

    if (this.credentials.connectionMethod !== 'Profile')
      var credentials = new Credentials({
        accessKeyId: this.credentials.accessKeyId,
        secretAccessKey: this.credentials.secretAccessKey,
        sessionToken: this.credentials.sessionToken,
      });
    else
      var credentials = new SharedIniFileCredentials({ profile: this.credentials.profile });

    this.connection = Promise.resolve(new Athena({
      credentials: credentials,
      region: this.credentials.region || 'us-east-1',
    }));

    return this.connection;
  }

  public async close() { }

  private sleep = (time: number) => new Promise((resolve) => setTimeout(() => resolve(true), time));

  private rawQuery = async (query: string) => {
    const db = await this.open();

    const queryExecution = await db.startQueryExecution({
      QueryString: query,
      WorkGroup: this.credentials.workgroup,
      ResultConfiguration: {
        OutputLocation: this.credentials.outputLocation
      }
    }).promise();

    const endStatus = new Set(['FAILED', 'SUCCEEDED', 'CANCELLED']);

    let queryCheckExecution;

    do {
      queryCheckExecution = await db.getQueryExecution({ 
        QueryExecutionId: queryExecution.QueryExecutionId,
      }).promise();

      await this.sleep(200);
    } while (!endStatus.has(queryCheckExecution.QueryExecution.Status.State))

    if (queryCheckExecution.QueryExecution.Status.State === 'FAILED') {
      throw new Error(queryCheckExecution.QueryExecution.Status.StateChangeReason)
    }

    /* Just get the first page of results so we can do column mapping */
    const payload: GetQueryResultsInput = {
      QueryExecutionId: queryExecution.QueryExecutionId
    };

    let core_result = await db.getQueryResults(payload).promise();

    console.log('Query check execution');
    console.log(queryCheckExecution);

    const bucket = queryCheckExecution.QueryExecution.ResultConfiguration.OutputLocation.split('/')[2];
    const key = queryCheckExecution.QueryExecution.ResultConfiguration.OutputLocation.split('/').slice(3).join('/');
  
    const s3 = new S3({ 
      apiVersion: '2006-03-01', 
      region: this.credentials.region || 'us-east-1', 
      credentials: new Credentials({
        accessKeyId: this.credentials.accessKeyId,
        secretAccessKey: this.credentials.secretAccessKey,
        sessionToken: this.credentials.sessionToken,
      })
    });

    let resultsBase: String = await new Promise((resolve, reject) => {
      const params = {
        Bucket: bucket,
        Key: key
      };
  
      s3.getObject(params, (err, data) => {
        if (err) {
          reject(err);
        } else {
          resolve(data.Body.toString('utf-8'));
        }
      });
    });


    // resultsBase CSV to JSON array
    let resultsJson = resultsBase.split('\n').map(row => row.split(','));
    
    // remove the first row since its a colum header
    resultsJson.shift();

    const columns = core_result.ResultSet.ResultSetMetadata.ColumnInfo.map((info) => info.Name);

    const resultSet = [];
    
    resultsJson.forEach(row => {
      let resultToAppend = {}
      columns.forEach((column, i) => {
        resultToAppend[column] = row[i];
      }
      );
      resultSet.push(resultToAppend);
    });

    return {
      columns: columns,
      results: resultSet
    };
  }

  public query: (typeof AbstractDriver)['prototype']['query'] = async (queries, opt = {}) => {
    const results = await this.rawQuery(queries.toString());
    const columns = results.columns;
    const resultSet = results.results;

    const response: NSDatabase.IResult[] = [{
      cols: columns,
      connId: this.getId(),
      messages: [{ date: new Date(), message: `Query ok with ${resultSet.length} results`}],
      results: resultSet,
      query: queries.toString(),
      requestId: opt.requestId,
      resultId: generateId(),
    }];

    return response;
  }

  /** if you need a different way to test your connection, you can set it here.
   * Otherwise by default we open and close the connection only
   */
  public async testConnection() {
    await this.open();
    await this.query('SELECT 1', {});
  }

  /**
   * This method is a helper to generate the connection explorer tree.
   * it gets the child items based on current item
   */
  public async getChildrenForItem({ item, parent }: Arg0<IConnectionDriver['getChildrenForItem']>) {
    const db = await this.connection;

    switch (item.type) {
      case ContextValue.CONNECTION:
      case ContextValue.CONNECTED_CONNECTION:
        return <MConnectionExplorer.IChildItem[]>[
          { label: 'Catalogs', type: ContextValue.RESOURCE_GROUP, iconId: 'folder', childType: ContextValue.SCHEMA },
        ]
      case ContextValue.SCHEMA:
        return <MConnectionExplorer.IChildItem[]>[
          { label: 'Databases', type: ContextValue.RESOURCE_GROUP, iconId: 'group-by-ref-type', childType: ContextValue.DATABASE },
        ];
      case ContextValue.DATABASE:
        return <MConnectionExplorer.IChildItem[]>[
          { label: 'Tables', type: ContextValue.RESOURCE_GROUP, iconId: 'folder', childType: ContextValue.TABLE },
          { label: 'Views', type: ContextValue.RESOURCE_GROUP, iconId: 'folder', childType: ContextValue.VIEW },
        ];
      case ContextValue.TABLE:
      case ContextValue.VIEW:
        const tableMetadata = await db.getTableMetadata({
          CatalogName: item.schema,
          DatabaseName: item.database,
          TableName: item.label
        }).promise();

        let to_return = [
          ...tableMetadata.TableMetadata.Columns,
          ...tableMetadata.TableMetadata.PartitionKeys,
        ].map(column => ({
          label: column.Name,
          type: ContextValue.COLUMN,
          dataType: column.Type,
          detail: `${column.Type}${column.Comment ? ` - ${column.Comment}` : ''}`,
          schema: item.schema,
          database: item.database,
          childType: ContextValue.NO_CHILD,
          isNullable: true,
          iconName: column.Name.toLowerCase() === 'id' ? 'pk' : 'column',
          table: parent,
        }));

        return to_return;
      case ContextValue.RESOURCE_GROUP:
        return this.getChildrenForGroup({ item, parent });
    }
    
    return [];
  }

  /**
   * This method is a helper to generate the connection explorer tree.
   * It gets the child based on child types
   */
  private async getChildrenForGroup({ parent, item }: Arg0<IConnectionDriver['getChildrenForItem']>) {
    const db = await this.connection;
    
    switch (item.childType) {
      case ContextValue.SCHEMA:
        const catalogs = await db.listDataCatalogs().promise();

        return catalogs.DataCatalogsSummary.map((catalog) => ({
          database: '',
          label: catalog.CatalogName,
          type: item.childType,
          schema: catalog.CatalogName,
          childType: ContextValue.DATABASE,
        }));
      case ContextValue.DATABASE:
        let databaseList = [];          
        let firstBatch:boolean = true;
        let nextToken:string|null = null;
        
        while (firstBatch == true || nextToken !== null) {
          firstBatch = false;
          let listDbRequest = {
            CatalogName: parent.schema,
          }
          if (nextToken !== null) {
            Object.assign(listDbRequest, {
              NextToken: nextToken,
            });
          }
          const catalog = await db.listDatabases(listDbRequest).promise();
          nextToken = 'NextToken' in catalog ? catalog.NextToken : null;

          databaseList = databaseList.concat(
            catalog.DatabaseList.map((database) => ({
              database: database.Name,
              detail: database.Description,
              label: database.Name,
              type: item.childType,
              schema: parent.schema,
              childType: ContextValue.TABLE,
            })));
        }
        return databaseList;
      case ContextValue.TABLE:
        const tables = await this.rawQuery(`SHOW TABLES IN \`${parent.database}\``);
        const views = await this.rawQuery(`SHOW VIEWS IN "${parent.database}"`);

        const viewsSet = new Set(views[0].ResultSet.Rows.map((row) => row.Data[0].VarCharValue));

        return tables[0].ResultSet.Rows
          .filter((row) => !viewsSet.has(row.Data[0].VarCharValue))
          .map((row) => ({
            database: parent.database,
            label: row.Data[0].VarCharValue,
            type: item.childType,
            schema: parent.schema,
            childType: ContextValue.COLUMN,
          }));
      case ContextValue.VIEW:
        const views2 = await this.rawQuery(`SHOW VIEWS IN "${parent.database}"`);
        
        return views2[0].ResultSet.Rows.map((row) => ({
          database: parent.database,
          label: row.Data[0].VarCharValue,
          type: item.childType,
          schema: parent.schema,
          childType: ContextValue.COLUMN,
        }));
    }
    return [];
  }

  /**
   * This method is a helper for intellisense and quick picks.
   */
  public async searchItems(itemType: ContextValue, search: string, _extraParams: any = {}): Promise<NSDatabase.SearchableItem[]> {
    const db = await this.connection;
    if (this.credentials.connectionMethod !== 'Profile')
      var credentials = new Credentials({
        accessKeyId: this.credentials.accessKeyId,
        secretAccessKey: this.credentials.secretAccessKey,
        sessionToken: this.credentials.sessionToken,
      });
    else
      var credentials = new SharedIniFileCredentials({ profile: this.credentials.profile });
    const glueCon = Promise.resolve(new Glue({
      credentials: credentials,
      region: this.credentials.region || 'us-east-1',
    }));
    const glue = await glueCon;

    switch (itemType) {
      case ContextValue.DATABASE:
        const dbOutput: NSDatabase.SearchableItem[] = [];
        var dbNextToken = 'first';
        const uniqueDbs = {};
        
        while (dbNextToken == 'first' || dbNextToken != null) {
          const dbParams: AWS.Glue.GetDatabasesRequest = {
            MaxResults: 100
          }
          if (dbNextToken != 'first') {
            dbParams.NextToken = dbNextToken;
          }
          
          await glue.getDatabases(dbParams, function(err, data) {
            if (err) console.log(err, err.stack);
            else {
              if (data.DatabaseList) {
                for (const db of data.DatabaseList) {
                  const dbDetails: NSDatabase.IDatabase = {
                    database: db.Name,
                    label: db.Name,
                    type: itemType,
                    schema: db.Name
                  }
                  uniqueDbs[db.Name] = dbDetails
                }
                dbNextToken = data.NextToken;
              }
            }
          }).promise();
        }

        for (const i in uniqueDbs) {
          dbOutput.push(uniqueDbs[i])
        }

        return dbOutput;

      case ContextValue.TABLE:
        const database = _extraParams.database;
        if (!database) {
          return [];
        }

        const tableResults = await this.rawQuery(`SHOW TABLES IN ${database}`);
        return tableResults[0].ResultSet.Rows
          .map((row) => ({
            database: database,
            label: row.Data[0].VarCharValue,
            type: itemType,
            schema: database
          }));

      case ContextValue.VIEW:
        console.log('view search');
        let j = 0;
        return [{
          database: 'fakedb',
          label: `${search || 'table'}${j++}`,
          type: itemType,
          schema: 'fakeschema',
          childType: ContextValue.COLUMN,
        },{
          database: 'fakedb',
          label: `${search || 'table'}${j++}`,
          type: itemType,
          schema: 'fakeschema',
          childType: ContextValue.COLUMN,
        },
        {
          database: 'fakedb',
          label: `${search || 'table'}${j++}`,
          type: itemType,
          schema: 'fakeschema',
          childType: ContextValue.COLUMN,
        }]
        
      case ContextValue.COLUMN:
        const tables = _extraParams.tables.filter(t => t.database);

        const columns: NSDatabase.SearchableItem[] = [];
        for await (const table of tables) {
          const params: AWS.Athena.GetTableMetadataInput = {
            CatalogName: 'AwsDataCatalog',
            DatabaseName: table.database,
            TableName: table.label
          };

          await db.getTableMetadata(params, function(err, data) {
            if (err) {
              console.log(err);
            } else {
              if (data.TableMetadata.Columns) {
                for (const col of data.TableMetadata.Columns) {
                  const colDetails: NSDatabase.IColumn = {
                    database: table.database,
                    label: col.Name,
                    type: itemType,
                    schema: table.database,
                    dataType: col.Type,
                    childType: ContextValue.NO_CHILD,
                    isNullable: true,
                    iconName: 'column',
                    table: table.label
                  }
                  columns.push(colDetails);
                }
              }
            }
          }).promise();
        }
        return columns;
    }
    return [];
}

  public getStaticCompletions: IConnectionDriver['getStaticCompletions'] = async () => {
    return {};
  }
}

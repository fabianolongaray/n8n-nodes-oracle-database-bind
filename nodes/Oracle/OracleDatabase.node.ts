import { IExecuteFunctions } from "n8n-workflow";
import {
  IDataObject,
  INodeExecutionData,
  INodeType,
  INodeTypeDescription,
  NodeOperationError,
} from "n8n-workflow";
import oracledb from "oracledb";
import { OracleConnection } from "./core/connection";

/**
 * Polyfill replaceAll (Node antigos)
 */
declare global {
  interface String {
    replaceAll(searchValue: string | RegExp, replaceValue: string): string;
    replaceAll(searchValue: string | RegExp, replacer: (substring: string, ...args: any[]) => string): string;
  }
}
if (typeof String.prototype.replaceAll === "undefined") {
  // eslint-disable-next-line no-extend-native
  String.prototype.replaceAll = function (
    searchValue: string | RegExp,
    replaceValue: string | ((substring: string, ...args: any[]) => string)
  ): string {
    return this.replace(new RegExp(searchValue as string, "g"), replaceValue as any);
  };
}

type ParamDirection = "in" | "out" | "inout";
type ParamDatatype = "string" | "number" | "date" | "cursor";

export class OracleDatabase implements INodeType {
  description: INodeTypeDescription = {
    displayName: "Oracle Database with Bind Variables",
    name: "Oracle Database with Bind Variables",
    icon: "file:oracle.svg",
    group: ["input"],
    version: 1,
    description: "Upsert, get, add and update data in Oracle database",
    defaults: {
      name: "Oracle Database",
    },
    inputs: ["main"],
    outputs: ["main"],
    credentials: [
      {
        name: "oracleCredentials",
        required: true,
      },
    ],
    properties: [
      {
        displayName: "SQL Statement",
        name: "query",
        type: "string",
        typeOptions: {
          alwaysOpenEditWindow: true,
        },
        default: "",
        placeholder: "SELECT id, name FROM product WHERE id < :param_name",
        required: true,
        description: "The SQL query to execute",
      },

      // ===== Collection de parâmetros =====
      {
        displayName: "Parameters",
        name: "params",
        placeholder: "Add Parameter",
        type: "fixedCollection",
        typeOptions: {
          multipleValueButtonText: "Add another Parameter",
          multipleValues: true,
        },
        default: {},
        options: [
          {
            displayName: "Values",
            name: "values",
            values: [
              {
                displayName: "Name",
                name: "name",
                type: "string",
                default: "",
                placeholder: "e.g. param_name",
                hint: 'Do not start with ":"',
                required: true,
              },
              {
                displayName: "Data Type",
                name: "datatype",
                type: "options",
                required: true,
                default: "string",
                options: [
                  { name: "String", value: "string" },
                  { name: "Number", value: "number" },
                  { name: "Date", value: "date" },
                  { name: "REF CURSOR (OUT)", value: "cursor" },
                ],
              },
              {
                displayName: "Direction",
                name: "direction",
                type: "options",
                required: true,
                default: "in",
                options: [
                  { name: "IN", value: "in" },
                  { name: "OUT", value: "out" },
                  { name: "IN OUT", value: "inout" },
                ],
              },
              {
                displayName: "Value",
                name: "value",
                type: "string",
                default: "",
                placeholder: "Example: 12345 or 2024-01-01",
                required: true,
                displayOptions: {
                  show: {
                    direction: ["in", "inout"],
                  },
                },
              },
              {
                displayName: "Parse for IN statement",
                name: "parseInStatement",
                type: "options",
                required: true,
                default: false,
                hint: 'If "Yes", the "Value" should be comma-separated: 1,2,3 or str1,str2',
                options: [
                  { name: "No", value: false },
                  { name: "Yes", value: true },
                ],
              },
              {
                displayName: "Max Size",
                name: "maxSize",
                type: "number",
                default: 4000,
                required: true,
                hint: "Required for OUT/INOUT of STRING values",
                displayOptions: {
                  show: {
                    direction: ["out", "inout"],
                    datatype: ["string"],
                  },
                },
              },
            ],
          },
        ],
      },
    ],
  };

  async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
    const credentials = await this.getCredentials("oracleCredentials");
    const oracleCredentials = {
      user: String(credentials.user),
      password: String(credentials.password),
      connectionString: String(credentials.connectionString),
    };

    const db = new OracleConnection(
      oracleCredentials,
      Boolean((credentials as IDataObject).thinMode)
    );
    const connection = await db.getConnection();

    let returnItems: INodeExecutionData[] = [];

    /**
     * Helpers
     */
    const mapDatatype = (dt: ParamDatatype): oracledb.DbType => {
      switch (dt) {
        case "number":
          return oracledb.NUMBER;
        case "date":
          return oracledb.DATE;
        case "cursor":
          return oracledb.CURSOR;
        case "string":
        default:
          return oracledb.STRING;
      }
    };

    const coerceVal = (raw: string, dt: ParamDatatype) => {
      if (dt === "number") return Number(raw);
      if (dt === "date") return new Date(raw);
      // cursor não tem val
      return String(raw);
    };

    const ensureOutMaxSize = (dt: ParamDatatype, maxSize?: number) => {
      // Para strings/buffers OUT/INOUT, o driver exige maxSize
      if (dt === "string" && (!maxSize || Number.isNaN(maxSize))) {
        return 2000;
      }
      return maxSize;
    };

    const normalizeOutBinds = async (outBinds: any): Promise<any> => {
      // Pode vir como objeto { name: val } ou array(s) quando RETURNING INTO
      const isResultSet = (v: any) =>
        v && typeof v === "object" && typeof v.getRows === "function" && typeof v.close === "function";

      const readResultSet = async (rs: oracledb.ResultSet<any>) => {
        // lê tudo; ajuste se precisar paginação
        const rows = await rs.getRows(10000);
        await rs.close();
        return rows;
      };

      if (Array.isArray(outBinds)) {
        // RETURNING INTO geralmente retorna arrays
        return Promise.all(
          outBinds.map(async (val) => (isResultSet(val) ? await readResultSet(val) : val))
        );
      }

      if (outBinds && typeof outBinds === "object") {
        const entries = await Promise.all(
          Object.entries(outBinds).map(async ([k, v]) => {
            if (isResultSet(v)) {
              const rows = await readResultSet(v as unknown as oracledb.ResultSet<any>);
              return [k, rows];
            }
            // Pode ser array (RETURNING) ou valor simples
            if (Array.isArray(v)) {
              const arr = await Promise.all(
                v.map(async (it) => (isResultSet(it) ? await readResultSet(it) : it))
              );
              return [k, arr];
            }
            return [k, v];
          })
        );
        return Object.fromEntries(entries);
      }

      return outBinds;
    };

    try {
      // Query
      let query = this.getNodeParameter("query", 0) as string;

      // Parâmetros do UI
      const parameterIDataObjectList =
        ((this.getNodeParameter("params", 0, {}) as IDataObject).values as {
          name: string;
          value?: string | number;
          datatype: ParamDatatype;
          direction: ParamDirection;
          maxSize?: number;
          parseInStatement: boolean;
        }[]) || [];

      // Montagem dos binds
      const bindParameters: { [key: string]: oracledb.BindParameter } =
        parameterIDataObjectList.reduce((result, item) => {
          const { name, value, datatype, direction, maxSize, parseInStatement } = item;
          const dbType = mapDatatype(datatype);

          // REGRAS:
          // - parseInStatement só é válido para IN
          if (parseInStatement) {
            if (direction !== "in") {
              throw new NodeOperationError(
                this.getNode(),
                `Parameter "${name}" uses "Parse for IN statement" but direction is ${direction.toUpperCase()}`
              );
            }
            if (datatype === "cursor") {
              throw new NodeOperationError(
                this.getNode(),
                `Parameter "${name}" cannot be CURSOR with "Parse for IN statement"`
              );
            }

            const valList = String(value ?? "")
              .split(",")
              .map((v) => v.trim())
              .filter((v) => v.length > 0);

            if (valList.length === 0) {
              throw new NodeOperationError(this.getNode(), `Parameter "${name}" has no values for IN list.`);
            }

            const crypto = require("crypto");
            let generatedSqlString = "(";

            valList.forEach((v: string) => {
              const uniqueId: string = crypto.randomUUID().replaceAll("-", "_");
              const newParamName = `${name}${uniqueId}`;

              result[newParamName] = {
                dir: oracledb.BIND_IN,
                type: dbType,
                val: coerceVal(v, datatype),
              };

              generatedSqlString += `:${newParamName},`;
            });

            generatedSqlString = generatedSqlString.slice(0, -1) + ")";
            query = query.replaceAll(":" + name, generatedSqlString);
            return result;
          }

          // Sem parseInStatement: bind simples conforme direção
          if (direction === "in") {
            if (datatype === "cursor") {
              throw new NodeOperationError(this.getNode(), `Parameter "${name}" cannot be CURSOR with IN direction.`);
            }
            result[name] = {
              dir: oracledb.BIND_IN,
              type: dbType,
              val: coerceVal(String(value ?? ""), datatype),
            };
          } else if (direction === "out") {
            const finalMax = ensureOutMaxSize(datatype, Number(maxSize));
            result[name] = {
              dir: oracledb.BIND_OUT,
              type: dbType,
              ...(dbType === oracledb.STRING ? { maxSize: finalMax } : {}),
            };
          } else {
            // IN OUT
            if (datatype === "cursor") {
              throw new NodeOperationError(this.getNode(), `Parameter "${name}" cannot be CURSOR with IN OUT direction.`);
            }
            const finalMax = ensureOutMaxSize(datatype, Number(maxSize));
            result[name] = {
              dir: oracledb.BIND_INOUT,
              type: dbType,
              val: coerceVal(String(value ?? ""), datatype),
              ...(dbType === oracledb.STRING ? { maxSize: finalMax } : {}),
            };
          }

          return result;
        }, {} as { [key: string]: oracledb.BindParameter });

      // Execução
      const execResult = await connection.execute(
        query,
        bindParameters,
        {
          outFormat: oracledb.OUT_FORMAT_OBJECT,
          autoCommit: true,
        }
      );

      // Monte um único objeto com tudo
      const item: IDataObject = {
        metaData: (execResult as any).metaData,
        rows: (execResult as any).rows,
        rowsAffected: (execResult as any).rowsAffected,
        lastRowid: (execResult as any).lastRowid,
        outBinds: (execResult as any).outBinds,
      };

      returnItems = this.helpers.returnJsonArray([item]);
    } catch (error: any) {
      throw new NodeOperationError(this.getNode(), error?.message || String(error));
    } finally {
      if (connection) {
        try {
          await connection.close();
        } catch (error) {
          // eslint-disable-next-line no-console
          console.error(`OracleDB: Failed to close the database connection: ${error}`);
        }
      }
    }

    return [returnItems];
  }
}
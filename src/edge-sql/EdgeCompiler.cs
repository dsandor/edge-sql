using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using Microsoft.SqlServer.Server;

public class EdgeCompiler
{
    public Func<object, Task<object>> CompileFunc(IDictionary<string, object> parameters)
    {
        string command = ((string)parameters["source"]).TrimStart();
        string connectionString = Environment.GetEnvironmentVariable("EDGE_SQL_CONNECTION_STRING");
        object tmp;
        if (parameters.TryGetValue("connectionString", out tmp))
        {
            connectionString = (string)tmp;
        }

        if (command.StartsWith("select ", StringComparison.InvariantCultureIgnoreCase))
        {
            return async (queryParameters) =>
            {
                return await this.ExecuteQuery(connectionString, command, (IDictionary<string, object>)queryParameters);
            };
        }
        else if (command.StartsWith("insert ", StringComparison.InvariantCultureIgnoreCase)
            || command.StartsWith("update ", StringComparison.InvariantCultureIgnoreCase)
            || command.StartsWith("delete ", StringComparison.InvariantCultureIgnoreCase))
        {
            return async (queryParameters) =>
            {
                return await this.ExecuteNonQuery(connectionString, command, (IDictionary<string, object>)queryParameters);
            };
        }
        else if (command.StartsWith("exec ", StringComparison.InvariantCultureIgnoreCase))
        {
            return async (queryParameters) => await
                this.ExecuteStoredProcedure(
                    connectionString,
                    command,
                    (IDictionary<string, object>)queryParameters);
        }
        else
        {
            throw new InvalidOperationException("Unsupported type of SQL command. Only select, insert, update, delete, and exec are supported.");
        }
    }

    void AddParamaters(SqlCommand command, IDictionary<string, object> parameters)
    {
        if (parameters != null)
        {
            foreach (KeyValuePair<string, object> parameter in parameters)
            {
                if (parameter.Value != null && parameter.Value.GetType() == typeof(ExpandoObject))
                {
                    if (((IDictionary<string, object>)parameter.Value).ContainsKey("UdtType"))
                    {
                        var dictionary = (IDictionary<string, object>)parameter.Value;

                        var udtType = dictionary["UdtType"].ToString();
                        var rows = dictionary["Rows"];
                        var sqlDataRecords = CreateSqlDataRecords((IEnumerable<object>)rows);

                        var commandParameter = command.Parameters.AddWithValue(parameter.Key, sqlDataRecords);
                        commandParameter.SqlDbType = SqlDbType.Structured;
                        commandParameter.TypeName = udtType;
                    }
                }
                else
                {
                    command.Parameters.AddWithValue(parameter.Key, parameter.Value ?? DBNull.Value);
                }
            }
        }
    }

    private DataTable CreateDataTable(IEnumerable<object> values)
    {
        DataTable table = new DataTable();

        var sampleFirstRow = values.FirstOrDefault();

        if (sampleFirstRow == null) return table;

        var row = sampleFirstRow as IDictionary<string, object>;

        foreach (var key in row.Keys)
        {
            table.Columns.Add(key, row[key].GetType());
        }

        foreach (IDictionary<string, object> value in values)
        {
            table.Rows.Add(value.Values.ToArray<object>());
        }

        return table;
    }

    private IEnumerable<SqlDataRecord> CreateSqlDataRecords(IEnumerable<object> values)
    {
        var sampleFirstRow = values.FirstOrDefault();

        if (sampleFirstRow == null) yield break;

        var row = sampleFirstRow as IDictionary<string, object>;

        var keyArray = row.Keys.ToArray<string>();

        SqlMetaData[] metaData = new SqlMetaData[row.Keys.Count];

        for (int i = 0; i < keyArray.Length; i++)
        {
            metaData[i] = new SqlMetaData(keyArray[i], GetSqlDbType(row[keyArray[i]].GetType()));
        }

        SqlDataRecord record = new SqlDataRecord(metaData);

        foreach (IDictionary<string, object> value in values)
        {
            record.SetValues(value.Values.ToArray<object>());
            yield return record;
        }
    }

    async Task<object> ExecuteQuery(string connectionString, string commandString, IDictionary<string, object> parameters)
    {
        using (SqlConnection connection = new SqlConnection(connectionString))
        {
            using (var command = new SqlCommand(commandString, connection))
            {
                return await this.ExecuteQuery(parameters, command, connection);
            }
        }
    }

    async Task<object> ExecuteQuery(IDictionary<string, object> parameters, SqlCommand command, SqlConnection connection)
    {
        List<object> recordsets = new List<object>();
        
        this.AddParamaters(command, parameters);
        await connection.OpenAsync();
        using (SqlDataReader reader = await command.ExecuteReaderAsync(CommandBehavior.CloseConnection))
        {
            IDataRecord record = (IDataRecord)reader;
            // try to process all result sets, even if they are empty
            while (true)
            {
                List<object> rows = new List<object>();
                while (await reader.ReadAsync())
                {
                    var dataObject = new ExpandoObject() as IDictionary<string, Object>;
                    var resultRecord = new object[record.FieldCount];
                    record.GetValues(resultRecord);

                    for (int i = 0; i < record.FieldCount; i++)
                    {
                        Type type = record.GetFieldType(i);
                        var sqlType = record.GetDataTypeName(i).ToLower();
                        
                        if (resultRecord[i] is System.DBNull)
                        {
                            resultRecord[i] = null;
                        }
                        else if (type == typeof(byte[]) || type == typeof(char[]))
                        {
                            resultRecord[i] = Convert.ToBase64String((byte[])resultRecord[i]);
                        }
                        else if (sqlType.Equals("time"))
                        {
                            var time = ((DateTime)resultRecord[i]);
                            resultRecord[i] = time.ToString("HH:mm:ss.fffffff");
                        }
                        else if (sqlType.Equals("date"))
                        {
                            var date = ((DateTime) resultRecord[i]);
                            resultRecord[i] = date.ToString("yyyy-MM-dd");
                        }
                        else if (sqlType.Equals("datetimeoffset"))
                        {
                            var date = ((DateTime)resultRecord[i]);
                            resultRecord[i] = date.ToString("yyyy-MM-ddTHH:mm:ss.fffffff");
                        }
                        else if (sqlType.Equals("datetime") || sqlType.Equals("datetime2") || sqlType.Equals("smalldatetime"))
                        {
                            var date = ((DateTime) resultRecord[i]);
                            resultRecord[i] = date.ToString("yyyy-MM-ddTHH:mm:ss.fffffff");
                        }
                        else if (type == typeof(DateTime))
                        {
                            var date = ((DateTime) resultRecord[i]);
                            resultRecord[i] = date.ToString("yyyy-MM-ddTHH:mm:ss.fffffff");
                        }
                        else if (type == typeof(Guid))
                        {
                            resultRecord[i] = resultRecord[i].ToString();
                        }
                        else if (type == typeof(IDataReader))
                        {
                            resultRecord[i] = "<IDataReader>";
                        }

                        dataObject.Add(record.GetName(i), resultRecord[i]);
                    }

                    rows.Add(dataObject);
                }

                recordsets.Add(rows);

                // Break if no more results
                if (!reader.NextResult()) break;
            }

            if (recordsets.Count == 1)
                return recordsets[0];
            else
                return recordsets;
        }
    }

    async Task<object> ExecuteNonQuery(string connectionString, string commandString, IDictionary<string, object> parameters)
    {
        using (var connection = new SqlConnection(connectionString))
        {
            using (var command = new SqlCommand(commandString, connection))
            {
                this.AddParamaters(command, parameters);
                await connection.OpenAsync();
                return await command.ExecuteNonQueryAsync();
            }
        }
    }

    async Task<object> ExecuteStoredProcedure(
        string connectionString,
        string commandString,
        IDictionary<string, object> parameters)
    {
        using (SqlConnection connection = new SqlConnection(connectionString))
        {
            SqlCommand command = new SqlCommand(commandString.Substring(5).TrimEnd(), connection)
            {
                CommandType = CommandType.StoredProcedure
            };
            using (command)
            {
                return await this.ExecuteQuery(parameters, command, connection);
            }
        }
    }

    public static Type GetClrType(SqlDbType sqlType)
    {
        switch (sqlType)
        {
            case SqlDbType.BigInt:
                return typeof(long?);

            case SqlDbType.Binary:
            case SqlDbType.Image:
            case SqlDbType.Timestamp:
            case SqlDbType.VarBinary:
                return typeof(byte[]);

            case SqlDbType.Bit:
                return typeof(bool?);

            case SqlDbType.Char:
            case SqlDbType.NChar:
            case SqlDbType.NText:
            case SqlDbType.NVarChar:
            case SqlDbType.Text:
            case SqlDbType.VarChar:
            case SqlDbType.Xml:
                return typeof(string);

            case SqlDbType.DateTime:
            case SqlDbType.SmallDateTime:
            case SqlDbType.Date:
            case SqlDbType.Time:
            case SqlDbType.DateTime2:
                return typeof(DateTime?);

            case SqlDbType.Decimal:
            case SqlDbType.Money:
            case SqlDbType.SmallMoney:
                return typeof(decimal?);

            case SqlDbType.Float:
                return typeof(double?);

            case SqlDbType.Int:
                return typeof(int?);

            case SqlDbType.Real:
                return typeof(float?);

            case SqlDbType.UniqueIdentifier:
                return typeof(Guid?);

            case SqlDbType.SmallInt:
                return typeof(short?);

            case SqlDbType.TinyInt:
                return typeof(byte?);

            case SqlDbType.Variant:
            case SqlDbType.Udt:
                return typeof(object);

            case SqlDbType.Structured:
                return typeof(DataTable);

            case SqlDbType.DateTimeOffset:
                return typeof(DateTimeOffset?);

            default:
                throw new ArgumentOutOfRangeException("sqlType");
        }
    }

    public static SqlDbType GetSqlDbType(Type clrType)
    {
        if (clrType == typeof(long) || clrType == typeof(long?))
           return SqlDbType.BigInt;

        if (clrType == typeof(byte[]))
           return SqlDbType.Binary;

        if (clrType == typeof(bool?) || clrType == typeof(bool))
            return SqlDbType.Bit;

        if (clrType == typeof(string))
            return SqlDbType.VarChar;

        if (clrType == typeof(DateTime) || clrType == typeof(DateTime?))
            return SqlDbType.DateTime2;

        if (clrType == typeof(double?) || clrType == typeof(double))
            return SqlDbType.Float;

        if (clrType == typeof(int?) || clrType == typeof(int))
            return SqlDbType.Int;

        if (clrType == typeof(float?) || clrType == typeof(float))
            return SqlDbType.Real;

        if (clrType == typeof(Guid?) || clrType == typeof(Guid))
            return SqlDbType.UniqueIdentifier;

        if (clrType == typeof(short?) || clrType == typeof(short))
            return SqlDbType.SmallInt;

        if (clrType == typeof(byte?) || clrType == typeof(byte))
            return SqlDbType.TinyInt;

        if (clrType == typeof(DataTable))
            return SqlDbType.Structured;

        if (clrType == typeof(DateTimeOffset) || clrType == typeof(DateTimeOffset?))
            return SqlDbType.DateTimeOffset;

        return SqlDbType.Variant;
    }
}

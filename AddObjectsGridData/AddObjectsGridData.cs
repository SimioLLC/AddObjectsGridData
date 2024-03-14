using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Drawing;
using System.Text;
using System.Threading.Tasks;
using SimioAPI;
using SimioAPI.Extensions;
using System.IO;
using System.Xml;
using System.Runtime.Remoting.Contexts;

namespace AddObjectsGridData
{
    [RequiresGetLocalTableRecords]
    public class AddObjectsImporterDefinition : IGridDataImporterDefinition
    {
        public string Name => "Add Objects";
        public string Description => "Add Objects Transformer";
        public Image Icon => null;

        static readonly Guid MY_ID = new Guid("f137d316-86b1-405e-9b80-8e460b51afe5");
        public Guid UniqueID => MY_ID;

        public IGridDataImporter CreateInstance(IGridDataImporterContext context)
        {
            return new AddObjectsGridData(context);
        }

        public void DefineSchema(IGridDataSchema schema)
        {
            var tablesProp = schema.OverallProperties.AddNameValuePairsProperty("Tables", null);
            tablesProp.DisplayName = "Tables";
            tablesProp.Description = "Tables.";
            tablesProp.DefaultValue = String.Empty;

            var controlsProp = schema.OverallProperties.AddNameValuePairsProperty("Controls", null);
            controlsProp.DisplayName = "Controls";
            controlsProp.Description = "Controls.";
            controlsProp.DefaultValue = String.Empty;
        }
    }

    class AddObjectsGridData : IGridDataImporter
    {
        public AddObjectsGridData(IGridDataImporterContext context)
        {
        }

        public OpenImportDataResult OpenData(IGridDataOpenImportDataContext openContext)
        {
            DataSet tablesDataSet = new DataSet();
            string sessionKey = "AddObjectsGridData";
            string xmlDataSet = String.Empty; 
            if (openContext.SessionCache != null)
            {
                xmlDataSet = (string)openContext.SessionCache.GetNamedValue(sessionKey);
            }

            if (xmlDataSet != null && xmlDataSet.Length > 0)
            {
                tablesDataSet.ReadXml(new XmlTextReader(new StringReader(xmlDataSet)));
            }
            else
            {
                var tablesStr = (string)openContext.Settings?.Properties["Tables"]?.Value;
                var tables = AddInPropertyValueHelper.NameValuePairsFromString(tablesStr);
                var tablesArr = tables.Select(z => z.Value).ToArray();
                if (tablesArr.Length == 0)
                    return OpenImportDataResult.Failed("No tables have been defined.");

                // Where Tranformation Happens
                foreach (var tableStr in tablesArr)
                {
                    var table = openContext.GetLocalTableRecords(tableStr);
                    var dataTable = AddObjectsUtils.ConvertExportContextToDataTable(table, tableStr);
                    tablesDataSet.Tables.Add(dataTable);
                }

                var controlsStr = (string)openContext.Settings?.Properties["Controls"]?.Value;
                var controls = AddInPropertyValueHelper.NameValuePairsFromString(controlsStr);
                var controlsArr = controls.Select(z => z.Value).ToArray();
                if (controlsArr.Length == 0)
                    return OpenImportDataResult.Failed("No Controls have been defined.");

                var depts = tablesDataSet.Tables["tbl_Depts"];
                var objects = tablesDataSet.Tables["tbl_Objects"];
                objects.Rows.Clear();

                foreach (DataRow deptsRow in depts.Rows)
                {
                    int numServers = Convert.ToInt32(controlsArr[Convert.ToInt32(deptsRow.ItemArray[depts.Columns.IndexOf("ControlIndex")])]);
                    int serverIndex = 0;

                    for(int i = 0; i < numServers; i++)
                    {
                        serverIndex++;
                        object[] thisRow = new object[objects.Columns.Count];
                        thisRow[objects.Columns.IndexOf("ObjectName")] = deptsRow.ItemArray[depts.Columns.IndexOf("ObjectType")] + serverIndex.ToString();
                        thisRow[objects.Columns.IndexOf("ObjectType")] = deptsRow.ItemArray[depts.Columns.IndexOf("ObjectType")];
                        thisRow[objects.Columns.IndexOf("DeptName")] = deptsRow.ItemArray[depts.Columns.IndexOf("DeptName")];
                        thisRow[objects.Columns.IndexOf("InputNode")] = "Input@" + thisRow[objects.Columns.IndexOf("ObjectName")];
                        thisRow[objects.Columns.IndexOf("X")] = Convert.ToString(((i + 1) * (double)deptsRow.ItemArray[depts.Columns.IndexOf("XOffset")]) + (double)deptsRow.ItemArray[depts.Columns.IndexOf("X")]);
                        thisRow[objects.Columns.IndexOf("Z")] = Convert.ToString(((i + 1) * (double)deptsRow.ItemArray[depts.Columns.IndexOf("ZOffset")]) + (double)deptsRow.ItemArray[depts.Columns.IndexOf("Z")]);
                        objects.Rows.Add(thisRow);
                    }                    
                }

                // write dataset to cache
                if (openContext.SessionCache != null)
                {
                    xmlDataSet = AddObjectsUtils.ToStringAsXml(tablesDataSet);
                    openContext.SessionCache.SetNamedValue(sessionKey, xmlDataSet);
                }
            }

            return new OpenImportDataResult()
            {
                Result = GridDataOperationResult.Succeeded,
                Records = new AddObjectsGridDataRecords(tablesDataSet, openContext.TableName)
            };
        }

        public string GetDataSummary(IGridDataSummaryContext context)
        {
            if (context == null)
                return null;

            var sourceTablesStr = (string)context.Settings?.Properties["SourceTables"]?.Value;
            var sourceTables = AddInPropertyValueHelper.NameValuePairsFromString(sourceTablesStr);
            var sourceTablesArr = sourceTables.Select(z => z.Value).ToArray();

            if (sourceTablesArr.Length == 0)
                return null;

            var sourceTablesJoin = String.Join(",", sourceTablesArr);

            return String.Format("Bound to {0} ", sourceTablesJoin);
        }

        public void Dispose()
        {
        }
    }

    class AddObjectsGridDataRecords : IGridDataRecords
    {
        // Acts as a cached dataset, specifically for cases of stored procedures, which may have side effects, so we only
        // want to 'call' once, not once to get schema, then AGAIN to get the data. We'll also use this for SQL 
        // statements... that really could be anything.
        DataSet _dataSet;
        String _tableName;

        public AddObjectsGridDataRecords(DataSet dataSet, String tableName)
        {
            _dataSet = dataSet;
            _tableName = tableName;
        }

        #region IGridDataRecords Members

        List<GridDataColumnInfo> _columnInfo;
        List<GridDataColumnInfo> ColumnInfo
        {
            get
            {
                if (_columnInfo == null)
                {
                    _columnInfo = new List<GridDataColumnInfo>();

                    if (_dataSet.Tables.Count > 0)
                    {
                        foreach (DataColumn dc in _dataSet.Tables[_tableName].Columns)
                        {
                            var name = dc.ColumnName;
                            var type = dc.DataType;

                            _columnInfo.Add(new GridDataColumnInfo()
                            {
                                Name = name,
                                Type = type
                            });
                        }
                    }
                }

                return _columnInfo;
            }
        }

        public IEnumerable<GridDataColumnInfo> Columns
        {
            get { return ColumnInfo; }
        }

        #endregion

        #region IEnumerable<IGridDataRecord> Members

        public IEnumerator<IGridDataRecord> GetEnumerator()
        {
            if (_dataSet.Tables.Count > 0)
            {
                foreach (DataRow dr in _dataSet.Tables[_tableName].Rows)
                {
                    yield return new AddObjectsGridDataRecord(dr);
                }

            }
        }

        #endregion

        #region IEnumerable Members

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
        }

        #endregion
    }

    class AddObjectsGridDataRecord : IGridDataRecord
    {
        private readonly DataRow _dr;
        public AddObjectsGridDataRecord(DataRow dr)
        {
            _dr = dr;
        }

        #region IGridDataRecord Members

        public string this[int index]
        {
            get
            {
                var theValue = _dr[index];

                // Simio will first try to parse dates in the current culture
                if (theValue is DateTime)
                    return ((DateTime)theValue).ToString();

                return String.Format(System.Globalization.CultureInfo.InvariantCulture, "{0}", _dr[index]);
            }
        }

        #endregion
    }

    public static class AddObjectsUtils
    {
        /// <summary>
        /// Examines the given DataSet to compute column information.
        /// </summary>
        internal static DataTable ConvertExportContextToDataTable(IGridDataOpenImportLocalRecordsContext importLocalRecordsContext, string tableName)
        {
            // New table
            var dataTable = new DataTable();
            dataTable.TableName = tableName;
            dataTable.Locale = CultureInfo.InvariantCulture;

            List<IGridDataExportColumnInfo> colImportLocalRecordsColumnInfoList = new List<IGridDataExportColumnInfo>();

            foreach (var col in importLocalRecordsContext.Records.Columns)
            {
                colImportLocalRecordsColumnInfoList.Add(col);
                var dtCol = dataTable.Columns.Add(col.Name, Nullable.GetUnderlyingType(col.Type) ?? col.Type);
            }

            // Add Rows to data table
            foreach (var record in importLocalRecordsContext.Records)
            {
                object[] thisRow = new object[dataTable.Columns.Count];

                int dbColIndex = 0;
                foreach (var colExportLocalRecordsColumnInfo in colImportLocalRecordsColumnInfoList)
                {
                    var valueObj = record.GetNativeObject(dbColIndex);
                    thisRow[dbColIndex] = valueObj;
                    dbColIndex++;
                }

                dataTable.Rows.Add(thisRow);
            }

            return dataTable;
        }

        internal static string ToStringAsXml(DataSet ds)
        {
            StringWriter sw = new StringWriter();
            ds.WriteXml(sw, XmlWriteMode.WriteSchema);
            string s = sw.ToString();
            return s;
        }

    }
}


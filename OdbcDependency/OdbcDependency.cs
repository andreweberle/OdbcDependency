using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.Linq;
using System.Threading.Tasks;

namespace OdbcDependency
{
    public delegate void OdbcEventHandler(object source, OdbcEventArgs odbcEvent);

    public class OdbcEventArgs : EventArgs
    {
        public int NumberOfChanges { get; private set; }
        public IEnumerable<DataRow> ChangedDataRow { get; private set; }

        public OdbcEventArgs(int numberOfChanges, IEnumerable<DataRow> dataRow)
        {
            this.NumberOfChanges = numberOfChanges;
            this.ChangedDataRow = dataRow;
        }
    }
    class OdbcDependency
    {
        /// <summary>
        /// Detect When Changes Occur
        /// </summary>
        public event OdbcEventHandler OnChangeEventHandler;

        /// <summary>
        /// Lock ConnectionString
        /// </summary>
        private LockObject Lock { get; set; }

        /// <summary>
        /// ConnectionString
        /// </summary>
        public string ConnectionString => Lock.ConnectionString;

        /// <summary>
        /// CommandText
        /// </summary>
        public string CommandText { get; private set; }

        /// <summary>
        /// StartPolling Boolean
        /// </summary>
        private bool StartPolling { get; set; }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="commandText"></param>
        public OdbcDependency(string connectionString, string commandText)
        {
            Lock = new LockObject(connectionString);
            CommandText = commandText;
        }

        /// <summary>
        /// Start Listening
        /// </summary>
        public void Start()
        {
            StartPolling = true;
            DoWork();
        }

        /// <summary>
        /// Start Listening
        /// </summary>
        /// <param name="timeSpan">Delay Between Each Poll</param>
        public void Start(TimeSpan timeSpan)
        {
            StartPolling = true;
            DoWork(timeSpan);
        }

        /// <summary>
        /// Start Polling For Changes.
        /// </summary>
        /// <param name="timeSpan">Thead Wait Time Delay</param>
        private void DoWork(TimeSpan timeSpan = default(TimeSpan))
        {
            if (!StartPolling)
            {
                return;
            }

            // Create New Thread.
            Task task = Task.Run(async () =>
            {
                // DataTable Object.
                DataTable DataTable = new DataTable();

                // DataSets
                DataSet DataSet1 = null;
                DataSet DataSet2 = null;

                while (StartPolling)
                {
                    using (var odbcConnection = new OdbcConnection(ConnectionString))
                    {
                        using (var odbcCommand = new OdbcCommand(this.CommandText, odbcConnection))
                        {
                            // If Entered, This Is The First Time.
                            if (DataSet1 == null && ConnectionToServerAsync(odbcConnection))
                            {
                                DataSet1 = new DataSet();
                                DataTable.Load(await odbcCommand.ExecuteReaderAsync());
                                DataSet1.Tables.Add(DataTable);
                            }
                            else
                            {
                                // DataSet2 Is Empty
                                if (DataSet2 == null && ConnectionToServerAsync(odbcConnection))
                                {
                                    DataSet2 = new DataSet();
                                    DataTable.Load(await odbcCommand.ExecuteReaderAsync());
                                    DataSet2.Tables.Add(DataTable);
                                }
                                // DataSet2 Table 0 Is Not Empty
                                else if (DataSet2.Tables[0] != null)
                                {
                                    // Swap The DataSet's Around.
                                    Swap(ref DataSet1, ref DataSet2);

                                    // Compate The DataSet's
                                    // EventHandler Will Pick Up Any Changes.
                                    CompareDataSets(DataSet1, DataSet2);
                                }
                            }
                        }
                    }
                    System.Threading.Thread.Sleep((TimeSpan)timeSpan);
                }
            });
            Task.WaitAny(task);
        }

        /// <summary>
        /// Connect To Server Async
        /// </summary>
        /// <param name="odbcConnection"></param>
        /// <returns></returns>
        private bool ConnectionToServerAsync(OdbcConnection odbcConnection)
        {
            bool isConnected = false;

            Task task = Task.Run(async () =>
            {
                try
                {
                    await odbcConnection.OpenAsync();
                    isConnected = true;
                }
                catch (OdbcException ex)
                {
                    Console.WriteLine(ex.Message);
                }
            });

            // Not The best, but will be fine for testing.
            Task.WaitAll(task);
            return isConnected;
        }

        /// <summary>
        /// Stop Listening.
        /// </summary>
        public void Stop()
        {
            OnChangeEventHandler -= OnChangeEventHandler;
            StartPolling = false;
        }

        /// <summary>
        /// Perform DataSet Swap
        /// </summary>
        /// <param name="ds1"></param>
        /// <param name="ds2"></param>
        private void Swap(ref DataSet ds1, ref DataSet ds2)
        {
            DataSet temp = ds1;
            ds1 = ds2;
            ds2 = temp;
        }

        /// <summary>
        /// Compare DataSets.
        /// </summary>
        /// <param name="ds1"></param>
        /// <param name="ds2"></param>
        /// <returns></returns>
        private IEnumerable<DataRow> CompareDataSets(DataSet ds1, DataSet ds2)
        {
            // Find The Difference In The DataSets.
            IEnumerable<DataRow> differences = ds1.Tables[0].AsEnumerable()
                                                            .Except(ds2.Tables[0].AsEnumerable(), DataRowComparer.Default);
            // If There Are Differences
            // Return Them And Raise EventHandler If Present.
            if (differences.Count() > 0)
            {
                OnChangeEventHandler?.Invoke(this, new OdbcEventArgs(differences.Count(), differences));
                return differences;
            }
            else
            {
                return null;
            }
        }
    }

    /// <summary>
    /// Lock Object.
    /// </summary>
    public sealed class LockObject
    {
        public readonly string ConnectionString;
        public LockObject(string connectionString)
        {
            if (connectionString.Length == 0)
            {
                throw new ArgumentNullException("connectionString");
            }
            else
            {
                this.ConnectionString = connectionString;
            }
        }
    }
}

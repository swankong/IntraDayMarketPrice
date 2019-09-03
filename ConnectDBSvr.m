function conn = ConnectDBSrv(dbName, dbAddr)
% SQL database (JDBC) connection 
% ++ INPUT ++
% (for simplicity, for now the involved parameters to commit a database 
%   connection by routine "database" are explicitly given)
% ++ OUTPUT ++
% the object of a database connection
    dftAddr = '127.0.0.1:1433';
    if isempty(dbAddr)
        dbAddr = dftAddr;
    end
    s.DataReturnFormat = 'cellarray';
    s.ErrorHandling = 'store';
    s.NullNumberRead = 'NaN';
    s.NullNumberWrite = 'NaN';
    s.NullStringRead = 'null';
    s.NullStringWrite = 'null';
    s.FetchInBatches = 'yes';
    s.FetchBatchSize = '100000';
    setdbprefs(s);
    server_name = dbName;
    login = 'dbuser';
    pwd = 'dbuser_password';
    driver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver';
    databaseUrl = ['jdbc:sqlserver://' dbAddr ';database=' dbName];
    conn = database(server_name, login, pwd, driver, databaseUrl);
    get(conn, 'AutoCommit');
end

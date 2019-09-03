%
% GetIntrPrices.m
%
% 获取证券的日内价格信息，日内可以运行若干次 
%
% 解析数据后插入SQLServer数据库表，可以替换成其他目标数据库
%
% 数据源：新浪股票行页面
%
% 出错处理：抛出异常，信息邮件发给相关者
%
% By Sun, Kang  
%
%

function GetIntraPrices(ifFlag)
% ifFlag: 股指期货行情标志
    infoUrlBase = 'http://hq.sinajs.cn/list='; % 获取行情根地址
    dtStr = cellstr(datestr(today(), 'yyyymmdd'));
    trdFlag = IsTradingDay(dtStr);
    if trdFlag == false
        exit();
    end
        
    try
        jydbConn = ConnectDBSvr('JYDB', []); % 从聚源数据库读取证券信息，可以替换成其他数据库或者其他数据源
        shSql = sprintf('SELECT [SecuCode] FROM [JYDB].[dbo].[SecuMain] WHERE [SecuCategory] = 1 AND [SecuMarket] = 83 AND [ListedState] = 1 ORDER BY [SecuCode]');
        szSql = sprintf('SELECT [SecuCode] FROM [JYDB].[dbo].[SecuMain] WHERE [SecuCategory] = 1 AND [SecuMarket] = 90 AND [ListedState] = 1 ORDER BY [SecuCode]');
        ifSql = sprintf('SELECT [ContractCode] FROM [JYDB]..[Fut_ContractMain] WHERE [ContractState] = 1 AND [ExchangeCode] = 20 AND [ContractType] = 4 AND [IfReal] = 1');
        shCode = fetch(jydbConn, shSql);
        szCode = fetch(jydbConn, szSql);
        ifCode = fetch(jydbConn, ifSql);
        idxCodeSH = {'000016'; '000300'; '000905'};
        idxCodeSZ = {'399317'};
        if isempty(shCode) || isempty(szCode) || isempty(ifCode)
            ME = MException('VerifyOutput:Return is null', 'Stock code query results are empty.');
            throw(ME);
        end
        if ifFlag == true
            ifRst = GetIndexFutures(ifCode, infoUrlBase);
            allRecs = ifRst;
        else
            secList = [strcat('sz', [szCode; idxCodeSZ]); strcat('sh', [shCode; idxCodeSH])];
            nSecs = size(secList, 1);
            STEPWIN = 800;
            OFFSET = STEPWIN - 1;
            retStr = [];
            stkRec = [];
            for idx = 1:STEPWIN:nSecs
                if idx + OFFSET <= nSecs
                    secLine = strjoin(secList(idx:idx + OFFSET), ',');
                else
                    secLine = strjoin(secList(idx:end), ',');
                end
                queryUrl = [infoUrlBase secLine];
                retStr = [retStr urlread(queryUrl)];
            end

            %save('retStr.mat', 'retStr');
            subStrs = strsplit(retStr, ';');
            nSubs = size(subStrs, 2); % number of returned results
            for i = 1:nSubs
                lnRst = subStrs{1, i};
                if size(lnRst) <= 1
                    continue;
                end
                newLn = strrep(lnRst, '"', ''); % remove " char
                segs = strsplit(newLn, '='); % 
                tickerStr = segs(1);
                valueStr = segs(2);
                rawTicker = char(regexp(tickerStr{1}, '(sh|sz)[0-9]{6}', 'match'));
                if strncmpi(rawTicker, 'sh', 2)
                    stockCode = cellstr(rawTicker(3:end));
                    ticker = strcat(stockCode, '.SH');
                elseif strncmpi(rawTicker, 'sz', 2)
                    stockCode = cellstr(rawTicker(3:end));
                    ticker = strcat(stockCode, '.SZ');
                else
                    ME = MException('VerifyOutput:Unrecognized symbol', 'Invalid stock code return from url.');
                    throw(ME);
                end
                cols = strsplit(valueStr{1}, ',', 'CollapseDelimiters', false);
                secName = cols(1);
                secOpen = num2cell(str2double(cols(2)));
                secPrec = num2cell(str2double(cols(3)));
                secLast = num2cell(str2double(cols(4)));
                secHigh = num2cell(str2double(cols(5)));
                secLow  = num2cell(str2double(cols(6)));
                secVols = num2cell(str2double(cols(9)));
                secAmts = num2cell(str2double(cols(10)));
                secTime = strcat(cols(32), '.', cols(33));
                dbRec = [cellstr(char(java.util.UUID.randomUUID.toString)) dtStr secTime ticker stockCode secName secOpen secPrec ...
                         secLast secHigh secLow secVols secAmts];
                stkRec = [stkRec; dbRec];      
            end
            allRecs = stkRec;
        end
        tblName = 'IntradayPrices';
        colNames = {'objectId' 'tradeDate' 'timeStamp' 'ticker' 'secCode' 'secName' 'dayOpen' 'preClose' 'lastPrice' 'dayHigh' ...
                    'dayLow' 'dayVols' 'dayAmt'};
        mfmConn = ConnectDBSvr('MFM', []);
        fastinsert(mfmConn, tblName, colNames, allRecs); % 插入数据库
        close(jydbConn);
        close(mfmConn);
    catch ME
        logMsg = fprintf('Error in generating intra day securities prices.\nError message: %s\n', ME.message);
        SendEmail(logMsg, char(dtStr));
        exit();
    end
    exit();
end

function [ret] = GetIndexFutures(idxCode, urlBase)
    idxList = strcat('CFF_RE_', idxCode);
    idxLine = strjoin(idxList, ',');
    dtStr = cellstr(datestr(today(), 'yyyymmdd'));
    queryUrl = [urlBase idxLine];
    retStr = urlread(queryUrl);
    subStrs = strsplit(retStr, ';');
    nSubs = size(subStrs, 2); % number of returned results
    ret = [];
    for i = 1:nSubs
        lnRst = subStrs{1, i};
        if size(lnRst) <= 1
            continue;
        end
        newLn = strrep(lnRst, '"', ''); % remove " char
        segs = strsplit(newLn, '='); % 
        tickerStr = segs(1);
        valueStr = segs(2);
        rawTicker = char(regexp(tickerStr{1}, '(IC|IF|IH)[0-9]{4}', 'match'));
        if isempty(rawTicker)
            ME = MException('VerifyOutput:Unrecognized symbol', 'Invalid index futures return from url.');
            throw(ME);
        end 
        ticker = strcat(rawTicker, '.CFE');
        contractCode = rawTicker;
        cols = strsplit(valueStr{1}, ',', 'CollapseDelimiters', false);
        secName = ticker;
        secOpen = num2cell(str2double(cols(1)));
        secHigh = num2cell(str2double(cols(2)));
        secLow  = num2cell(str2double(cols(3)));
        secLast = num2cell(str2double(cols(4)));
        secVols = num2cell(str2double(cols(5)));
        secAmts = num2cell(str2double(cols(6)));
        secPrec = num2cell(str2double(cols(14)));
        secTime = cols(38);
        retRec = [cellstr(char(java.util.UUID.randomUUID.toString)) dtStr secTime ticker contractCode secName secOpen secPrec ...
                  secLast secHigh secLow secVols secAmts];
        ret = [ret; retRec];
    end
end

function [trdFlag] = IsTradingDay(dtStr)
    dbName = 'JYDB';
    tabName = 'QT_TradingDayNew'; % JYDB交易日历表
    dbConn = ConnectDBSvr(dbName, []);
    qStr = sprintf('SELECT [IfTradingDay] FROM [%s]..[%s] WHERE [SecuMarket] = 10 AND [TradingDate] = ''%s'' ', ...
                   dbName, tabName, datestr(datenum(dtStr, 'yyyymmdd'), 'yyyy-mm-dd'));
    qRst = fetch(dbConn, qStr);
    trdFlag = cell2mat(qRst);
end

function SendEmail(logMsg, dtStr)
    toList = {'xxx@yyy.com'; 'xxx@yyy.com'}; 
    mailTitle = '获取日内价格信息运行异常';
    title = [mailTitle '--' dtStr]; 
    mySMTP = 'xxx.yyy.com';
    myEmail = 'xxx@yyy.com';
    setpref('Internet', 'SMTP_Server', mySMTP);
    setpref('Internet', 'E_mail', myEmail);
    setpref('Internet', 'SMTP_Username', 'sunkang-pbj');
    setpref('Internet', 'SMTP_Password', 'Sinosig120103');
    props = java.lang.System.getProperties;
    props.setProperty('mail.smtp.auth', 'true');
    sendmail(toList, title, logMsg);
end
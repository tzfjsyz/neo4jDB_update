/*
用于全量更新[tCR0002_V2.0]表中的relation信息
wrote by tzf, 2017/12/8
*/
const req = require('require-yml');
const Db = require('mssql');
const Mssql = req('./lib/mssql');
const Pool = req('./lib/pool');
const config = req("./config/source.yml");
const log4js = require('log4js');
const fs = require('fs');
const writeLineStream = require('lei-stream').writeLine;
const transactions = require('./transactions.js');
const NodeCache = require("node-cache");
const myCache = new NodeCache({ stdTTL: 100, checkperiod: 120 });         //缓存失效时间3h
const UUID = require('uuid');

log4js.configure({
    appenders: {
        'out': {
            type: 'file',         //文件输出
            filename: 'logs/updateData.log',
            maxLogSize: config.logInfo.maxLogSize
        }
    },
    categories: { default: { appenders: ['out'], level: 'info' } }
});

const logger = log4js.getLogger();

//set汇率转换
async function setRateConvert() {
    return new Promise(async (resolve, reject) => {
        try {
            let now = Date.now();
            let sql = 'select RE9003_001,RE9003_002,RE9003_003,RE9003_004,RE9003_005,RE9003_006,RE9003_007,RE9003_008,RE9003_009,RE9003_010 from [dbo].[tRE9003] where flag<> 1';
            let res = await Mssql.connect(config.mssql_rate).query(sql);
            let setRateConvertCost = Date.now() - now;
            let rows = res.recordset;
            let fetched = rows.length;      //每次查询SQL Server的实际记录数
            if (fetched > 0) {
                for (let i = 0; i < rows.length; i++) {
                    let MY = rows[i].RE9003_001;               //美元
                    let OY = rows[i].RE9003_002;               //欧元
                    let RY = rows[i].RE9003_003;               //日元
                    let GY = rows[i].RE9003_004;               //港元
                    let YB = rows[i].RE9003_005;               //英镑
                    let JNDY = rows[i].RE9003_006;             //加拿大元
                    let ODLYY = rows[i].RE9003_007;            //澳大利亚元
                    let XXLY = rows[i].RE9003_008;             //新西兰元
                    let XJPY = rows[i].RE9003_009;             //新加坡元
                    let RSFL = rows[i].RE9003_010;             //瑞士法郎
                    let obj = {
                        MY: `${MY}`,
                        OY: `${OY}`,
                        RY: `${RY}`,
                        GY: `${GY}`,
                        YB: `${YB}`,
                        JNDY: `${JNDY}`,
                        ODLYY: `${ODLYY}`,
                        XXLY: `${XXLY}`,
                        XJPY: `${XJPY}`,
                        RSFL: `${RSFL}`
                    };
                    myCache.set("currencyRate", obj, function (err, success) {
                        if (!err && success) {
                            return resolve(success);
                            console.log(success);
                            logger.info('myCache set currencyRate status: ' + success);
                            console.log('setRateConvertCost: ' + setRateConvertCost + 'ms');
                            logger.info('setRateConvertCost: ' + setRateConvertCost + 'ms');
                        }
                    });
                }
            }
        } catch (err) {
            console.error(err);
            logger.error(err);
            return reject(err);
        }
    });
}

//get汇率转换
function getRateConvert() {
    let res = {};
    try {
        myCache.get("currencyRate", function (err, value) {
            if (!err) {
                if (value == undefined) {
                    console.log('can not get the currencyRate value');
                    logger.info('can not get the currencyRate value');
                    return ({});
                } else {
                    res = value;
                    console.log('the currencyRate value: ' + '美元: ' + value.MY + ', 欧元: ' + value.OY + ', 日元: ' + value.RY + ', 香港元: ' + value.GY + ', 英镑: ' + value.YB + ', 加拿大元: ' + value.JNDY + ', 澳大利亚元: ' + value.GNDY + ', 新西兰元: ' + value.XXLY + ', 新加坡元: ' + value.XJPY + ', 瑞士法郎: ' + value.RSFL);
                    logger.info('the currencyRate value: ' + '美元: ' + value.MY + ', 欧元: ' + value.OY + ', 日元: ' + value.RY + ', 香港元: ' + value.GY + ', 英镑: ' + value.YB + ', 加拿大元: ' + value.JNDY + ', 澳大利亚元: ' + value.GNDY + ', 新西兰元: ' + value.XXLY + ', 新加坡元: ' + value.XJPY + ', 瑞士法郎: ' + value.RSFL);
                }
            }
        });
        return res;
    } catch (err) {
        console.error(err);
        logger.error(err);
        return err;
    }
}

//判断货币种类
function judgeRateFlag(subAmountUnit) {
    let rateFlag = 'RMB';
    if (subAmountUnit == '万?美元' || subAmountUnit == '万美元' || subAmountUnit == '万元美元') {
        rateFlag = 'MY';
    }
    else if (subAmountUnit == '万欧元' || subAmountUnit == '万元欧元') {
        rateFlag = 'OY';
    }
    else if (subAmountUnit == '万日元' || subAmountUnit == '万元日元') {
        rateFlag = 'RY';
    }
    else if (subAmountUnit == '万香港元' || subAmountUnit == '万元港元') {
        rateFlag = 'GY';
    }
    else if (subAmountUnit == '万英镑' || subAmountUnit == '万元英镑') {
        rateFlag = 'YB';
    }
    else if (subAmountUnit == '万加拿大元' || subAmountUnit == '万加元' || subAmountUnit == '万元加拿大元') {
        rateFlag = 'JNDY';
    }
    else if (subAmountUnit == '万澳大利亚元' || subAmountUnit == '万元澳大利亚元') {
        rateFlag = 'ODLYY';
    }
    else if (subAmountUnit == '万新西兰元' || subAmountUnit == '万元新西兰元') {
        rateFlag = 'XXLY';
    }
    else if (subAmountUnit == '万新加坡元' || subAmountUnit == '万元新加坡元') {
        rateFlag = 'XJPY';
    }
    else if (subAmountUnit == '万瑞士法郎' || subAmountUnit == '万元瑞士法郎') {
        rateFlag = 'RSFL';
    }
    else if (subAmountUnit == '元人民币' || subAmountUnit == '股') {
        rateFlag = 'OTHER';
    }
    return rateFlag;
}

//过滤字符串中的特殊字符
function stripscript(s) {
    let pattern = new RegExp("[`~!@#$^&*()=|{}':;',\\[\\].<>/?~！@#￥……&*（）&mdash;—|{}【】‘；：”“'。，、？]");
    let rs = "";
    for (let i = 0; i < s.length; i++) {
        rs = rs + s.substr(i, 1).replace(pattern, '');
    }
    //去除,"、和前后空格符
    rs = (((rs.replace(/,/g, '')).replace(/"/g, '')).replace(/\n/g, '')).replace(/(^\s*)|(\s*$)/g, "");
    //去除隐含的特殊字符
    rs = rs.replace(/\u0000|\u0001|\u0002|\u0003|\u0004|\u0005|\u0006|\u0007|\u0008|\u0009|\u000a|\u000b|\u000c|\u000d|\u000e|\u000f|\u0010|\u0011|\u0012|\u0013|\u0014|\u0015|\u0016|\u0017|\u0018|\u0019|\u001a|\u001b|\u001c|\u001d|\u001e|\u001f/g, "");
    return rs;
}

let updateTotalRelation = {
    //初始化extraNodes.csv文件
    initExtraNodesCSV: function () {
        //机构代码为空的nodes
        let CSVFilePathExtraNodes = '../neo4jDB_update/totalData/extraNodes.csv';
        let extraNodeW = writeLineStream(fs.createWriteStream(CSVFilePathExtraNodes), {
            // 换行符，默认\n
            newline: '\n',
            // 编码器，可以为函数或字符串（内置编码器：json，base64），默认null
            encoding: function (data) {
                return data;
            },
            // 缓存的行数，默认为0（表示不缓存），此选项主要用于优化写文件性能，当数量缓存的内容超过该数量时再一次性写入到流中，可以提高写速度
            cacheLines: 0
        });
        return extraNodeW;
    },

    //初始化persons.csv文件
    initPersonsCSV: function () {
        //自然人的nodes
        let CSVFilePath = '../neo4jDB_update/totalData/persons.csv';
        let personW = writeLineStream(fs.createWriteStream(CSVFilePath), {
            // 换行符，默认\n
            newline: '\n',
            // 编码器，可以为函数或字符串（内置编码器：json，base64），默认null
            encoding: function (data) {
                return data;
            },
            // 缓存的行数，默认为0（表示不缓存），此选项主要用于优化写文件性能，当数量缓存的内容超过该数量时再一次性写入到流中，可以提高写速度
            cacheLines: 0
        });
        return personW;
    },

    //初始化relation_invest.csv文件
    initInvestCSV: function () {
        let CSVFilePathInvest = '../neo4jDB_update/totalData/relations_invest.csv';
        // writeLineStream第一个参数为ReadStream实例，也可以为文件名
        let investW = writeLineStream(fs.createWriteStream(CSVFilePathInvest), {
            // 换行符，默认\n
            newline: '\n',
            // 编码器，可以为函数或字符串（内置编码器：json，base64），默认null
            encoding: function (data) {
                return data;
            },
            // 缓存的行数，默认为0（表示不缓存），此选项主要用于优化写文件性能，当数量缓存的内容超过该数量时再一次性写入到流中，可以提高写速度
            cacheLines: 0
        });
        return investW;
    },

    //读取CRDB的tCR0063表建立personalCode->ITCode2的字典数据
    startFormHolderDict: async function (flag) {
        if (flag) {
            try {
                //清空personalCode->ITCode2字典数据
                transactions.flushHolderDict();
                let id = config.updateInfo.relationId_holder;
                let i = 1;
                let ctx = await transactions.getContext(id);
                let fetched = 0;
                if (!ctx.last)
                    ctx.last = 0;
                let resultCount = 0;
                let startTime = Date.now();
                let updateInfo = {};
                let updateStatus = 0;
                do {
                    let rows = [];
                    let now = Date.now();
                    let sql = `
                                select top 10000 cast(tmstamp as bigint) as _ts,ITCode2,PersonalCode from [tCR0063] WITH(READPAST) 
                                where ITCode2 is not null and PersonalCode is not null and flag<> 1 and tmstamp > cast( cast(${ctx.last} as bigint) as binary(8)) order by tmstamp;
                              `;
                    let res = await Mssql.connect(config.mssql).query(sql);
                    let queryCost = Date.now() - now;
                    rows = res.recordset;
                    fetched = rows.length;                                                                   //每次查询SQL Server的实际记录数
                    let keyValues = [];
                    if (fetched > 0) {
                        resultCount += fetched;
                        for (let i = 0; i < rows.length; i++) {
                            let startId = rows[i].PersonalCode;
                            let endId = rows[i].ITCode2;
                            keyValues.push(`${startId}-${endId}`, '1');
                        }
                        transactions.saveHolderDict(keyValues);
                        ctx.last = rows[fetched - 1]._ts;
                        ctx.updatetime = now;
                        ctx.latestUpdated = resultCount;

                        // 保存同步到的位置
                        transactions.saveContext(id, ctx)
                            .catch(err => console.error(err));
                        if (fetched > 0)
                            logger.info(`Total table: 'tCR0063' qry:${queryCost} ms; result:${fetched}` + ', 读写次数: ' + i + ', last timestamp: ' + ctx.last);
                        console.log('全量更新表tCR0063中relation信息,读写次数: ' + i + '， 查询SQLServer耗时：' + queryCost + 'ms' + ', last timestamp: ' + ctx.last);
                        i++;

                        //for test
                        // if(i == 2 )
                        //     break;
                    }
                } while (fetched >= 10000);
                let totalCost = Date.now() - startTime;
                let logInfo = '全量更新表tCR0063中relation信息, 总耗时: ' + totalCost + ', 更新记录数: ' + resultCount;
                updateStatus = 1;
                updateInfo.status = updateStatus;
                updateInfo.info = logInfo;
                logger.info(`counts: ` + i++ + `, totalConst :${totalCost} ms; resultCount: ${resultCount}`);
                console.log(logInfo);
                return updateInfo;
            } catch (err) {
                console.error(err);
                logger.error(err);
                return err;
            }
        }
    },

    //投资关系
    startQueryInvestRelation: async function (flag, extraNodeW, personW, investW) {
        if (flag) {
            try {
                //set汇率转换
                let covertFlag = await setRateConvert();
                let rateValueMap = {};
                if (covertFlag == true) {
                    rateValueMap = getRateConvert();
                }
                let id = config.updateInfo.relationId_invest;
                let i = 1;
                let ctx = await transactions.getContext(id);
                let fetched = 0;
                let filtered = 0;
                if (!ctx.last)
                    ctx.last = 0;
                let resultCount = 0;
                let startTime = Date.now();
                let updateInfo = {};
                let updateStatus = 0;
                let line1 = 'timestamp:string,:START_ID,relation:TYPE,weight:float,subAmountRMB:float,subAmount:float,subAmountUnit:string,:END_ID';
                investW.write(line1);

                let personLine1 = 'isPerson:string,ITCode2:ID,name:string,isExtra:string,originTable:string';
                personW.write(personLine1);
                let isPerson = 0;                                                                   //0代表不是自然人  
                let isExtra = 0;                                                                    //0代表有机构代码
                let startIsBranches = 0;                                                            //初始化分支机构属性,0表示不是分支机构，1表示是分支机构
                let endIsBranches = 0;                                                              //无机构代码的默认非分支机构
                let surStatus = 1;                                                                  //续存状态默认为1                          
                let originTable = 'tCR0002_V2.0';                                                   //数据来源 
                let RMBFund = 0;
                let regFund = 0;
                let regFundUnit = 'null';
                do {
                    let rows = [];
                    let now = Date.now();
                    let sql = `
                                select top 10000 cast(tmstamp as bigint) as _ts,ITCode2,ITName,CR0002_011,CR0002_003,CR0002_004,CR0002_007,CR0002_008,PersonalCode from [tCR0002_V2.0] WITH(READPAST) 
                                where flag<> 1 and tmstamp > cast( cast(${ctx.last} as bigint) as binary(8)) order by tmstamp;
                              `;
                    let res = await Mssql.connect(config.mssql).query(sql);
                    let queryCost = Date.now() - now;
                    rows = res.recordset;
                    fetched = rows.length;                                                                   //每次查询SQL Server的实际记录数
                    writeStart = Date.now();
                    if (fetched > 0) {
                        let keyLines = [];
                        let perLines = [];
                        resultCount += fetched;
                        for (let i = 0; i < rows.length; i++) {
                            let startId = rows[i].CR0002_011;
                            let endId = rows[i].ITCode2;
                            let holdWeight = rows[i].CR0002_004;
                            let subAmount = rows[i].CR0002_007;
                            let subAmountUnit = rows[i].CR0002_008;
                            let perCode = rows[i].PersonalCode;
                            let timestamp = rows[i]._ts;
                            if (!holdWeight) holdWeight = 0;
                            if (!subAmount) subAmount = 0;
                            if (!subAmountUnit) subAmountUnit = '万人民币元';
                            let rate = null;
                            let rateValue = 1;
                            if (subAmountUnit != null) {
                                rate = judgeRateFlag(subAmountUnit);
                            }
                            if (rate == 'RMB') rateValue = 1;
                            else if (rate == 'OTHER') rateValue = 0.0001;                                       //转成万单位
                            else if (rate != null) {
                                rateValue = parseFloat(rateValueMap[`${rate}`]);
                            }
                            let subAmountRMB = subAmount * rateValue;
                            if (!startId && !perCode) {                                                         //机构代码CR0002_011为空，并且PersonalCode为空时，则该机构随机生成ID
                                let id = rows[i]._ts + transactions.createRndNum(6);
                                startId = id;
                                let startName = rows[i].CR0002_003;
                                let name = stripscript(startName);                                              //过滤特殊字符
                                if (!name) name = 'others';
                                isExtra = 1;                                                                   //1代表没有机构代码
                                let extraLineN = `${isPerson},${id},${name},${RMBFund},${regFund},${regFundUnit},${isExtra},${surStatus},${originTable},${startIsBranches}`;
                                extraNodeW.write(extraLineN);
                            }
                            if (!endId) {
                                let id = transactions.createRndNum(6) + rows[i]._ts;
                                endId = id;
                                let endName = rows[i].ITName;
                                let name = stripscript(endName);
                                if (!name) name = 'others';
                                isExtra = 1;
                                let extraLineN = `${isPerson},${id},${name},${RMBFund},${regFund},${regFundUnit},${isExtra},${surStatus},${originTable},${endIsBranches}`;
                                extraNodeW.write(extraLineN);
                            }
                            if (perCode) {                                                                     //PersonalCode不为空时，则为自然人，ID用PersonalCode代替
                                startId = perCode;
                                let startName = rows[i].CR0002_003;
                                let name = stripscript(startName);                                                  //过滤特殊字符
                                if (!name) name = 'others';
                                isExtra = 1;
                                let personLineN = `1,${perCode},${name},${isExtra},${originTable}`;
                                personW.write(personLineN);
                                keyLines.push(`${perCode}-${endId}`);
                                perLines.push(`${timestamp},${startId},invests,${holdWeight},${subAmountRMB},${subAmount},${subAmountUnit},${endId}`);
                            }
                            else if (!perCode) {
                                let lineN = `${timestamp},${startId},invests,${holdWeight},${subAmountRMB},${subAmount},${subAmountUnit},${endId}`;
                                investW.write(lineN);
                            }
                        }
                        if (keyLines.length > 0) {
                            let flags = await transactions.getHolderDict(keyLines);
                            if (flags && flags.length == keyLines.length) {
                                for (let i = 0; i < perLines.length; i++) {
                                    if ('1' == flags[i]) {
                                        investW.write(perLines[i]);
                                    }
                                    else {
                                        filtered ++;
                                    }
                                }
                            }
                            else {
                                console.error('查询HolderDict字典失败!');
                                logger.error('查询HolderDict字典失败!');
                                return -1;
                            }
                        }
                        
                        ctx.last = rows[fetched - 1]._ts;
                        ctx.updatetime = now;
                        ctx.latestUpdated = resultCount;

                        // 保存同步到的位置
                        transactions.saveContext(id, ctx)
                            .catch(err => console.error(err));
                        writeCost = Date.now() - writeStart;
                        if (fetched > 0)
                            logger.info(`Total table: 'tCR0002_V2.0' qry:${queryCost} ms; result:${fetched}` +', writeCost: ' + writeCost + 'ms' + ', 读写次数: ' + i + ', last timestamp: ' + ctx.last);
                        console.log('全量更新表tCR0002_V2.0中relation信息,读写次数: ' + i + '， 查询SQLServer耗时：' + queryCost + 'ms' +', writeCost: ' + writeCost + 'ms' + ', last timestamp: ' + ctx.last);
                        i++;

                        //for test
                        // if(i == 2 )
                        //     break;
                    }
                } while (fetched >= 10000);
                // 结束
                investW.end(function () {
                    // 回调函数可选
                    console.log('relations_invest.csv write end');
                    logger.info('relations_invest.csv write end');
                });
                let totalCost = Date.now() - startTime;
                let logInfo = '全量更新表tCR0002_V2.0中relation信息, 总耗时: ' + totalCost + ', 更新记录数: ' + resultCount +', 过滤总记录数: ' +filtered;
                updateStatus = 1;
                updateInfo.status = updateStatus;
                updateInfo.info = logInfo;
                logger.info(`counts: ` + i++ + `, totalConst :${totalCost} ms; resultCount: ${resultCount}; 过滤总记录数: ${filtered}`);
                console.log(logInfo);
                return updateInfo;
            } catch (err) {
                console.error(err);
                logger.error(err);
                return err;
            }
        }
    },

    //担保关系
    startQueryGuaranteeRelation: async function (flag, extraNodeW) {
        if (flag) {
            try {
                let id = config.updateInfo.relationId_guarantee;
                let i = 1;
                let ctx = await transactions.getContext(id);
                let fetched = 0;
                if (!ctx.last)
                    ctx.last = 0;
                let resultCount = 0;
                let startTime = Date.now();
                let updateInfo = {};
                let updateStatus = 0;
                let CSVFilePath = '../neo4jDB_update/totalData/relations_guarantee.csv';
                // writeLineStream第一个参数为ReadStream实例，也可以为文件名
                let w = writeLineStream(fs.createWriteStream(CSVFilePath), {
                    // 换行符，默认\n
                    newline: '\n',
                    // 编码器，可以为函数或字符串（内置编码器：json，base64），默认null
                    encoding: function (data) {
                        return data;
                    },
                    // 缓存的行数，默认为0（表示不缓存），此选项主要用于优化写文件性能，当数量缓存的内容超过该数量时再一次性写入到流中，可以提高写速度
                    cacheLines: 0
                });
                let line1 = 'timestamp:string,:START_ID,relation:TYPE,:END_ID,weight:float';
                w.write(line1);

                let extraLine1 = 'isPerson:string,ITCode2:ID,name:string,RMBFund:float,regFund:float,regFundUnit:string,isExtra:string,surStatus:string,originTable:string,isBranches:string';
                extraNodeW.write(extraLine1);
                let startIsBranches = 0;                                                            //初始化分支机构属性,0表示不是分支机构，1表示是分支机构
                let endIsBranches = 0;                                                              //无机构代码的默认非分支机构
                let isExtra = 0;
                let isPerson = 0;
                let surStatus = 1;                                                                  //续存状态默认为1
                let originTable = 'tCR0008_V2.0';
                let RMBFund = 0;
                let regFund = 0;
                let regFundUnit = 'null';
                do {
                    let rows = [];
                    let now = Date.now();
                    let sql = `
                                select top 10000 cast(tmstamp as bigint) as _ts,ITCode2,ITName,CR0008_003,CR0008_002 from [tCR0008_V2.0] WITH(READPAST) 
                                where flag<> 1 and tmstamp > cast( cast(${ctx.last} as bigint) as binary(8)) order by tmstamp;
                              `;
                    let res = await Mssql.connect(config.mssql).query(sql);
                    let queryCost = Date.now() - now;
                    rows = res.recordset;
                    fetched = rows.length;      //每次查询SQL Server的实际记录数
                    if (fetched > 0) {
                        resultCount += fetched;
                        for (let i = 0; i < rows.length; i++) {
                            let startId = rows[i].ITCode2;
                            let endId = rows[i].CR0008_003;
                            let timestamp = rows[i]._ts;
                            if (!startId) {
                                let id = rows[i]._ts + transactions.createRndNum(6);
                                startId = id;
                                let startName = rows[i].ITName;
                                let name = stripscript(startName);
                                if (!name) name = 'others';
                                isExtra = 1;
                                let extraLineN = `${isPerson},${id},${name},${RMBFund},${regFund},${regFundUnit},${isExtra},${surStatus},${originTable},${startIsBranches}`;
                                extraNodeW.write(extraLineN);
                            }
                            if (!endId) {
                                let id = transactions.createRndNum(6) + rows[i]._ts;
                                endId = id;
                                let endName = rows[i].CR0008_002;
                                let name = stripscript(endName);
                                if (!name) name = 'others';
                                isExtra = 1;
                                let extraLineN = `${isPerson},${id},${name},${RMBFund},${regFund},${regFundUnit},${isExtra},${surStatus},${originTable},${endIsBranches}`;
                                extraNodeW.write(extraLineN);
                            }
                            let lineN = `${timestamp},${startId},guarantees,${endId},0`;
                            w.write(lineN);
                        }
                        ctx.last = rows[fetched - 1]._ts;
                        ctx.updatetime = now;
                        ctx.latestUpdated = resultCount;

                        // 保存同步到的位置
                        transactions.saveContext(id, ctx)
                            .catch(err => console.error(err));
                        if (fetched > 0)
                            logger.info(`Total table: 'tCR0008_V2.0' qry:${queryCost} ms; result:${fetched}` + ', 读写次数: ' + i + ', last timestamp: ' + ctx.last);
                        console.log('全量更新表tCR0008_V2.0中guarantee信息,读写次数: ' + i + '， 查询SQLServer耗时：' + queryCost + 'ms' + ', last timestamp: ' + ctx.last);
                        i++;

                        //for test
                        // if(i == 2 )
                        //     break;
                    }
                } while (fetched >= 10000);
                // 结束
                w.end(function () {
                    // 回调函数可选
                    console.log('relations_guarantee.csv write end');
                    logger.info('relations_guarantee.csv write end');
                });
                let totalCost = Date.now() - startTime;
                let logInfo = '全量更新表tCR0008_V2.0中guarantee信息, 总耗时: ' + totalCost + 'ms' + ', 更新记录数: ' + resultCount;
                updateStatus = 1;
                updateInfo.status = updateStatus;
                updateInfo.info = logInfo;
                logger.info(`counts: ` + i++ + `, totalConst :${totalCost} ms; resultCount: ${resultCount}`);
                console.log(logInfo);
                logger.info(logInfo);
                return updateInfo;
            } catch (err) {
                console.error(err);
                logger.error(err);
                return err;
            }
        }
    },

    //家族关系
    startQueryFamilyRelation: async function (flag, personW) {
        if (flag) {
            try {
                let id = config.updateInfo.relationId_family;
                let i = 1;
                let ctx = await transactions.getContext(id);
                let fetched = 0;
                if (!ctx.last)
                    ctx.last = 0;
                let resultCount = 0;
                let startTime = Date.now();
                let updateInfo = {};
                let updateStatus = 0;
                let CSVFilePath = '../neo4jDB_update/totalData/relations_family.csv';
                // writeLineStream第一个参数为ReadStream实例，也可以为文件名
                let familyW = writeLineStream(fs.createWriteStream(CSVFilePath), {
                    // 换行符，默认\n
                    newline: '\n',
                    // 编码器，可以为函数或字符串（内置编码器：json，base64），默认null
                    encoding: function (data) {
                        return data;
                    },
                    // 缓存的行数，默认为0（表示不缓存），此选项主要用于优化写文件性能，当数量缓存的内容超过该数量时再一次性写入到流中，可以提高写速度
                    cacheLines: 0
                });
                let familyLine1 = 'timestamp:string,:START_ID,relation:TYPE,:END_ID,relationCode:int,relationName:string,weight:float';
                familyW.write(familyLine1);

                let isExtra = 1;                                                    //isExtra = 1用来区分有无ITCode2
                let isPerson = 1;
                let originTable = 'tCR0058';
                do {
                    let rows = [];
                    let now = Date.now();
                    let sql = `
                                select top 10000 cast(tmstamp as bigint) as _ts,PersonalCode,Cname,PersonalCode2,Cname2,CR0058_001,CR0058_002 from [tCR0058] WITH(READPAST) 
                                where flag<> 1 and tmstamp > cast( cast(${ctx.last} as bigint) as binary(8)) order by tmstamp;
                              `;
                    let res = await Mssql.connect(config.mssql).query(sql);
                    let queryCost = Date.now() - now;
                    rows = res.recordset;
                    fetched = rows.length;                                              //每次查询SQL Server的实际记录数
                    if (fetched > 0) {
                        resultCount += fetched;
                        for (let i = 0; i < rows.length; i++) {
                            let startId = rows[i].PersonalCode;
                            let perName1 = rows[i].Cname;
                            let endId = rows[i].PersonalCode2;
                            let perName2 = rows[i].Cname2;
                            let relationCode = rows[i].CR0058_001;
                            let relationName = rows[i].CR0058_002;
                            let timestamp = rows[i]._ts;
                            if (!perName1) {
                                perName1 = 'others';
                            }
                            else if (perName1) {
                                perName1 = stripscript(perName1);
                            }
                            if (!perName2) {
                                perName2 = 'others';
                            }
                            else if (perName2) {
                                perName2 = stripscript(perName2);
                            }
                            if (!relationCode) relationCode = 0;
                            if (!relationName) relationName = 'others';
                            if (!startId) {
                                startId = rows[i]._ts + transactions.createRndNum(6);
                            }
                            // else if (startId) {
                            //     startId = parseInt(startId.replace(/P/g, ''));                                     //personCode去掉P，转成int型
                            // }
                            if (!endId) {
                                endId = rows[i]._ts + transactions.createRndNum(6);
                            }
                            // else if (endId) {
                            //     endId = parseInt(endId.replace(/P/g, '')); 
                            // }
                            //写relation_family.csv文件
                            let familyLineN = `${timestamp},${startId},family,${endId},${relationCode},${relationName},0`;
                            familyW.write(familyLineN);
                            //写persons.csv文件
                            let personLineN1 = `${isPerson},${startId},${perName1},${isExtra},${originTable}`;
                            personW.write(personLineN1);
                            let personLineN2 = `${isPerson},${endId},${perName2},${isExtra},${originTable}`;
                            personW.write(personLineN2);
                        }
                        ctx.last = rows[fetched - 1]._ts;
                        ctx.updatetime = now;
                        ctx.latestUpdated = resultCount;
                        // 保存同步到的位置
                        transactions.saveContext(id, ctx).catch(err => console.error(err));
                        console.log('全量更新表tCR0058中relation信息,读写次数: ' + i + '， 查询SQLServer耗时：' + queryCost + 'ms' + ', last timestamp: ' + ctx.last);
                        logger.info(`total table: 'tCR0058' qry:${queryCost} ms; result:${fetched}` + ', 读写次数: ' + i + ', last timestamp: ' + ctx.last);
                        i++;
                    }
                } while (fetched >= 10000);
                //写relations_famliy.CSV文件结束
                familyW.end(function () {
                    console.log('relations_family.csv write end');
                    logger.info('relations_family.csv write end');
                });
                let totalCost = Date.now() - startTime;
                let logInfo = '全量更新表tCR0058中relation信息, 总耗时: ' + totalCost + ', 更新记录数: ' + resultCount;
                updateStatus = 1;
                updateInfo.status = updateStatus;
                updateInfo.info = logInfo;
                logger.info(`counts: ` + i++ + `, totalConst :${totalCost} ms; resultCount: ${resultCount}`);
                console.log(logInfo);
                return updateInfo;
            } catch (err) {
                console.error(err);
                logger.error(err);
                return err;
            }
        }
    },

    //高管投资关系
    startQueryExecutiveInvestRelation: async function (flag, extraNodeW, personW) {
        if (flag) {
            try {
                let id = config.updateInfo.relationId_executive;
                let i = 1;
                let ctx = await transactions.getContext(id);
                let fetched = 0;
                if (!ctx.last)
                    ctx.last = 0;
                let resultCount = 0;
                let startTime = Date.now();
                let updateInfo = {};
                let updateStatus = 0;
                let CSVFilePath = '../neo4jDB_update/totalData/relations_execute.csv';
                // writeLineStream第一个参数为ReadStream实例，也可以为文件名
                let executeW = writeLineStream(fs.createWriteStream(CSVFilePath), {
                    // 换行符，默认\n
                    newline: '\n',
                    // 编码器，可以为函数或字符串（内置编码器：json，base64），默认null
                    encoding: function (data) {
                        return data;
                    },
                    // 缓存的行数，默认为0（表示不缓存），此选项主要用于优化写文件性能，当数量缓存的内容超过该数量时再一次性写入到流中，可以提高写速度
                    cacheLines: 0
                });
                let executeLine1 = 'timestamp:string,:START_ID,relation:TYPE,:END_ID,weight:float';
                executeW.write(executeLine1);
                let isExtra = 1;                                                                    //0代表有机构代码
                let isBranches = 0;                                                                 //初始化分支机构属性,0表示不是分支机构，1表示是分支机构
                let surStatus = 1;                                                                  //续存状态默认为1                          
                let originTable = 'tCR0064';                                                        //数据来源 
                let RMBFund = 0;
                let regFund = 0;
                let regFundUnit = 'null';
                do {
                    let rows = [];
                    let now = Date.now();
                    let sql = `
                                select top 10000 cast(tmstamp as bigint) as _ts,PersonalCode,Cname,ITCode2,ITName from [tCR0064] WITH(READPAST) 
                                where flag<> 1 and tmstamp > cast( cast(${ctx.last} as bigint) as binary(8)) order by tmstamp;                    
                              `;
                    let res = await Mssql.connect(config.mssql).query(sql);
                    let queryCost = Date.now() - now;
                    rows = res.recordset;
                    fetched = rows.length;                                                                   //每次查询SQL Server的实际记录数
                    if (fetched > 0) {
                        resultCount += fetched;
                        for (let i = 0; i < rows.length; i++) {
                            let startId = rows[i].PersonalCode;
                            let endId = rows[i].ITCode2;
                            let personName = rows[i].Cname;
                            let compName = rows[i].ITName;
                            let timestamp = rows[i]._ts;
                            if (null != personName || '' != personName) {
                                personName = stripscript(personName);                                       //过滤特殊字符
                            }
                            else if (!personName && startId) {
                                personName = 'others';
                            }
                            if (!endId && (null != compName || '' != compName)) {
                                endId = rows[i]._ts + transactions.createRndNum(6);
                                name = stripscript(compName);
                                let extraLineN = `0,${endId},${name},${RMBFund},${regFund},${regFundUnit},${isExtra},${surStatus},${originTable},${isBranches}`;
                                extraNodeW.write(extraLineN);                                              //过滤特殊字符
                            }
                            if (!startId && (null != personName || '' != personName)) {
                                startId = rows[i]._ts + transactions.createRndNum(6);
                            }
                            //写relations_execute.csv文件
                            let executeLineN = `${timestamp},${startId},executes,${endId},0`;
                            executeW.write(executeLineN);

                            //写persons.csv文件
                            let personLineN = `1,${startId},${personName},${isExtra},${originTable}`;
                            personW.write(personLineN);
                        }
                        ctx.last = rows[fetched - 1]._ts;
                        ctx.updatetime = now;
                        ctx.latestUpdated = resultCount;

                        // 保存同步到的位置
                        transactions.saveContext(id, ctx)
                            .catch(err => console.error(err));
                        if (fetched > 0)
                            logger.info(`total table: 'tCR0064' qry:${queryCost} ms; result:${fetched}` + ', 读写次数: ' + i + ', last timestamp: ' + ctx.last);
                        console.log('全量更新表tCR0064中relation信息,读写次数: ' + i + '， 查询SQLServer耗时：' + queryCost + 'ms' + ', last timestamp: ' + ctx.last);
                        i++;

                        //for test
                        // if(i == 2 )
                        //     break;
                    }
                } while (fetched >= 10000);
                // 写relations_execute.scv文件结束
                executeW.end(function () {
                    // 回调函数可选
                    console.log('relations_execute.csv write end');
                    logger.info('relations_execute.csv write end');
                });
                // 写persons.csv文件结束
                personW.end(function () {
                    // 回调函数可选
                    console.log('persons.csv write end');
                    logger.info('persons.csv write end');
                });
                // 写extraNodes.csv文件结束
                extraNodeW.end(function () {
                    console.log('extraNodes.csv write end');
                    logger.info('extraNodes.csv write end');
                })
                let totalCost = Date.now() - startTime;
                let logInfo = '全量更新表tCR0064中relation信息, 总耗时: ' + totalCost + ', 更新记录数: ' + resultCount;
                updateStatus = 1;
                updateInfo.status = updateStatus;
                updateInfo.info = logInfo;
                logger.info(`counts: ` + i++ + `, totalConst :${totalCost} ms; resultCount: ${resultCount}`);
                console.log(logInfo);
                return updateInfo;
            } catch (err) {
                console.error(err);
                logger.error(err);
                return err;
            }
        }
    }

}


module.exports = updateTotalRelation;
/*
用于全量更新[tCR0001_V2.0]表中的company信息
wrote by tzf, 2017/12/8
*/
const req = require('require-yml');
const Db = require('mssql');
const Mssql = req('./lib/mssql');
const Pool = req('./lib/pool');
const config = req('./config/source.yml');
const log4js = require('log4js');
const fs = require('fs');
const writeLineStream = require('lei-stream').writeLine;
const transactions = require('./transactions.js');
const UUID = require('uuid');
const NodeCache = require("node-cache");
const myCache = new NodeCache({ stdTTL: 100, checkperiod: 120 });         //缓存失效时间3h

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
                    console.log('the currencyRate value: ' + '美元: ' + value.MY + ', 欧元: ' + value.OY + ', 日元: ' + value.RY + ', 香港元: ' + value.GY + ', 英镑: ' + value.YB + ', 加拿大元: ' + value.JNDY + ', 澳大利亚元: ' + value.ODLYY + ', 新西兰元: ' + value.XXLY + ', 新加坡元: ' + value.XJPY + ', 瑞士法郎: ' + value.RSFL);
                    logger.info('the currencyRate value: ' + '美元: ' + value.MY + ', 欧元: ' + value.OY + ', 日元: ' + value.RY + ', 香港元: ' + value.GY + ', 英镑: ' + value.YB + ', 加拿大元: ' + value.JNDY + ', 澳大利亚元: ' + value.ODLYY + ', 新西兰元: ' + value.XXLY + ', 新加坡元: ' + value.XJPY + ', 瑞士法郎: ' + value.RSFL);
                }
            }
        });
        return res;
    } catch (err) {
        console.log(err);
        logger.error(err);
        return reject(err);
    }
}

//判断货币类型
function judgeCurrencyFlag(currencyFlag) {
    let rateFlag = 'RMB';
    if (currencyFlag == 840) rateFlag = 'MY';
    else if (currencyFlag == 954) rateFlag = 'OY';
    else if (currencyFlag == 392) rateFlag = 'RY';
    else if (currencyFlag == 344) rateFlag = 'GY';
    else if (currencyFlag == 826) rateFlag = 'YB';
    else if (currencyFlag == 124) rateFlag = 'JNDY';
    else if (currencyFlag == 36) rateFlag = 'ODLYY';
    else if (currencyFlag == 554) rateFlag = 'XXLY';
    else if (currencyFlag == 702) rateFlag = 'XJPY';
    else if (currencyFlag == 756) rateFlag = 'RSFL';
    else if (currencyFlag == 156 || currencyFlag == 0) rateFlag = 'RMB';
    return rateFlag;
}

let updateTotalCompany = {
    startQueryCompany: async function (flag) {
        if (flag) {
            try {
                //set汇率转换
                let covertFlag = await setRateConvert();
                let rateValueMap = {};
                if (covertFlag == true) {
                    rateValueMap = getRateConvert();
                }
                let id = config.updateInfo.companyId;
                let i = 1;
                let ctx = await transactions.getContext(id);
                let fetched = 0;
                if (!ctx.last)
                    ctx.last = 0;                  //全量更新置0
                let resultCount = 0;
                let startTime = Date.now();
                let updateInfo = {'logInfo': '', 'updateStatus': 0};
                let CSVFilePath = '../neo4jDB_update/totalData/companies.csv';                                //windows

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
                // let line1 = 'ITCode2:ID,ITName:string'; 
                let line1 = 'timestamp:string,isPerson:string,ITCode2:ID,RMBFund:float,regFund:float,regFundUnit:string,isExtra:string,surStatus:string,originTable:string,isBranches:string';
                w.write(line1);

                let originTable = 'tCR0001_V2.0';                                         //数据来源
                // let isPerson = 0;                                                         //0代表不是自然人
                do {
                    let rows = [];
                    let now = Date.now();
                    let sql = `
                                select top 10000 cast(tmstamp as bigint) as _ts, ITCode2,CR0001_005,CR0001_006,CR0001_040,CR0001_041 from [tCR0001_V2.0] WITH(READPAST) 
                                where flag<> 1 and tmstamp > cast( cast(${ctx.last} as bigint) as binary(8)) order by tmstamp;
                              `;
                    let res = await Mssql.connect(config.mssql).query(sql);
                    let queryCost = Date.now() - now;
                    rows = res.recordset;
                    fetched = rows.length;                                                //每次查询SQL Server的实际记录数
                    if (fetched > 0) {
                        resultCount += fetched;
                        let lines = [];
                        let codes = [];
                        for (let i = 0; i < rows.length; i++) {
                            let rate = null;                                                          //汇率标识
                            let rateValue = 1;
                            let ITCode = rows[i].ITCode2;
                            let timestamp = rows[i]._ts;
                            if (ITCode) {
                                codes.push(ITCode);
                            }
                            if (!ITCode) {                                                //如果ITCode为null,则传入UUID,并在node上的isExtra置1；
                                // ITCode = UUID.v4(); 
                                // ITCode = transactions.createRndNum(12);                
                                ITCode = rows[i]._ts + transactions.createRndNum(6);      //产生6位随机数 + timestamp作为ITCode
                                isExtra = 1;                                              //1代表没有机构代码
                            }
                            else {
                                isExtra = 0;
                            }
                            let fund = rows[i].CR0001_005;                                //注册资金，未换算的值
                            let currencyUnit = rows[i].CR0001_006;                        //货币类型
                            let currencyFlag = rows[i].CR0001_040;                        //货币种类标识
                            let surStatus = rows[i].CR0001_041;                           //续存状态
                            if (!surStatus) surStatus = 1;                                //默认为1
                            if (!currencyFlag) currencyFlag = 0;
                            if (!currencyUnit) currencyUnit = '万人民币元';
                            if (currencyFlag != null) {
                                rate = judgeCurrencyFlag(currencyFlag);
                            }
                            if (!fund) fund = 0;
                            if (rate == 'RMB') rateValue = 1;
                            else if (rate != null) {
                                rateValue = parseFloat(rateValueMap[`${rate}`]);
                            }
                            let RMBFund = fund * rateValue;
                            // let ITName =  rows[i].ITName.replace(/"/g, '\\"');         //将ITName中包含的所有引号转义
                            // let ITName =  rows[i].ITName.replace(/"/g, '\""');
                            // let lineN = `"${ITCode}","${ITName}"`;
                            // let lineN = `${ITCode},"${ITName}"`;
                            //let lineN = `${ITCode},${RMBFund},${fund},${currencyUnit},${isExtra},${isBranches}`;
                            // w.write(lineN);
                            lines.push([timestamp,0, ITCode, RMBFund, fund, currencyUnit, isExtra, surStatus, originTable]);
                        }

                        let branches = null;
                        let retryCount = 0;
                        do {
                            try {
                                branches = await transactions.judgeBranches(codes);
                                break;
                            } catch (err) {
                                retryCount++;
                                console.error(err);
                                logger.error(err);
                            }
                        } while (retryCount < 3)
                        if (retryCount == 3) {
                            console.error('全量更新表tCR0001_V2.0中company信息，查询分支机构失败');
                            logger.error('全量更新表tCR0001_V2.0中company信息，查询分支机构失败');
                            // break;
                            return updateInfo;
                        }
                        for (let i = 0; i < lines.length; ++i) {
                            let line = lines[i];
                            // let isBranches = 0;                                          //初始化分支机构属性,0表示不是分支机构，1表示是分支机构
                            if (branches[i] == "") {
                                branches[i] = 0;
                            } else if (branches[i] != "") {
                                branches[i] = 1;
                            }
                            line.push(branches[i]);
                            w.write(line.join(","));
                        }

                        ctx.last = rows[fetched - 1]._ts;
                        ctx.updatetime = now;
                        ctx.latestUpdated = resultCount;
                        // 保存同步到的位置
                        transactions.saveContext(id, ctx)
                            .catch(err => console.error(err));
                        if (fetched > 0)
                            logger.info(`Total table: 'tCR0001_V2.0' qry:${queryCost} ms; result:${fetched}` + ', 读写次数: ' + i + ', last timestamp: '+ ctx.last);
                        console.log('全量更新表tCR0001_V2.0中company信息，读写次数: ' + i + '， 查询SQLServer耗时：' + queryCost + 'ms' + ', last timestamp: '+ ctx.last);
                        i++;

                        //for test
                        // if(i == 2 )
                        //     break;
                    }
                } while (fetched >= 10000);
                // 结束
                w.end(function () {
                    // 回调函数可选
                    console.log('companies.csv write end');
                    logger.info('companies.csv write end');
                });
                let totalCost = Date.now() - startTime;
                logInfo = '全量更新表tCR0001_V2.0中company信息，总耗时: ' + totalCost + ', 更新记录数: ' + resultCount;
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


module.exports = updateTotalCompany;
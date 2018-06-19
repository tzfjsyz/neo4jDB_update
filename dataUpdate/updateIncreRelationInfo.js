/*
用于增量更新[tCR0002_V2.0]表中的relation(flag=0, flag=1, flag=2)的信息, 将SQL Server提取出来的数据放入csv文件中
wrote by tzf, 2017/12/8
*/
const req = require('require-yml');
const Mssql = req('./lib/mssql');
const Pool = req('./lib/pool');
const config = req("./config/source.yml");
const log4js = require('log4js');
const fs = require('fs');
const writeLineStream = require('lei-stream').writeLine;
const Redis = require('ioredis');
const redis = new Redis(config.redisUrl[0]);
const neo4j = require('neo4j-driver').v1;
//const driver = neo4j.driver(`${config.neo4jServer.url}`, neo4j.auth.basic(`${config.neo4jServer.user}`, `${config.neo4jServer.password}`), { maxTransactionRetryTime: 30000 });

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

//分类csv文件的数目
let fromIdChangeFileNum = 0;
let fromToIdSyncAbsentFileNum = 0;
let fromToIdSyncExistFileNum = 0;
let toIdChangeFileNum = 0;
let relationsDeleteFileNum = 0;
let relationsModifyFileNum = 0;

async function saveContext(id, ctx) {
    let ctx_id = `ctx_${id}`;
    let res = await redis.set(ctx_id, JSON.stringify(ctx));
    return res;
}

async function getContext(id) {
    let ctx_id = `ctx_${id}`;
    let res = await redis.get(ctx_id);
    if (res) {
        return JSON.parse(res);
    } else {
        return {};
    }
}

//查询fromId是否存在
async function queryFromId(fromId) {
    return new Promise(async (resolve, reject) => {
        let session = driver.session();
        let hasFromId = false;
        try {
            let queryBody = `MATCH (from:company {ITCode2: ${fromId}}) return from`;
            let writeTxPromise = await session.writeTransaction(tx => tx.run(queryBody));
            if (writeTxPromise.records && writeTxPromise.records.length >= 1) {
                hasFromId = true;
            }
            return resolve(hasFromId);

        } catch (err) {
            return reject(err);
            console.log(err);
        }
    });
}

//查询toId是否存在
async function queryToId(toId) {
    return new Promise(async (resolve, reject) => {
        let session = driver.session();
        let hasToId = false;
        try {
            let queryBody = `MATCH (to:company {ITCode2: ${toId}}) return to`;
            let writeTxPromise = await session.writeTransaction(tx => tx.run(queryBody));
            if (writeTxPromise.records && writeTxPromise.records.length >= 1) {
                hasToId = true;
            }
            return resolve(hasToId);

        } catch (err) {
            return reject(err);
            console.log(err);
        }
    });
}

//不同的flag值的数据在SQL Server中一次性提取(flag=0, flag=1, flag=2)
async function querySQLServer(ctx, id) {
    return new Promise(async (resolve, reject) => {
        try {
            let i = 1;            //记录读写文件次数
            let fetched = 0;
            if (!ctx.last)
                ctx.last = 0;
            let resultCount = 0;
            let startTime = Date.now();
            let sql = '';
            let queryRes = null;
            let updateInfo = {};
            let updateStatus = 0;
            //满足条件的记录数
            let dataIndex = 0;
            //定义csv文件的writeLineStream
            let dataW = {};

            do {
                let rows = [];
                let now = Date.now();

                sql = `
                        select top 10000 cast(tmstamp as bigint) as _ts,ITCode2,CR0002_011,CR0002_004,flag from [tCR0002_V2.0] WITH(READPAST) 
                        where ITCode2 is not null and CR0002_011 is not null and tmstamp > cast( cast(${ctx.last} as bigint) as binary(8)) order by tmstamp;
                        `;

                queryRes = await Mssql.connect(config.mssql).query(sql);

                let queryCost = Date.now() - now;
                rows = queryRes.recordset;
                fetched = rows.length;      //每次查询SQL Server的实际记录数
                let writeCost = 0;

                if (fetched > 0) {
                    resultCount += fetched;
                    //生成CSV header
                    let csvHeader = 'flag,fromId,relation,weight,toId';
                    let name = '';
                    let fileName = '';
                    let lineN = '';
                    let catalog = '';
                    //遍历每行记录
                    for (let i = 0; i < rows.length; i++) {
                        let flag = rows[i].flag;
                        let startId = rows[i].CR0002_011;
                        let endId = rows[i].ITCode2;
                        let holdWeight = rows[i].CR0002_004;
                        if (!holdWeight) holdWeight = 0;

                        //通过fromId/toId查询neo4j数据库，判断fromId/toId的是否存在，根据不同的情况写入不同的CSV文件中
                        // let fromIdStatus = await queryFromId(startId);
                        // let toIdStatus = await queryToId(endId);

                        // writeLineStream第一个参数为ReadStream实例，也可以为文件名
                        fileName = `incrementalRelations.csv`;
                        //生成一个CSV文件，CSV header生成一次
                        dataW = writeLineStream(fs.createWriteStream(`../neo4jDB_update/incrementalData/${fileName}`), {
                            // 换行符，默认\n 
                            newline: '\n',
                            // 编码器，可以为函数或字符串（内置编码器：json，base64），默认null
                            encoding: function (data) {
                                return data;
                            },
                            // 缓存的行数，默认为0（表示不缓存），此选项主要用于优化写文件性能，当数量缓存的内容超过该数量时再一次性写入到流中，可以提高写速度
                            cacheLines: 0
                        });
                        console.log(`生成${fileName}成功！`);
                        logger.info(`create the file: ${fileName} successfully!`);
                        dataW.write(csvHeader);

                        lineN = `${flag},${startId},invests,${holdWeight},${endId}`;
                        // await createCSVFile(catalog, fileName, index, lineN, fetched);
                        //写入CSv的每一行内容
                        dataW.write(lineN);
                        console.log(`写入${fileName}数据：第${dataIndex}行 ${lineN}`);
                        dataIndex++;                                               //判断结果为true的次数，即实际写入的记录数
                    }
                    ctx.last = rows[fetched - 1]._ts;
                    ctx.updatetime = now;
                    ctx.latestUpdated = resultCount;

                    // 保存同步到的位置
                    saveContext(id, ctx)
                        .catch(err => console.error(err));
                    if (fetched > 0)
                        logger.info(`Incremental table: 'tCR0002_V2.0' qry:${queryCost} ms; result:${fetched}` + ', 读写次数: ' + i);
                    console.log(`增量更新表tCR0002_V2.0信息, 读写次数: ` + i + '， 查询SQLServer耗时：' + queryCost + 'ms');
                    i++;
                }

            } while (fetched >= 10000);
            console.log(`写入${fileName}成功！`);

            // 结束
            dataW.end(function () {
                // 回调函数可选
                console.log('end');
            });

            let totalCost = Date.now() - startTime;
            let logInfo = `增量更新表tCR0002_V2.0信息, 总耗时: ` + totalCost + ', 更新记录数: ' + resultCount;
            updateStatus = 1;
            updateInfo.status = updateStatus;
            updateInfo.info = logInfo;
            console.log(logInfo);
            logger.info('updateInfo: ' + logInfo);
            updateInfo.dataLines = dataIndex;
            return resolve(updateInfo);
        } catch (err) {
            console.log(err);
        }
    });

}

let updateIncreRelationInfo = {
    startQueryRelation: async function (isUpdate) {
        return new Promise(async (resolve, reject) => {
            if (isUpdate) {
                try {
                    let id = config.updateInfo.relationId;                             //在redis中记录上次增量更新信息的KeyName
                    let ctx = await getContext(id);
                    let updateRelationInfo = {};

                    // //查询flag=0的记录
                    // let relAddRes = await querySQLServer( 0,ctx,id );
                    // updateRelationInfo.addInfo = relAddRes;
                    // //查询flag=1的记录
                    // let relDeleteRes = await querySQLServer( 1,ctx,id );
                    // updateRelationInfo.deleteInfo = relDeleteRes;
                    // //查询flag=2的记录
                    // let relModifyRes = await querySQLServer( 2,ctx,id );
                    // updateRelationInfo.modifyInfo = relModifyRes;

                    //直接将flag=0,1,2的数据存入 同一个csv文件中
                    let relLoadRes = await querySQLServer(ctx, id);
                    updateIncreRelationInfo.loadDataInfo = relLoadRes;

                    return resolve(updateRelationInfo);
                } catch (err) {
                    console.error(err);
                    return resolve(err);
                }
            }

        });

    }
}


module.exports = updateIncreRelationInfo;
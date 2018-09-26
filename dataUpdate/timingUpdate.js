/*
用于全量导入和增量更新neo4j的数据
wrote by tzf, 2017/12/27
*/
const updateTotalCompany = require("./updateTotalCompany.js");
const updateTotalRelation = require("./updateTotalRelation.js");
// const updateIncreCompany = require("./updateIncreCompany.js");
const updateIncreRelationInfo = require("./updateIncreRelationInfo.js");
const log4js = require('log4js');
const req = require('require-yml');
const config = req('./config/source.yml');

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

let timingUpdate = {
    // 全量更新
    startTotalUpdate: async function (upCompFlag, upRelFlag) {
        try {
            let compRes = null;
            let invRelRes = null;
            let guaRelRes = null;
            let famRelRes = null;
            let totalUpdateStatus = { 
                                      compUpdateStatus: {}, guaRelUpdateStatus: {}, invRelUpdateStatus: {}, 
                                      famRelUpdateStatus: {}, exeInvRelUpdateStatus: {}, holderDictUpdateStatus: {}
                                    };

            let extraNodeW = updateTotalRelation.initExtraNodesCSV();
            let personW = updateTotalRelation.initPersonsCSV();
            let investW = updateTotalRelation.initInvestCSV();
            //1. for test
            // holderRes = { status: 1 };
            holderRes = await updateTotalRelation.startFormHolderDict('true');

            if (holderRes && 1 == holderRes.status) {
                totalUpdateStatus.holderDictUpdateStatus = holderRes;
                //2. for test
                // compRes = { status: 1 };
                compRes = await updateTotalCompany.startQueryCompany(upCompFlag);

                if (compRes && compRes.status == 1) {
                    totalUpdateStatus.compUpdateStatus = compRes;
                    //3. for test
                    // guaRelRes = { status: 1 };
                    guaRelRes = await updateTotalRelation.startQueryGuaranteeRelation(upRelFlag, extraNodeW);

                    if (guaRelRes && guaRelRes.status == 1) {
                        totalUpdateStatus.guaRelUpdateStatus = guaRelRes;
                        //4. for test
                        // invRelRes = { status: 1 };
                        invRelRes = await updateTotalRelation.startQueryInvestRelation(upRelFlag, extraNodeW, personW, investW);

                        if (invRelRes && invRelRes.status == 1) {
                            totalUpdateStatus.invRelUpdateStatus = invRelRes;
                            //5. for test
                            famRelRes = await updateTotalRelation.startQueryFamilyRelation(upRelFlag, personW);
                            // famRelRes = { status: 1 };

                            if (famRelRes && famRelRes.status == 1) {
                                totalUpdateStatus.famRelUpdateStatus = famRelRes;
                                exeInvRelRes = await updateTotalRelation.startQueryExecutiveInvestRelation(upRelFlag, extraNodeW, personW);
                                if (exeInvRelRes && exeInvRelRes.status == 1) {
                                    totalUpdateStatus.exeInvRelUpdateStatus = exeInvRelRes;
                                }
                                else {
                                    console.error('startQueryExecutiveInvestRelation 执行失败！');
                                    logger.error('startQueryExecutiveInvestRelation 执行失败！');
                                    return totalUpdateStatus;
                                }
                            }
                            else {
                                console.error('startQueryFamilyRelation 执行失败！');
                                logger.error('startQueryFamilyRelation 执行失败！');
                                return totalUpdateStatus;
                            }
                        }
                        else {
                            console.error('startQueryInvestRelation 执行失败！');
                            logger.error('startQueryInvestRelation 执行失败！');
                            return totalUpdateStatus;
                        }
                    }
                    else {
                        console.error('startQueryGuaranteeRelation 执行失败！');
                        logger.error('startQueryGuaranteeRelation 执行失败！');
                        return totalUpdateStatus;
                    }
                }
                else {
                    console.error('startQueryCompany 执行失败！');
                    logger.error('startQueryCompany 执行失败！');
                    return totalUpdateStatus;
                }
            }
            else {
                console.error('startFormHolderDict 执行失败！');
                logger.error('startFormHolderDict 执行失败！');
                return totalUpdateStatus;
            }


            // extraNodeW.end(function () {
            //     console.log('extra.csv write end');
            //     logger.info('extra.csv write end');
            // });
            console.log('totalUpdateStatus: success!');
            logger.info('totalUpdateStatus: success!');
            return totalUpdateStatus;
        } catch (err) {
            console.log(err);
            logger.info('err: ' + err);
            return ({});
        }
    },

    //增量更新
    startIncreUpdate: async function (upRelFlag) {
        try {
            // let compRes = null;
            let relAddRes = null;
            let inreUpdateStatus = {};
            // compRes = await updateIncreCompany.startQueryCompany(upCompFlag);
            // if (compRes) inreUpdateStatus.compUpdateStatus = compRes;
            relUpdateRes = await updateIncreRelationInfo.startQueryRelation(upRelFlag);         //relations增量更新信息
            if (relUpdateRes) inreUpdateStatus.relAddUpdateStatus = relUpdateRes;
            return inreUpdateStatus;
        } catch (err) {
            console.log(err);
            logger.info('err: ' + err);
            return ({});
        }
    }

}


module.exports = timingUpdate;
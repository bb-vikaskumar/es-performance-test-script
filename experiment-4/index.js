"use strict";

/*
    This is a test to compare performance of ES in below 2 scenarios:
        1. Keep CET in one index. And 2.5 Lakh Mid for that doc in another index.
        2. Merge results from both indices. 

    No. of Docs in index: 5k
    Total running time: 30 minutes
    Final Results can be seen here: https://docs.google.com/spreadsheets/d/1uINIX6-0XB6LGJKuBN1GwLSEFb54NBx2pYew4d0oVLM/edit#gid=0
*/

let elasticsearch = require('elasticsearch');
let _ = require('lodash');
const fs = require('fs')
  
const ES_INDEX_NAME = "camp_only";
const ES_INDEX_WITH_MID_NAME = "mid_only";
const ELASTICSEARCH_IP = "https://vpc-es-benchmarking-test-tg4mvjtk2uzeba4wvby3hanfy4.us-east-1.es.amazonaws.com";
// const ELASTICSEARCH_IP = "http://127.0.0.1:9200";
const ELASTICSEARCH_PORT = 443;

const progress = require('./progress.json');
const ACTIVITY_TYPE = 'create'; // create, fetch
const TOTAL_DOCS_COUNT = (progress.target - progress.done);
const MAX_DOCS_IN_ONE_GO = 40000;
const DOCS_TO_SAVE = 1000;
const ACTIVITY_QTY_TYPE='count'  // time, count
const BATCH_SIZE = 20;
const SEARCH_DURATION_IN_MINS = 0.01;
const WITH_MID = false;
const MIDS_COUNT_PER_DOC = 250000;
const MIDS_SPACE_COUNT = MIDS_COUNT_PER_DOC * 30;
const MIDS_DOCUMENTS_PERCENT = 100;
const LOG_MOD = 5000; // every LOD_MOD record will get logged
const SEIGE_URLS_TO_KEEP = 3000;
const ES_INDEX_FINAL = WITH_MID ? ES_INDEX_WITH_MID_NAME : ES_INDEX_NAME;

let savedRecordFile = `./saved_docs.json`;



let esClient = new elasticsearch.Client({
    host: ELASTICSEARCH_IP
});


class ElasticSearchConnector {

    static ping() {

        return new Promise(_ping);
        function _ping(resolve, reject) {

            esClient.ping({
                requestTimeout:3000,
            })
                .then(function(res) {
                    return resolve("Elastic Search Cluster is Up");
                })
                .catch(function(err) {

                    return reject(err)
                });
        }
    }

    static addDocument(indexName, _id, payload, docType) {

        return new Promise(_addDocument);
        function _addDocument(resolve, reject) {

            esClient.index({
                index: indexName,
                type: docType || "_doc",
                id: _id,
                body: payload
            })
                .then(function (res) {
                    // console.log('** added res: ', res);
                    return resolve({"campaign_id":_id,"res":res,"payload":payload});
                })
                .catch(function (err)  {
                    console.log('** error while adding document: ', err);
                    return reject(err);
                });
        }
    }

    static search(indexName, payload, docType, from, size) {
        let searchQuery = {
            index: indexName,
            preference: '_local',
            type: docType || "_doc",
            body: payload
        }
        if(from != undefined) {
            searchQuery['from'] = from;
        }
        if(size) {
            searchQuery['size'] = size;
        }

        return new Promise(_search);
        function _search(resolve, reject) {

            esClient.search(searchQuery)
                .then(function (res) {
                    console.log('** search res: ', JSON.stringify(res));
                    return resolve(res);
                })
                .catch(function (err)  {
                    console.log('** error while searching: ', err);
                    return reject(err);
                });
        }
    }

    static multiSearch(indexName, queries, docType) {
        let searchQuery = {
            body: []
        }
        queries.map(query => {
            searchQuery.body.push({ index: indexName});
            searchQuery.body.push({query, "_source": []});
        });


        return new Promise(_msearch);
        function _msearch(resolve, reject) {

            esClient.msearch(searchQuery)
                .then(function (res) {
                    console.log('** multisearch res: ', JSON.stringify(res));
                    return resolve(res);
                })
                .catch(function (err)  {
                    console.log('** error while multisearching: ', err);
                    return reject(err);
                });
        }
    }
}


function generateSeries({from, count}) {
    let series = [];
    for(let i=from; i<from+count; i++) {
        series.push(i);
    }
    return series;
}

function generateRandomNumbers({from, to, count}) {
    let nums = [];
    for(let i=0; i<count; i++) {
        nums.push(from + Math.floor(Math.random() * (to-from)));   
    }
    return nums;
}

function generateEmails({count}) {
    let emails = [];
    for(let i=0; i<count; i++) {
        let initialChars = 'abcdefghijklmnopqrstuvwxyz';
        let firstChar = initialChars[Math.floor(Math.random() * initialChars.length)];

        let chars = 'abcdefghijklmnopqrstuvwxyz1234567890';
        let email = firstChar;
        for(let j=0; j<10; j++){
            email += chars[Math.floor(Math.random() * chars.length)];
        }
        email = email + '@gmail.com';
        emails.push(email);
    }
    return emails;
}

function generateStrings({count}) {
    let strings = [];
    for(let i=0; i<count; i++) {
        let chars = 'abcdefghijklmnopqrstuvwxyz';
        let str = '';
        for(let j=0; j<5; j++){
            str += chars[Math.floor(Math.random() * chars.length)];
        }
        strings.push(str);
    }
    return strings;
}

function generatePhoneNumbers({count}) {
    let phones = [];
    for(let i=0; i<count; i++) {
        let initialChars = '123456789';
        let firstNum = initialChars[Math.floor(Math.random() * initialChars.length)];

        let chars = '1234567890';
        let phone = firstNum;
        for(let j=0; j<9; j++){
            phone += chars[Math.floor(Math.random() * chars.length)];
        }
        phones.push(phone);
    }
    return phones;
}

function generateCombination({list, size}) {
    if(!list || !list.length) {
        return [];
    }
    if(size >= list.length) {
        return list;
    }
    let comb = new Set();
    while(comb.size < size){
        comb.add(list[Math.floor(Math.random() * list.length)]);
    }
    return [...comb];
}

function pickOne({list}) {
    return list[Math.floor(Math.random() * list.length)];
}

function generatePermutedDoc({sourceSpace, addMid}) {
    let doc = {
        "mtype": generateCombination({list: sourceSpace.mtype, size: 2}),
        "dc": generateCombination({list: sourceSpace.dc, size: 2}),
        "ds": generateCombination({list: sourceSpace.ds, size: 5}),
        "bbstar": generateCombination({list: sourceSpace.bbstar, size: 1}),
        "status": [pickOne({list: sourceSpace.status})],
        "source": generateCombination({list: sourceSpace.source, size: 1}),
        "emails": generateCombination({list: sourceSpace.emails, size: 5}),
        "phone_numbers": generateCombination({list: sourceSpace.phone_numbers, size: 5}),
        "cp": generateCombination({list: sourceSpace.cp, size: 2}),
        "entry_context": generateCombination({list: sourceSpace.entry_context, size: 2}),
        "sa_city_ids": generateCombination({list: sourceSpace.sa_city_ids, size: 1}),
        "sa_ids": generateCombination({list: sourceSpace.sa_ids, size: 1}),
        "campaign_id": pickOne({list: sourceSpace.campaign_id}),
        "c_sku_id": generateCombination({list: sourceSpace.c_sku_id, size: 1}),
        "c_brand": generateCombination({list: sourceSpace.c_brand, size: 1}),
        "c_tlc": generateCombination({list: sourceSpace.c_tlc, size: 1}),
        "c_mlc": generateCombination({list: sourceSpace.c_mlc, size: 1}),
        "c_llc": generateCombination({list: sourceSpace.c_llc, size: 1}),
        "c_group": generateCombination({list: sourceSpace.c_group, size: 1}),
        "r_sku_id": generateCombination({list: sourceSpace.r_sku_id, size: 1}),
        "r_brand": generateCombination({list: sourceSpace.r_brand, size: 1}),
        "r_tlc": generateCombination({list: sourceSpace.r_tlc, size: 1}),
        "r_mlc": generateCombination({list: sourceSpace.r_mlc, size: 1}),
        "r_llc": generateCombination({list: sourceSpace.r_llc, size: 1}),
        "r_group": generateCombination({list: sourceSpace.r_group, size: 1}),
        "camp_info": generateCombination({list: sourceSpace.camp_info, size: 1}),
    
    };
    if(addMid) {
        doc['mid'] = generateCombination({list: sourceSpace.mid, size: MIDS_COUNT_PER_DOC})
    }
    return doc;
}

function generateMids({sourceSpace, size, addAll}) {
    let mids = generateCombination({list: sourceSpace.mid, size: size});
    if(addAll) {
        mids = [...new Set(['all', ...mids])];
    }
    return mids;
}

function generatePermutedDocs({sourceSpace, count}) {
    let docs = [];
    for(let i=0; i<count; i++) {
        let doc = generatePermutedDoc({sourceSpace, index: i});
        docs.push(doc);
    }
    return docs;
}

function demoPromise({returnVal, delay}) {
    return new Promise(function(resolve) {
        setTimeout(() => resolve(returnVal), delay);
    });
}

function processBatch({docs, startFrom, from, batchId, batchSize, esIndex=ES_INDEX_NAME}) {
    let totalDocsCount = docs.length;
    from = from % totalDocsCount;
    let to = from+batchSize;
    let batchDocs = docs.slice(from, to);
    if(to > docs.length) {
        batchDocs = batchDocs.concat(docs.slice(0, to-totalDocsCount))
    }
    return Promise.all(_.map(batchDocs, (doc, docIndex) => {
        let docId = startFrom + (batchId * batchSize) + docIndex;
        if(ACTIVITY_TYPE === 'create') {
            // console.log('write on ', '_doc: ', docId, ' | ', esIndex, ' : ', JSON.stringify(doc));
            // return demoPromise({returnVal: 'processed '+ docId+ '_'+ batchId, delay: 2000});
            return ElasticSearchConnector.addDocument(esIndex, docId, doc);
        } else if(ACTIVITY_TYPE === 'fetch') {
            console.log('multisearch on ', esIndex, ' : ', JSON.stringify(doc));
            // return demoPromise({returnVal: 'processed '+ docId+ '_'+ batchId, delay: 2000});
            return ElasticSearchConnector.multiSearch(esIndex, doc);
        }else {
            return demoPromise({returnVal: 'processed '+ docId+ '_'+ batchId, delay: 1000});
        } 
    }))
}

let batchResults = {};

async function processAllBatches({docs, startFrom, batchSize, esIndex}) {
    let batchId;
    let startTime;
    try {

        let totalDocsCount = docs.length;
        
        if(ACTIVITY_TYPE === 'fetch' && ACTIVITY_QTY_TYPE === 'time') {
            let targetTime = new Date().getTime() + SEARCH_DURATION_IN_MINS * 60 * 1000;
            batchId = 0;

            while(targetTime > new Date().getTime()) {
                let from = batchId * batchSize;
                startTime = new Date();
                let processedAck = await processBatch({docs, startFrom, from, batchId, batchSize, esIndex: esIndex});

                if(batchId % LOG_MOD === 0) {
                    batchResults[batchId] = {
                        timeElapsed: `${new Date() - startTime}ms`,
                        totalDocsCount: totalDocsCount,
                        from: from,
                        batchSize: batchSize                     
                    }
                    console.log(`[success] time batch: ${batchId} | ${new Date() - startTime}ms`);
                }
                batchId++;
            }

        } else {
            for(batchId=0; (batchId*batchSize)<totalDocsCount; batchId++) {
                let from = batchId * batchSize;
                startTime = new Date();
    
                let processedAck = await processBatch({docs, startFrom, from, batchId, batchSize, esIndex: esIndex});
    
                if(batchId % LOG_MOD === 0) {
                    batchResults[batchId] = {
                        timeElapsed: `${new Date() - startTime}ms`,
                        totalDocsCount: totalDocsCount,
                        from: from,
                        batchSize: batchSize                     
                    }
                    console.log(`[success] batch: ${batchId} | ${new Date() - startTime}ms`);
                }
            }
        }
        
    } catch(e) {
        batchResults[batchId] = {
            error: e.stack,
            totalDocsCount: docs.length,
            batchId: batchId,
            batchSize: batchSize                     
        }
        console.log(`[error] batch: ${batchId} | ${new Date() - startTime}ms, error: ${e.stack}`);
        reject(e);
    }
}

function generateFetchQueries({docs, termsCount}) {
    let queries = [];
    
    for(let i=0; i<docs.length; i+=2) {

        let normalDoc = docs[i];
        let docWithMid = docs[i+1];

        let query_normal = {
            'query': {
                'bool': {
                    'filter': []
                }
            },
            "_source": ["camp_info"]
        };

        for(let field of Object.keys(normalDoc)) {
            let values;
            if(field === 'camp_info' || field === 'campaign_id') {
                continue;
            }
            if(field === 'mid') {
                continue;
            }

            values = generateCombination({list: normalDoc[field], size: termsCount[field]});
            if(field != 'c_sku_id' && field !== 'mtype' && field !== 'status') {
                values = [...new Set(['all', ...values])];
            }
            let terms = {
                [field]: values
            };

            query_normal.query.bool.filter.push({'terms': terms});
        }

        let query_mid = {
            'query': {
                'bool': {
                    'filter': []
                }
            },
            "_source": false,
            "size": 50
        };

        for(let field of Object.keys(docWithMid)) {
            let values;
            values = generateCombination({list: docWithMid[field], size: termsCount[field]});
            let terms = {
                [field]: values
            };
            query_mid.query.bool.filter.push({'terms': terms});
        }

        let query = [];
        query.push({index: ES_INDEX_NAME});
        query.push(query_normal);
        query.push({index: ES_INDEX_WITH_MID_NAME});
        query.push(query_mid);
        queries.push(query);
    }

    return queries;
}


async function createRecords({docCount, batchSize}) {
    let batchId;
    let totalDocsCount = TOTAL_DOCS_COUNT;
    let startTime = new Date();

    try {
        let sourceSpace = {
            mtype: ["normal", "corporate", "visitor", "kirana"],
            dc: ["all", ...generateSeries({from: 1, count: 1000})],
            ds: ["all", ...generateSeries({from: 1, count: 1000})],
            bbstar: ["all", "true", "false"],
            status: ['active', 'inactive'],
            source: ['all', 'bigbasket'],
            emails: ['all', ...generateEmails({count: 1000})],
            phone_numbers: ['all', ...generatePhoneNumbers({count: 1000})],
            cp: ['all', ...generateSeries({from: 1, count: 1000})],
            entry_context: [...generateSeries({from: 1, count: 100})],
            sa_city_ids: ['all', ...generateSeries({from: 1, count: 500})],
            sa_ids: ['all', ...generateSeries({from: 1, count: 1000})],
            mid: ['all', ...generateSeries({from: 1, count: MIDS_SPACE_COUNT})],
            campaign_id: [...generateSeries({from: 1000000, count: 20})],
            c_sku_id: [...generateSeries({from: 1000000, count: 10000})],
            c_brand: ['all', ...generateStrings({count: 1000})],
            c_tlc: ['all', ...generateStrings({count: 1000})],
            c_mlc: ['all', ...generateStrings({count: 1000})],
            c_llc: ['all', ...generateStrings({count: 1000})],
            c_group: ['all', ...generateStrings({count: 1000})],
            r_sku_id: ['all', ...generateStrings({count: 1000})],
            r_brand: ['all', ...generateStrings({count: 1000})],
            r_tlc: ['all', ...generateStrings({count: 1000})],
            r_mlc: ['all', ...generateStrings({count: 1000})],
            r_llc: ['all', ...generateStrings({count: 1000})],
            r_group: ['all', ...generateStrings({count: 1000})],
            camp_info: [generateStrings({count: 5}).toString()+ "{'\''entry_context'\'': [100, 8, 9], '\''offer_category'\'': '\''REGULAR'\'', '\''campaign_theme'\'': '\''[]'\'', '\''discount_type'\'': '\''PERCENT'\'', '\''member_specific'\'': 0, '\''category_breakup'\'': '\''0.00'\'', '\''vendor_breakup'\'': '\''100.00'\'', '\''reward_sku_id'\'': 10000148, '\''redemption_campaign_limit'\'': 9999999, '\''bbstar_campaign'\'': '\''true'\'', '\''redemption_member_limit'\'': 9999999, '\''discount_value'\'': '\''50.00'\'', '\''redemption_order_limit'\'': 9999999, '\''marketing_breakup'\'': '\''0.00'\''}"+ generateStrings({count: 5}).toString()]
        }


        const TOTAL_ITERATIONS = Math.max(docCount/MAX_DOCS_IN_ONE_GO, 1);
        const DOCS_PER_ITERATION = Math.min(docCount, MAX_DOCS_IN_ONE_GO);
        const DOCS_TO_SAVE_PER_ITERATION = DOCS_TO_SAVE / TOTAL_ITERATIONS;
        const SAVE_DOCS_PER_ITERATION_SKIP = Math.max(0, Math.floor(DOCS_PER_ITERATION/DOCS_TO_SAVE_PER_ITERATION));

        console.log('> TOTAL DOCS: ', docCount);
        console.log('> First DocID: ', progress.done);
        console.log('> DOCS_PER_ITERATION: ', DOCS_PER_ITERATION);
        console.log('> TOTAL_ITERATIONS: ', TOTAL_ITERATIONS);
        console.log('> TOTAL_DOCS_TO_SAVE: ', DOCS_TO_SAVE);
        console.log('> DOCS_TO_SAVE_PER_ITERATION: ', DOCS_TO_SAVE_PER_ITERATION);
        console.log('> SAVE_DOCS_PER_ITERATION_SKIP: ', SAVE_DOCS_PER_ITERATION_SKIP);
        
        fs.writeFile(`rally_docs.json`, '', (err) => {
            if (err) throw err;
            console.log(`Initiated file rally_docs.json`);
        })

        for(let k=0; k<TOTAL_ITERATIONS; k++) {
            let progressFile = {...progress, done: (progress.done + k*MAX_DOCS_IN_ONE_GO)};
            fs.writeFile('progress.json', JSON.stringify(progressFile), (err) => {
                if (err) throw err;
            });

            console.log(`[${k}/${docCount/MAX_DOCS_IN_ONE_GO}] Generating records from space...`);
            let allDocs = generatePermutedDocs({sourceSpace: sourceSpace, count: DOCS_PER_ITERATION});
            let writeDocs = [];
            console.log(`Record generation success.`);

            startTime = new Date();
            await processAllBatches({docs: allDocs, startFrom: (progress.done + k*MAX_DOCS_IN_ONE_GO), batchSize: batchSize, esIndex: ES_INDEX_NAME})
            console.log({k: k, batchResults: batchResults, totalWriteTime: `${new Date() - startTime}ms`});

            // for(let i=0; i<allDocs.length; i+=SAVE_DOCS_PER_ITERATION_SKIP+1) {
            //     if(!allDocs[i]) {
            //         console.log('** error. WriteDocs undefined', {i, k});
            //     }
            //     fs.appendFile(`rally_docs.json`, JSON.stringify(allDocs[i])+'\n', (err) => {
            //         if (err) throw err;
            //     })
            // }
            // console.log(`Record written to rally_docs.json`);



            if(!WITH_MID) {
                continue;
            }
    
            try {
                fs.writeFile(`rally_docs.json`, '', (err) => {
                    if (err) throw err;
                    console.log(`Initiated file rally_docs.json`);
                })

                for(let i=0; i<docCount; i++) {
                    let midCount = MIDS_COUNT_PER_DOC;
                    let docWithMid = {
                        mid: generateMids({sourceSpace: sourceSpace, size: midCount, addAll: (i%2 === 0)})
                    }
                                            
                    console.log(`[${i}] Record generation success (with ${midCount} MID).`);
                    
                    batchId = i;
                    from = 0;
                    batchSize = 1;
                    startTime = new Date();
        
                    let processedAck = await processBatch({docs: [docWithMid], from, batchId, batchSize, esIndex: ES_INDEX_WITH_MID_NAME});
        
                    if(batchId % LOG_MOD === 0) {
                        batchResults[batchId] = {
                            timeElapsed: `${new Date() - startTime}ms`,
                            totalDocsCount: totalDocsCount,
                            k: k,
                            from: from,
                            batchSize: batchSize                     
                        }
                        console.log(`[success] [${i}/${docCount/MAX_DOCS_IN_ONE_GO}] batch: ${batchId} | ${new Date() - startTime}ms`);
                    }

                    let minDocWithMid = { mid: docWithMid['mid'].slice(1, 4) };
                    writeDocs.push(allDocs[i]);
                    writeDocs.push(minDocWithMid);
                }
                // Write data in 'Output.txt' .
                // fs.writeFile(`saved_docs.json`, JSON.stringify({records: writeDocs}), (err) => {
                //     if (err) throw err;
                //     console.log(`Records written to saved_docs.json`);
                // })

                for(let i=0; i<writeDocs.length; i+=2*SAVE_DOCS_PER_ITERATION_SKIP) {
                    if(!writeDocs[i] || !writeDocs[i+1]) {
                        console.log('** error. mid WriteDocs undefined', {i, k});
                    }
                    fs.appendFile(`rally_docs.json`, JSON.stringify(writeDocs[i])+'\n'+JSON.stringify(writeDocs[i])+'\n', (err) => {
                        if (err) throw err;
                    })
                }
                console.log(`Records written to rally_docs.json`);
                    
            } catch(e) {
                batchResults[batchId] = {
                    error: e.stack,
                    totalDocsCount: totalDocsCount,
                    batchId: batchId,
                    batchSize: batchSize                     
                }
                console.log(`[error] batch (with mid): ${batchId} | ${new Date() - startTime}ms, error: ${e.stack}`);
            }

        }
    } catch(e) {
        console.log({batchResults: batchResults, totalWriteTime: `${new Date() - startTime}ms`, error: e});
    }
}

async function fetchRecords({docCount, batchSize}) {
    let termsCount = {
        mtype: 1,
        dc: 1,
        ds: 2,
        bbstar: 2,
        status: 1,
        source: 1,
        emails: 1,
        phone_numbers: 1,
        cp: 1,
        entry_context: 1,
        sa_city_ids: 1,
        sa_ids: 1,
        mid: 1,
        campaign_id: 1,
        c_sku_id: 1,
        c_brand: 1,
        c_tlc: 1,
        c_mlc: 1,
        c_llc: 1,
        c_group: 1,
        r_sku_id: 1,
        r_brand: 1,
        r_tlc: 1,
        r_mlc: 1,
        r_llc: 1,
        r_group: 1,
    };

    console.log('Reading records from ', savedRecordFile);
    let savedDocs = require(savedRecordFile).records;
    let allDocs = [];
    
    if(ACTIVITY_QTY_TYPE == 'count') {
        for(let i=0; i<(2*docCount); i++) {
            allDocs.push(savedDocs[i%savedDocs.length]);
         }
    } else {
        allDocs = savedDocs;
    }

    let allQueries = generateFetchQueries({docs: allDocs, termsCount: termsCount});
    
    // let siegeUrls = [];
    // for(let query of allQueries.slice(0, SEIGE_URLS_TO_KEEP)) {
    //     let siegeUrl = `${ELASTICSEARCH_IP}/${ES_INDEX_FINAL}/_search POST ${JSON.stringify(query)}`;
    //     siegeUrls.push(siegeUrl);
    // }

    // fs.writeFile(`siegeUrls${WITH_MID ? '_withMid': ''}.txt`, siegeUrls.join('\n'), (err) => {
    //     if (err) throw err;
    // })

    let startTime = new Date();
    processAllBatches({docs: allQueries, batchSize: batchSize, esIndex: ES_INDEX_FINAL})
        .then((res) => { 
            console.log({batchResults: batchResults, totalReadTime: `${new Date() - startTime}ms`})
        })
        .catch((e) => {
            console.log({batchResults: batchResults, totalReadTime: `${new Date() - startTime}ms`, error: e});
        });
}

if(ACTIVITY_TYPE === 'create') {
    createRecords({docCount:TOTAL_DOCS_COUNT, batchSize: BATCH_SIZE});
} else if (ACTIVITY_TYPE === 'fetch') {
    fetchRecords({docCount:TOTAL_DOCS_COUNT, batchSize: BATCH_SIZE});
}
const etl = require('etl')
const fs = require('fs');
const kafka = require('kafka-node');
const path = require("path");

const _ = require('lodash');

// process.setMaxListeners(0);


(function fetchTransformLoadRawData(req, res) {

    var dataObject = {};
    var dataArray = [];
    var noOfFiles = 0, noOfTimesFilesRead = 0;

    const directoryPath = path.join(__dirname, './', 'ClockData');

    const directoryPathBkp = path.join(__dirname, 'ClockData_BKP');

    fs.readdirSync(directoryPath).forEach(directories => {


        var pth_to_read = path.join(directoryPath, directories);

        console.log("pth_to_read", pth_to_read)
        fs.readdirSync(pth_to_read).forEach(file => {

            console.log("heree", file)
            noOfFiles += 1;

            var pathToFollow = pth_to_read + '/';
            console.log("pathToFollow", pathToFollow + file);

            //once directory is read we create a bkp copy of the same with _bkp name

            if (!fs.existsSync(directoryPathBkp)) {
                console.log("why n how no dir")
                fs.mkdirSync(directoryPathBkp);
            }

            var bkpClckDataFolder = path.join(directoryPathBkp, directories)

            var bkpTargetFolderName = bkpClckDataFolder + '_BKP';

            console.log("wht is coming", bkpTargetFolderName)

            console.log("ehere")
            if (!fs.existsSync(bkpTargetFolderName)) {
                console.log("why n how")
                fs.mkdirSync(bkpTargetFolderName);
            }

            // // //copy the files in bkp folder now

            var targetBkpFIleLocation = bkpTargetFolderName + '/';

            console.log("targetBkpFIleLocation", targetBkpFIleLocation)
            fs.readdirSync(pth_to_read).forEach((files) => {
                // console.log("files",files)
                fs.writeFileSync(targetBkpFIleLocation + file, fs.readFileSync(pathToFollow + file));
            })

            fs.readFile(pathToFollow + file, 'utf8', function (err, data) {
                noOfTimesFilesRead += 1;

                // console.log("tie wise", dataArray)

                if (err) {
                    throw err;
                }

                var formattedFile = data.split(/\r?\n/);
                // console.log("in here", typeof (formattedFile))

                // console.log(formattedFile)
                var obj = {};
                var objectDataArray = [];
                formattedFile.forEach((d, index) => {
                    if (index != 0) {
                        var x = d.split(',');

                        obj = {
                            "EmpCode": x[0],
                            "Transaction Date(yyyymmdd)": x[1],
                            "Transaction Time": x[2],
                            "clientsid": x[3]
                        }
                        objectDataArray.push(obj)
                    }

                })

                // console.log("objectDataArray", objectDataArray)

                if (objectDataArray.length > 0) {

                    objectDataArray.map((d) => {

                        if (dataArray.length > 0 && (dataArray.filter(e => (e.ee_id === d.EmpCode && e.logged_date == d['Transaction Date(yyyymmdd)'] && d['Transaction Time'] != "" && e.clientsid === d['clientsid'])).length > 0)) {

                            dataArray.map((each) => {
                                if (each.ee_id === d.EmpCode && each.clientsid === d.clientsid) {
                                    each['time'].push(d["Transaction Time"])
                                }

                            })
                        } else {
                            if (d.EmpCode != undefined && d['Transaction Time'] != "") {

                                // console.log("finally inside")
                                dataObject = {
                                    'ee_id': d.EmpCode,
                                    "time": [d["Transaction Time"]],
                                    "logged_date": d['Transaction Date(yyyymmdd)'],
                                    "clientsid": d['clientsid']
                                }
                                dataArray.push(dataObject)
                            }
                        }
                    })

                    console.log("file has been read fully")


                    // removed duplicates

                    dataArray.forEach((da) => {
                        var x = Array.from(new Set(da.time));
                        da.time = x;

                    })

                    // console.log("da", dataArray)

                    console.log("noOfFiles = 0,noOfTimesFilesRead", noOfFiles, noOfTimesFilesRead)

                    if (noOfFiles === noOfTimesFilesRead) {
                        //now sending data to kafka producer inorder to pulish it to consuming applications

                        try {

                            const Producer = kafka.Producer;
                            const client = new kafka.KafkaClient('localhost:9092');
                            const producer = new Producer(client, { partitionerType: 3 });
                            const kafka_topic = "kafka-clockdata-processing";
                            try {

                                var entireLogList = [];

                                dataArray.forEach((eachRecord) => {

                                    var keyCombo = eachRecord.ee_id + '_' + eachRecord.clientsid;
                                    console.log("keycombo", keyCombo)
                                    let eachPayload = {};
                                    eachPayload = {
                                        topic: kafka_topic,
                                        messages: eachRecord,
                                        key: keyCombo
                                    }

                                    entireLogList.push(eachPayload);

                                    //if the records go beyonfd 100 i.e. entireLogList.length > 100 then send100 first tehn remaining in next producer.send
                                    //this might nee u to slice the array of first 100 
                                    //need a a proper algo here

                                    if (entireLogList.length > 5) {

                                        console.log("chunking")
                                        var x = _.chunk(entireLogList, 5)
                                        console.log(x);
                                        x.forEach((eachChunk) => {
                                            let payload = eachChunk;
                                            producer.on('ready', function () {
                                                producer.send(payload, function (err, data) {
                                                    console.log(data);
                                                });
                                            });

                                            producer.on('error', function (err) {
                                                console.log("errr", err)
                                            })


                                        })

                                    }
                                    else {
                                        console.log("not chunking")
                                        let payload = entireLogList;
                                        producer.on('ready', function () {
                                            producer.send(payload, function (err, data) {
                                                console.log(data);
                                            });
                                        });

                                        producer.on('error', function (err) {
                                            console.log("errr", err)
                                        })
                                    }





                                })

                                console.log("entireLogList", entireLogList.length)
                                //now sending array of payloads to kafka producer


                                let payload = entireLogList;
                                producer.on('ready', function () {
                                    producer.send(payload, function (err, data) {
                                        console.log(data);
                                    });
                                });

                                producer.on('error', function (err) {
                                    console.log("errr", err)
                                })
                            }
                            catch (e) {
                                console.log("here in catch", e)

                            }
                        }
                        catch (e) {
                            console.log(e);
                        }

                    }






                    //code to remove file once read

                    // fs.unlink(pathToFollow + file, (err) => {
                    //     if (err) {
                    //         console.error(err)
                    //         return
                    //     }
                    //     console.log("remived 1")
                    //     //file removed
                    // })

                }
            });

        });
    })


})();

import utils = require("utils");

var viewUnderTest = 'resources/databases';

describe(viewUnderTest, () => {
    utils.initTest();

    it('should bind', (done) => {

        utils.injector.require(["common/changesContext"],
            (changesContext: any) => {

                utils.mockCommand('commands/resources/getDatabasesCommand', () => getDatabasesData());

                changesContext.default.connectServerWideNotificationCenter();

                utils.runViewmodelTest(viewUnderTest, {})
                    .then(() => done());
        });
    });
});

function getDatabasesData(): Raven.Client.Server.Operations.DatabasesInfo {
    return {
        "Databases": [
            {
                "Bundles": ["Replication"],
                "IsAdmin": true,
                "Name": "sample",
                LoadError: null,
                "Disabled": false,
                "TotalSize": {
                    "HumaneSize": "80.4 GBytes",
                    "SizeInBytes": 86328842649.6
                },
                "Errors": 5,
                "Alerts": 7,
                "UpTime": null,
                "BackupInfo": {
                    FullBackupInterval: "7.00:00:00",
                    IncrementalBackupInterval: null,
                    LastFullBackup: null,
                    LastIncrementalBackup: null
                },
                "DocumentsCount": 10234,
                "IndexesCount": 30,
                "RejectClients": true,
                "IndexingStatus": null
            },
            {
                "Bundles": [],
                "IsAdmin": true,
                "Name": "sample2",
                LoadError: null,
                "Disabled": true,
                "TotalSize": {
                    "HumaneSize": "80.4 GBytes",
                    "SizeInBytes": 86328842649.6
                },
                "Errors": 0,
                "Alerts": 0,
                "UpTime": "05:00:00",
                "BackupInfo": null,
                "DocumentsCount": 10234,
                "IndexesCount": 30,
                "RejectClients": false,
                "IndexingStatus": "Running"
            }
        ]
    };
}
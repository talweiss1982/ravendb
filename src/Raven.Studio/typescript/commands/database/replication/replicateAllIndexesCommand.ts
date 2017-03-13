import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class replicateAllIndexesCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<void> {
        var indexesUrl = '/databases/' + this.db.name + '/replication/replicate-indexes?op=replicate-all';//TODO: use endpoints

        return this.post(indexesUrl, null, null, { dataType: undefined })
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to send replicate all indexes command!", response.responseText, response.statusText);
            }).done(() => {
                this.reportSuccess("Sent replicate all indexes command.");
            });
    }
}

export = replicateAllIndexesCommand;

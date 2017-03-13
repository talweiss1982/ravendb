import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import replicationDestination = require("models/database/replication/replicationDestination");

class replicateTransformersCommand extends commandBase {
    constructor(private db: database, private destination: replicationDestination) {
        super();
    }

    execute(): JQueryPromise<void> {
        var transformersUrl = '/databases/' + this.db.name + '/replication/replicate-transformers?op=replicate-all-to-destination';//TODO: use endpoints
        var destinationJson = JSON.stringify(this.destination.toDto());
        return this.post(transformersUrl, destinationJson, null, { dataType: undefined })
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to send replicate transformers command!", response.responseText, response.statusText);
            }).done(() => {
                this.reportSuccess("Sent replicate transformers command");
            });
    }
}

export = replicateTransformersCommand;  

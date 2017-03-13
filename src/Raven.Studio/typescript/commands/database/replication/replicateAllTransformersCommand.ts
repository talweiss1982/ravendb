import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class replicateAllTransformersCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<void> {
        var transformersUrl = '/databases/' + this.db.name + '/replication/replicate-transformers?op=replicate-all';//TODO: use endpoints

        return this.post(transformersUrl, null, null, { dataType: undefined })
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to send replicate all transformers command!", response.responseText, response.statusText);
            }).done(() => {
                this.reportSuccess("Sent replicate all transformers command");
            });
    }
}

export = replicateAllTransformersCommand; 

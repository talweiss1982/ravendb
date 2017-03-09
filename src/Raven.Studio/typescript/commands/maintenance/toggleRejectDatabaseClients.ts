import commandBase = require("commands/commandBase");

class toggleRejectDatabaseClients extends commandBase {
    /**
    * @param names - The array of database names to toggle
    * @param isSettingDisabled - Status of disabled to set
    */
    constructor(private dbName: string, private rejectClientsEnabled: boolean) {
        super();
    }

    execute(): JQueryPromise<any> {
        var action = this.rejectClientsEnabled ? "reject clients mode" : "accept clients mode";
        var args = {
            id: this.dbName,
            isRejectClientsEnabled: this.rejectClientsEnabled
        }
        var url = "/admin/databases-toggle-reject-clients" + this.urlEncodeArgs(args);//TODO: use endpoints
        var toggleTask = this.post(url, null, null, { dataType: undefined });
        toggleTask.done(() => this.reportSuccess("Successfully switched to " + action + " in " + this.dbName));
        toggleTask.fail((response: JQueryXHR) => this.reportError("Failed switching to " + action + " in " + this.dbName, response.responseText, response.statusText));
        return toggleTask;
    }
}

export = toggleRejectDatabaseClients;   

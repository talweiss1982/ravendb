import viewModelBase = require("viewmodels/viewModelBase");
import sqlReplicationConnections = require("models/database/sqlReplication/sqlReplicationConnections");
import predefinedSqlConnection = require("models/database/sqlReplication/predefinedSqlConnection");
import document = require("models/database/documents/document");
import documentMetadata = require("models/database/documents/documentMetadata");
import saveDocumentCommand = require("commands/database/documents/saveDocumentCommand");
import appUrl = require("common/appUrl");
import getSqlReplicationConnectionsCommand = require("commands/database/sqlReplication/getSqlReplicationConnectionsCommand");
import eventsCollector = require("common/eventsCollector");

class sqlReplicationConnectionStringsManagement extends viewModelBase{
    
    htmlSelector ="#sqlReplicationConnectionsManagement";
    connections = ko.observable<sqlReplicationConnections>();
    isSaveEnabled: KnockoutComputed<boolean>;
    
    constructor() {
        super();
    }

    loadConnections():JQueryPromise<any> {
        return new getSqlReplicationConnectionsCommand(this.activeDatabase())
            .execute()
            .done((repSetup: Raven.Server.Documents.SqlReplication.SqlConnections) => {

                this.connections(new sqlReplicationConnections(repSetup));
                if (this.connections().predefinedConnections().length > 0) {
                    this.connections().predefinedConnections().forEach(x=> this.subscribeToSqlReplicationConnectionName(x));
                }
            })
            .fail(() => {
                this.connections(sqlReplicationConnections.empty());
            });
    }

    canActivate() {
        var def = $.Deferred();
        this.loadConnections()
            .always(() => def.resolve({ can: true }));
        return def;
    }

    activate(args: any) {
        super.activate(args);
        this.dirtyFlag = new ko.DirtyFlag([this.connections]);
        this.isSaveEnabled = ko.computed(() => this.dirtyFlag().isDirty());
    }

    save() {
        eventsCollector.default.reportEvent("sql-replication-connections", "save");

        var newDoc = new document(this.connections().toDto());
        this.attachReservedMetaProperties("Raven/SqlReplication/Connections", newDoc.__metadata);

        var saveCommand = new saveDocumentCommand("Raven/SqlReplication/Connections", newDoc, this.activeDatabase());
        var saveTask = saveCommand.execute();
        saveTask.done(() => this.dirtyFlag().reset());
    }

    attachReservedMetaProperties(id: string, target: documentMetadata) {
        target.etag(0);
        target.collection = target.collection || document.getCollectionFromId(id);
        target.id = id;
    }

    getSqlReplicationConnectionsUrl() {
        return appUrl.forSqlReplicationConnections(this.activeDatabase());
    }


    addSqlReplicationConnection() {
        eventsCollector.default.reportEvent("sql-replication-connections", "create");

        var newPredefinedConnection = predefinedSqlConnection.empty();
        this.connections().predefinedConnections.unshift(newPredefinedConnection);
        this.subscribeToSqlReplicationConnectionName(newPredefinedConnection);
        newPredefinedConnection.name("New");
    }

    removeSqlReplicationConnection(connection: predefinedSqlConnection) {
        eventsCollector.default.reportEvent("sql-replication-connections", "remove");

        this.connections().predefinedConnections.remove(connection);
    }

    subscribeToSqlReplicationConnectionName(con: predefinedSqlConnection) {
        con.name.subscribe((previousName: string) => {
                //Get the previous value of 'name' here before it's set to newValue
            var nameInputArray = $('input[name="name"]')
                    .each((index: number, inputField: any) => {
                    inputField.setCustomValidity("");
                });
        }, this, "beforeChange");
        con.name.subscribe((newName) => {
            var message = "";
            if (newName === "") {
                message = "Please fill out this field.";
            }
            else if (this.isSqlPredefinedConnectionNameExists(newName)) {
                message = "SQL Replication Connection name already exists.";
            }
            $('input[name="name"]')
                .filter(function () { return this.value === newName; })
                .each((index: number, element: any) => {
                    element.setCustomValidity(message);
                });
        });
    }

    isSqlPredefinedConnectionNameExists(connectionName: string): boolean {
        if (this.connections().predefinedConnections().some(x => x.name() === connectionName)) {
            return true;
        }
        return false;
    }

    providerChanged(obj: predefinedSqlConnection, event: JQueryEventObject) {
        if (event.originalEvent) {
            var curConnectionString = !!obj.connectionString() ? obj.connectionString().trim() : "";
            if (curConnectionString === "" ||
                sqlReplicationConnections.sqlProvidersConnectionStrings.find(x => x.ConnectionString == curConnectionString)) {
                var matchingConnectionStringPair: { ProviderName: string; ConnectionString: string; } = sqlReplicationConnections.sqlProvidersConnectionStrings.find(x => x.ProviderName === (<any>event.originalEvent.srcElement).selectedOptions[0].value);
                if (!!matchingConnectionStringPair) {
                    var matchingConnectionStringValue: string = matchingConnectionStringPair.ConnectionString;
                    obj.connectionString(matchingConnectionStringValue);
                }
            }
        }
    }

}

export =sqlReplicationConnectionStringsManagement;
